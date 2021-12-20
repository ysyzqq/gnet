// Copyright 2019 Andy Pan. All rights reserved.
// Copyright 2018 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// +build linux darwin netbsd freebsd openbsd dragonfly

package gnet

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/internal/netpoll"
	"golang.org/x/sys/unix"
)

type eventloop struct {
	idx          int             // loop index in the server loops list 第几个
	svr          *server         // server in loop server的引用
	codec        ICodec          // codec for TCP
	packet       []byte          // read packet buffer
	poller       *netpoll.Poller // epoll or kqueue 轮询器 linux下是epoll, FreeBSD下是kqueue
	connCount    int32           // number of active connections in event-loop 活动连接数量
	connections  map[int]*conn   // loop connections fd -> conn 连接
	eventHandler EventHandler    // user eventHandler
}

// 同步加减, 要保证原子性
func (el *eventloop) plusConnCount() {
	atomic.AddInt32(&el.connCount, 1)
}

func (el *eventloop) minusConnCount() {
	atomic.AddInt32(&el.connCount, -1)
}

func (el *eventloop) loadConnCount() int32 {
	return atomic.LoadInt32(&el.connCount)
}

func (el *eventloop) closeAllConns() {
	// Close loops and all outstanding connections
	for _, c := range el.connections {
		_ = el.loopCloseConn(c, nil)
	}
}

// udp 时没有主反应堆, 所有的el都调用这个方法来启动
func (el *eventloop) loopRun() {
	defer func() {
		el.closeAllConns()
		if el.idx == 0 && el.svr.opts.Ticker {
			close(el.svr.ticktock)
		}
		el.svr.signalShutdown()
	}()

	if el.idx == 0 && el.svr.opts.Ticker {
		go el.loopTicker()
	}

	el.svr.logger.Printf("event-loop:%d exits with error: %v\n", el.idx, el.poller.Polling(el.handleEvent))
}

// 处理listen的端口
func (el *eventloop) loopAccept(fd int) error {
	if fd == el.svr.ln.fd {
		if el.svr.ln.pconn != nil { // udp
			return el.loopReadUDP(fd)
		}
		nfd, sa, err := unix.Accept(fd)
		if err != nil {
			if err == unix.EAGAIN {
				return nil
			}
			return err
		}
		if err = unix.SetNonblock(nfd, true); err != nil {
			return err
		}
		c := newTCPConn(nfd, el, sa)
		if err = el.poller.AddRead(c.fd); err == nil {
			el.connections[c.fd] = c
			el.plusConnCount()
			return el.loopOpen(c)
		}
		return err
	}
	return nil
}

// 在el中开启连接
func (el *eventloop) loopOpen(c *conn) error {
	c.opened = true
	c.localAddr = el.svr.ln.lnaddr // 本地服务监听的地址
	c.remoteAddr = netpoll.SockaddrToTCPOrUnixAddr(c.sa) // 远程地址
	out, action := el.eventHandler.OnOpened(c) // 回调事件
	if el.svr.opts.TCPKeepAlive > 0 {
		if _, ok := el.svr.ln.ln.(*net.TCPListener); ok {
			_ = netpoll.SetKeepAlive(c.fd, int(el.svr.opts.TCPKeepAlive/time.Second))
		}
	}
	if out != nil { // 如果连接开启的回调有输出[]byte, 写入
		c.open(out)
	}

	if !c.outboundBuffer.IsEmpty() { // 如果已经有写入数据, 轮询器里添加可写事件的监听
		_ = el.poller.AddWrite(c.fd)
	}

	return el.handleAction(c, action)
}

// 从el里的一个连接中读取数据
func (el *eventloop) loopRead(c *conn) error {
	n, err := unix.Read(c.fd, el.packet)
	if n == 0 || err != nil {
		if err == unix.EAGAIN { // 没有数据可以读取
			return nil
		}
		return el.loopCloseConn(c, err) // 异常, 关闭连接
	}
	c.buffer = el.packet[:n] // 读取的buffer先暂存到c.buffer里

	// 先清空当前inboundBuffer里的数据,
	for inFrame, _ := c.read(); inFrame != nil; inFrame, _ = c.read() {
		out, action := el.eventHandler.React(inFrame, c) // 响应解码数据的回调, 这是通过指定协议解码后的数据
		if out != nil { // 如果有返回, 写入到连接, 会返回给客户端
			outFrame, _ := el.codec.Encode(c, out)
			el.eventHandler.PreWrite()
			c.write(outFrame)
		}
		switch action {
		case None:
		case Close:
			return el.loopCloseConn(c, nil)
		case Shutdown:
			return errServerShutdown
		}
		if !c.opened {
			return nil
		}
	}
	// 写入
	_, _ = c.inboundBuffer.Write(c.buffer)

	return nil
}

// 给el里的一个连接中写入数据 写到fd
func (el *eventloop) loopWrite(c *conn) error {
	el.eventHandler.PreWrite()

	head, tail := c.outboundBuffer.LazyReadAll()
	// 操作系统写进fd里
	n, err := unix.Write(c.fd, head)
	if err != nil {
		if err == unix.EAGAIN {
			return nil
		}
		return el.loopCloseConn(c, err)
	}
	// 写入的n, 移除n个
	c.outboundBuffer.Shift(n)
	// 如果head写完, tail还有, 再把剩余的写进去
	// 因为是ringbuffer 当read大于write时, 类似[...w(tail), ..., r, ...(head)]
	if len(head) == n && tail != nil {
		n, err = unix.Write(c.fd, tail)
		if err != nil {
			if err == unix.EAGAIN {
				return nil
			}
			return el.loopCloseConn(c, err)
		}
		c.outboundBuffer.Shift(n)
	}

	if c.outboundBuffer.IsEmpty() {
		_ = el.poller.ModRead(c.fd)
	}
	return nil
}

// 关闭el里的连接
func (el *eventloop) loopCloseConn(c *conn, err error) error {
	if !c.outboundBuffer.IsEmpty() && err == nil { // 关闭前先清空出站数据
		_ = el.loopWrite(c)
	}
	err0, err1 := el.poller.Delete(c.fd), unix.Close(c.fd)
	if err0 == nil && err1 == nil { // 关闭成功
		delete(el.connections, c.fd)
		el.minusConnCount()
		switch el.eventHandler.OnClosed(c, err) {
		case Shutdown:
			return errServerShutdown
		}
		c.releaseTCP()
	} else {
		if err0 != nil {
			el.svr.logger.Printf("failed to delete fd:%d from poller, error:%v\n", c.fd, err0)
		}
		if err1 != nil {
			el.svr.logger.Printf("failed to close fd:%d, error:%v\n", c.fd, err1)
		}
	}
	return nil
}

// 通过给连接中写入数据来唤醒
func (el *eventloop) loopWake(c *conn) error {
	//if co, ok := el.connections[c.fd]; !ok || co != c {
	//	return nil // ignore stale wakes.
	//}
	out, action := el.eventHandler.React(nil, c)
	if out != nil {
		frame, _ := el.codec.Encode(c, out)
		c.write(frame)
	}
	return el.handleAction(c, action)
}

func (el *eventloop) loopTicker() {
	var (
		delay time.Duration
		open  bool
		err   error
	)
	for {
		err = el.poller.Trigger(func() (err error) {
			delay, action := el.eventHandler.Tick()
			el.svr.ticktock <- delay
			switch action {
			case None:
			case Shutdown:
				err = errServerShutdown
			}
			return
		})
		if err != nil {
			el.svr.logger.Printf("failed to awake poller with error:%v, stopping ticker\n", err)
			break
		}
		if delay, open = <-el.svr.ticktock; open {
			time.Sleep(delay)
		} else { // chan获取失败时退出
			break
		}
	}
}

func (el *eventloop) handleAction(c *conn, action Action) error {
	switch action {
	case None:
		return nil
	case Close:
		return el.loopCloseConn(c, nil)
	case Shutdown:
		return errServerShutdown
	default:
		return nil
	}
}

func (el *eventloop) loopReadUDP(fd int) error {
	n, sa, err := unix.Recvfrom(fd, el.packet, 0)
	if err != nil || n == 0 {
		if err != nil && err != unix.EAGAIN {
			el.svr.logger.Printf("failed to read UDP packet from fd:%d, error:%v\n", fd, err)
		}
		return nil
	}
	c := newUDPConn(fd, el, sa)
	out, action := el.eventHandler.React(el.packet[:n], c)
	if out != nil {
		el.eventHandler.PreWrite()
		_ = c.sendTo(out)
	}
	switch action {
	case Shutdown:
		return errServerShutdown
	}
	c.releaseUDP()
	return nil
}
