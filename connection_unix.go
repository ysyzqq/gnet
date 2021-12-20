// Copyright 2019 Andy Pan. All rights reserved.
// Copyright 2018 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// +build linux darwin netbsd freebsd openbsd dragonfly

package gnet

import (
	"net"

	"github.com/panjf2000/gnet/internal/netpoll"
	"github.com/panjf2000/gnet/pool/bytebuffer"
	prb "github.com/panjf2000/gnet/pool/ringbuffer"
	"github.com/panjf2000/gnet/ringbuffer"
	"golang.org/x/sys/unix"
)

type conn struct {
	fd             int                    // file descriptor 内部socket的fd
	sa             unix.Sockaddr          // remote socket address 远程socket地址
	ctx            interface{}            // user-defined context 用户定义的上下文
	loop           *eventloop             // connected event-loop 所在的事件轮询
	buffer         []byte                 // reuse memory of inbound data as a temporary buffer 临时缓冲, 操作系统从fd读取
	codec          ICodec                 // codec for TCP
	opened         bool                   // connection opened event fired
	localAddr      net.Addr               // local addr
	remoteAddr     net.Addr               // remote addr
	byteBuffer     *bytebuffer.ByteBuffer // bytes buffer for buffering current packet and data in ring-buffer
	inboundBuffer  *ringbuffer.RingBuffer // buffer for data from client 入站缓冲 来自客户端
	outboundBuffer *ringbuffer.RingBuffer // buffer for data that is ready to write to client
}

func newTCPConn(fd int, el *eventloop, sa unix.Sockaddr) *conn {
	return &conn{
		fd:             fd,
		sa:             sa,
		loop:           el,
		codec:          el.codec,
		inboundBuffer:  prb.Get(),
		outboundBuffer: prb.Get(),
	}
}

// 释放tcp连接
func (c *conn) releaseTCP() {
	c.opened = false
	c.sa = nil
	c.ctx = nil
	c.buffer = nil
	c.localAddr = nil
	c.remoteAddr = nil
	prb.Put(c.inboundBuffer)
	prb.Put(c.outboundBuffer)
	c.inboundBuffer = nil
	c.outboundBuffer = nil
	bytebuffer.Put(c.byteBuffer)
	c.byteBuffer = nil
}

func newUDPConn(fd int, el *eventloop, sa unix.Sockaddr) *conn {
	return &conn{
		fd:         fd,
		sa:         sa,
		localAddr:  el.svr.ln.lnaddr,
		remoteAddr: netpoll.SockaddrToUDPAddr(sa),
	}
}

func (c *conn) releaseUDP() {
	c.ctx = nil
	c.localAddr = nil
	c.remoteAddr = nil
}

// 打开连接
func (c *conn) open(buf []byte) {
	n, err := unix.Write(c.fd, buf)
	if err != nil {
		_, _ = c.outboundBuffer.Write(buf)
		return
	}

	if n < len(buf) {
		_, _ = c.outboundBuffer.Write(buf[n:])
	}
}

// 读取连接数据, 通过编解码器来读取
func (c *conn) read() ([]byte, error) {
	return c.codec.Decode(c)
}

func (c *conn) write(buf []byte) {
	if !c.outboundBuffer.IsEmpty() {
		_, _ = c.outboundBuffer.Write(buf)
		return
	}
	n, err := unix.Write(c.fd, buf)
	if err != nil {
		if err == unix.EAGAIN { // 资源临时不可用, 重试
			_, _ = c.outboundBuffer.Write(buf)
			_ = c.loop.poller.ModReadWrite(c.fd) // 跟新fd在事件轮询中的读写事件
			return
		}
		_ = c.loop.loopCloseConn(c, err)
		return
	}
	if n < len(buf) { // 其他数据
		_, _ = c.outboundBuffer.Write(buf[n:])
		_ = c.loop.poller.ModReadWrite(c.fd)
	}
}

func (c *conn) sendTo(buf []byte) error {
	return unix.Sendto(c.fd, buf, 0, c.sa)
}

// ================================= Public APIs of gnet.Conn =================================

func (c *conn) Read() []byte {
	if c.inboundBuffer.IsEmpty() {
		return c.buffer
	}
	c.byteBuffer = c.inboundBuffer.WithByteBuffer(c.buffer)
	return c.byteBuffer.Bytes()
}

// 重置连接的入站缓冲
func (c *conn) ResetBuffer() {
	c.buffer = c.buffer[:0]
	c.inboundBuffer.Reset()
	bytebuffer.Put(c.byteBuffer)
	c.byteBuffer = nil
}

// 读取n字节
func (c *conn) ReadN(n int) (size int, buf []byte) {
	inBufferLen := c.inboundBuffer.Length()
	tempBufferLen := len(c.buffer)
	if totalLen := inBufferLen + tempBufferLen; totalLen < n || n <= 0 {
		n = totalLen
	}
	size = n
	if c.inboundBuffer.IsEmpty() {
		buf = c.buffer[:n]
		return
	}
	head, tail := c.inboundBuffer.LazyRead(n)
	c.byteBuffer = bytebuffer.Get() // 从池里拿出一个字节缓冲
	_, _ = c.byteBuffer.Write(head)
	_, _ = c.byteBuffer.Write(tail)
	if inBufferLen >= n {
		buf = c.byteBuffer.Bytes()
		return
	}

	restSize := n - inBufferLen
	_, _ = c.byteBuffer.Write(c.buffer[:restSize])
	buf = c.byteBuffer.Bytes()
	return
}

func (c *conn) ShiftN(n int) (size int) {
	inBufferLen := c.inboundBuffer.Length()
	tempBufferLen := len(c.buffer)
	if inBufferLen+tempBufferLen < n || n <= 0 {
		c.ResetBuffer()
		size = inBufferLen + tempBufferLen
		return
	}
	size = n
	if c.inboundBuffer.IsEmpty() {
		c.buffer = c.buffer[n:]
		return
	}

	bytebuffer.Put(c.byteBuffer)
	c.byteBuffer = nil

	if inBufferLen >= n {
		c.inboundBuffer.Shift(n)
		return
	}
	c.inboundBuffer.Reset()

	restSize := n - inBufferLen
	c.buffer = c.buffer[restSize:]
	return
}

func (c *conn) BufferLength() int {
	return c.inboundBuffer.Length() + len(c.buffer)
}

// 异步写入, el里(epoll 事件轮询)添加一个异步任务
func (c *conn) AsyncWrite(buf []byte) (err error) {
	var encodedBuf []byte
	if encodedBuf, err = c.codec.Encode(c, buf); err == nil {
		return c.loop.poller.Trigger(func() error {
			if c.opened {
				c.write(encodedBuf)
			}
			return nil
		})
	}
	return
}

func (c *conn) SendTo(buf []byte) error {
	return c.sendTo(buf)
}

func (c *conn) Wake() error {
	return c.loop.poller.Trigger(func() error {
		return c.loop.loopWake(c)
	})
}

func (c *conn) Close() error {
	return c.loop.poller.Trigger(func() error {
		return c.loop.loopCloseConn(c, nil)
	})
}

func (c *conn) Context() interface{}       { return c.ctx }
func (c *conn) SetContext(ctx interface{}) { c.ctx = ctx }
func (c *conn) LocalAddr() net.Addr        { return c.localAddr }
func (c *conn) RemoteAddr() net.Addr       { return c.remoteAddr }
