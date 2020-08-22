// Copyright 2019 Andy Pan. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// +build linux

package gnet

import "github.com/panjf2000/gnet/internal/netpoll"

// 参考netty架构
// 主反应堆处理连接, 分发连接给副反应堆处理
func (svr *server) activateMainReactor() {
	// 主反应堆退出后发送关闭信号
	defer svr.signalShutdown()
	// 开启轮询, 使用os的轮询库, 所以这个方法应该被新开一个协程调用 go activateMainReactor()
	// 退出时返回错误, 并打印
	svr.logger.Printf("main reactor exits with error:%v\n", svr.mainLoop.poller.Polling(func(fd int, ev uint32) error {
		// 主反应堆的回调处理新的连接
		return svr.acceptNewConnection(fd)
	}))
}

// 激活副反应堆
func (svr *server) activateSubReactor(el *eventloop) {
	// 副反应堆退出时, 事件轮询关闭所有连接
	defer func() {
		el.closeAllConns()
		if el.idx == 0 && svr.opts.Ticker {
			close(svr.ticktock)
		}
		svr.signalShutdown()
	}()

	// 第一个事件轮询会额外开启一个时钟
	// 每隔一段时间将触发用户传入的eventHandler的Ticker的方法加入异步任务队列
	if el.idx == 0 && svr.opts.Ticker {
		go el.loopTicker()
	}

	// 开启事件轮询
	svr.logger.Printf("event-loop:%d exits with error:%v\n", el.idx, el.poller.Polling(func(fd int, ev uint32) error {
		// 副反应堆的回调, 处理主反应堆分发过来的连接
		if c, ack := el.connections[fd]; ack {
			switch c.outboundBuffer.IsEmpty() {
			// Don't change the ordering of processing EPOLLOUT | EPOLLRDHUP / EPOLLIN unless you're 100%
			// sure what you're doing!
			// Re-ordering can easily introduce bugs and bad side-effects, as I found out painfully in the past.
			case false:
				if ev&netpoll.OutEvents != 0 { // 输出事件
					return el.loopWrite(c)
				}
				return nil
			case true:
				if ev&netpoll.InEvents != 0 { // 输入
					return el.loopRead(c)
				}
				return nil
			}
		}
		return nil
	}))
}
