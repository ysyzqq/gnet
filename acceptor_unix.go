// Copyright 2019 Andy Pan. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// +build linux darwin netbsd freebsd openbsd dragonfly

package gnet

import "golang.org/x/sys/unix"

// 主反应堆接收新的连接
func (svr *server) acceptNewConnection(fd int) error {
	nfd, sa, err := unix.Accept(fd) // 返回os提供的sock地址和对应的fd
	if err != nil {
		if err == unix.EAGAIN {
		return err
	}
	return nil
}
	if err := unix.SetNonblock(nfd, true); err != nil {
		return err
	}
	// 通过负载均衡返回的副el
	el := svr.subLoopGroup.next(nfd)
	c := newTCPConn(nfd, el, sa) // 生成一个tcp连接
	_ = el.poller.Trigger(func() (err error) {
		// 在事件轮询中提交fd的可读事件
		if err = el.poller.AddRead(nfd); err != nil {
			return
		}
		// 添加连接map
		el.connections[nfd] = c
		el.plusConnCount()
		// 开始事件轮询
		err = el.loopOpen(c)
		return
	})
	return nil
}
