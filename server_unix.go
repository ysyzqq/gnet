// Copyright 2019 Andy Pan. All rights reserved.
// Copyright 2018 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// +build linux darwin netbsd freebsd openbsd dragonfly

package gnet

import (
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/panjf2000/gnet/internal/netpoll"
)
// netty架构
type server struct {
	ln           *listener          // all the listeners
	wg           sync.WaitGroup     // event-loop close WaitGroup 等待所有的事件轮询关闭
	opts         *Options           // options with server
	once         sync.Once          // make sure only signalShutdown once, 对一些确保只做一次的操作都用once
	cond         *sync.Cond         // shutdown signaler 关闭等待
	codec        ICodec             // codec for TCP stream 编码
	logger       Logger             // customized logger for logging info
	ticktock     chan time.Duration // ticker channel
	mainLoop     *eventloop         // main loop for accepting connections 主的事件轮询
	eventHandler EventHandler       // user eventHandler 用户传入的事件处理
	subLoopGroup IEventLoopGroup    // loops for handling events 子事件轮询组, 里面有多个事件轮询
}

// waitForShutdown waits for a signal to shutdown
// 等待shutdown的信号
func (svr *server) waitForShutdown() {
	svr.cond.L.Lock()
	svr.cond.Wait()
	svr.cond.L.Unlock()
}

// signalShutdown signals a shutdown an begins server closing
func (svr *server) signalShutdown() {
	svr.once.Do(func() {
		svr.cond.L.Lock()
		svr.cond.Signal()
		svr.cond.L.Unlock()
	})
}

// 开始事件轮询
func (svr *server) startLoops() {
	svr.subLoopGroup.iterate(func(i int, el *eventloop) bool {
		svr.wg.Add(1)
		go func() {
			el.loopRun()
			svr.wg.Done() // 事件轮询启动后减1
		}()
		return true
	})
}

func (svr *server) closeLoops() {
	svr.subLoopGroup.iterate(func(i int, el *eventloop) bool {
		_ = el.poller.Close()
		return true
	})
}

// 激活子loop
func (svr *server) startReactors() {
	svr.subLoopGroup.iterate(func(i int, el *eventloop) bool {
		svr.wg.Add(1)
		go func() {
			svr.activateSubReactor(el)
			svr.wg.Done()
		}()
		return true
	})
}

func (svr *server) activateLoops(numEventLoop int) error {
	// Create loops locally and bind the listeners.
	for i := 0; i < numEventLoop; i++ {
		// 打开一个轮询器
		if p, err := netpoll.OpenPoller(); err == nil {
			// 生成el
			el := &eventloop{
				idx:          i,
				svr:          svr,
				codec:        svr.codec,
				poller:       p,
				packet:       make([]byte, 0x10000),
				connections:  make(map[int]*conn),
				eventHandler: svr.eventHandler,
			}
			// udp或port重用时, 每个loop的轮询器都会给listener添加可读事件, 不同于tcp模式
			// 给listener添加可读事件
			_ = el.poller.AddRead(svr.ln.fd)
			// 注册到group中
			svr.subLoopGroup.register(el)
		} else {
			return err
		}
	}
	// Start loops in background
	svr.startLoops()
	return nil
}

func (svr *server) activateReactors(numEventLoop int) error {
	for i := 0; i < numEventLoop; i++ {
		if p, err := netpoll.OpenPoller(); err == nil {
			el := &eventloop{
				idx:          i,
				svr:          svr,
				codec:        svr.codec,
				poller:       p,
				packet:       make([]byte, 0x10000),
				connections:  make(map[int]*conn),
				eventHandler: svr.eventHandler,
			}
			svr.subLoopGroup.register(el)
		} else {
			return err
		}
	}
	// Start sub reactors.
	// 启动副反应堆
	svr.startReactors()
	// 启动主反应堆
	if p, err := netpoll.OpenPoller(); err == nil {
		el := &eventloop{
			idx:    -1,
			poller: p,
			svr:    svr,
		}
		// 只有主loop的轮询器才会给ln添加可读事件
		_ = el.poller.AddRead(svr.ln.fd)
		// 主
		svr.mainLoop = el
		// Start main reactor.
		svr.wg.Add(1)
		go func() {
			svr.activateMainReactor()
			svr.wg.Done()
		}()
	} else {
		return err
	}
	return nil
}
// 启动服务
func (svr *server) start(numEventLoop int) error {
	// 如果是数据包连接
	if svr.opts.ReusePort || svr.ln.pconn != nil {
		return svr.activateLoops(numEventLoop)
	}
	return svr.activateReactors(numEventLoop)
}

func (svr *server) stop() {
	svr.logger.Printf("启动成功") // 阻塞在这里
	// Wait on a signal for shutdown
	svr.waitForShutdown()
	svr.logger.Printf("收到关闭信号")

	// Notify all loops to close by closing all listeners
	// 给所有的子事件轮询添加一个关闭的异步任务
	svr.subLoopGroup.iterate(func(i int, el *eventloop) bool {
		sniffErrorAndLog(el.poller.Trigger(func() error {
			return errServerShutdown
		}))
		return true
	})

	if svr.mainLoop != nil {
		svr.ln.close()
		sniffErrorAndLog(svr.mainLoop.poller.Trigger(func() error {
			return errServerShutdown
		}))
	}

	// Wait on all loops to complete reading events
	svr.wg.Wait()
	// 关闭轮询器
	svr.closeLoops()
	// 关闭主轮询
	if svr.mainLoop != nil {
		sniffErrorAndLog(svr.mainLoop.poller.Close())
	}
}

func serve(eventHandler EventHandler, listener *listener, options *Options) error {
	// Figure out the correct number of loops/goroutines to use.
	// 设置副事件轮询的数量
	numEventLoop := 1
	if options.Multicore {
		numEventLoop = runtime.NumCPU()
	}
	if options.NumEventLoop > 0 {
		numEventLoop = options.NumEventLoop
	}

	svr := new(server)
	svr.opts = options
	svr.eventHandler = eventHandler
	svr.ln = listener

	// 根据负载均衡策略生成对应的事件轮询组
	switch options.LB {
	case RoundRobin:
		svr.subLoopGroup = new(roundRobinEventLoopGroup)
	case LeastConnections:
		svr.subLoopGroup = new(leastConnectionsEventLoopGroup)
	case SourceAddrHash:
		svr.subLoopGroup = new(sourceAddrHashEventLoopGroup)
	}

	svr.cond = sync.NewCond(&sync.Mutex{})
	svr.ticktock = make(chan time.Duration, 1) // 1个缓冲
	svr.logger = func() Logger {
		if options.Logger == nil {
			return defaultLogger
		}
		return options.Logger
	}()
	// 编解码器
	svr.codec = func() ICodec {
		if options.Codec == nil {
			return new(BuiltInFrameCodec)
		}
		return options.Codec
	}()
	// 再包一层
	server := Server{
		svr:          svr,
		Multicore:    options.Multicore,
		Addr:         listener.lnaddr,
		NumEventLoop: numEventLoop,
		ReusePort:    options.ReusePort,
		TCPKeepAlive: options.TCPKeepAlive,
	}
	switch svr.eventHandler.OnInitComplete(server) {
	case None:
	case Shutdown:
		return nil
	}
	defer svr.eventHandler.OnShutdown(server)

	// 生成系统信号的关闭chan
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer close(shutdown)

	go func() {
		if <-shutdown == nil {
			return
		}
		// 收到关闭信号后关闭
		svr.signalShutdown()
	}()

	// 启动
	if err := svr.start(numEventLoop); err != nil {
		svr.closeLoops()
		svr.logger.Printf("gnet server is stoping with error: %v\n", err)
		return err
	}
	defer svr.stop()

	return nil
}
