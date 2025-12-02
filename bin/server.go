package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

// CmdHandler 指令处理器类型
type CmdHandler func(req *Request) *Response

// TCPServer 服务器结构体
type TCPServer struct {
	addr     string                // 监听地址（如 :8888）
	handlers map[string]CmdHandler // 指令处理器映射表
	listener net.Listener          // TCP监听器
	stopChan chan struct{}         // 停止信号
}

// NewTCPServer 创建服务器实例
func NewTCPServer(addr string) *TCPServer {
	server := &TCPServer{
		addr:     addr,
		handlers: make(map[string]CmdHandler),
		stopChan: make(chan struct{}),
	}

	// 注册内置指令处理器（请求分发的核心）
	server.registerHandler(CmdQuery, handleQuery)
	server.registerHandler(CmdInsert, handleInsert)
	server.registerHandler(CmdExit, handleExit)

	return server
}

// 注册指令处理器
func (s *TCPServer) registerHandler(cmd string, handler CmdHandler) {
	s.handlers[cmd] = handler
}

// Start 启动服务器
func (s *TCPServer) Start() error {
	// 监听TCP端口
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("监听失败：%v", err)
	}
	s.listener = listener
	fmt.Printf("TrainSQL服务器启动成功，监听地址：%s\n", s.addr)
	fmt.Println("等待客户端连接...")

	// 处理停止信号
	go func() {
		<-s.stopChan
		s.listener.Close()
		fmt.Println("服务器已停止")
	}()

	// 循环接受客户端连接
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopChan:
				return nil
			default:
				fmt.Printf("接受连接失败：%v\n", err)
				continue
			}
		}
		// 每个客户端连接启动独立goroutine处理
		go s.handleClient(conn)
	}
}

// 处理单个客户端连接
func (s *TCPServer) handleClient(conn net.Conn) {
	// 延迟关闭连接
	defer func() {
		conn.Close()
		fmt.Printf("客户端 %s 断开连接\n", conn.RemoteAddr())
	}()

	// 客户端连接成功，发送欢迎信息（模仿MySQL的欢迎包）
	//welcomeMsg := "Welcome to TrainSQL Server (version 1.0)\n" +
	//	"支持的指令：\n" +
	//	"  1. query: [SQL查询语句] （如 query: select * from user）\n" +
	//	"  2. insert: [SQL插入语句] （如 insert: insert into user values (1, 'zhangsan')）\n" +
	//	"  3. exit: 退出连接\n" +
	//	"请输入指令：\n"

	welcomeMsg := "trainSQL >> "
	conn.Write([]byte(welcomeMsg))

	// 读取客户端输入
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		// 获取客户端发送的指令（去除首尾空格/换行）
		data := trimSpace(scanner.Text())
		if data == "" {
			conn.Write([]byte(new(Response).Serialize()))
			conn.Write([]byte(welcomeMsg))
			continue
		}

		// 解析请求
		req, err := ParseRequest(data)
		if err != nil {
			resp := &Response{Success: false, Msg: err.Error()}
			conn.Write([]byte(resp.Serialize()))
			conn.Write([]byte(welcomeMsg))
			continue
		}

		// 分发请求：根据指令类型找到对应的处理器
		handler, ok := s.handlers[req.Cmd]
		if !ok {
			resp := &Response{
				Success: false,
				Msg:     fmt.Sprintf("不支持的指令：%s，支持的指令：%s", req.Cmd, strings.Join([]string{CmdQuery, CmdInsert, CmdExit}, ", ")),
			}
			conn.Write([]byte(resp.Serialize()))
			conn.Write([]byte(welcomeMsg))
			continue
		}

		// 执行处理器并返回响应
		resp := handler(req)
		conn.Write([]byte(resp.Serialize()))
		conn.Write([]byte(welcomeMsg))

		// 如果是exit指令，断开连接
		if req.Cmd == CmdExit {
			break
		}
	}

	// 处理读取错误
	if err := scanner.Err(); err != nil {
		fmt.Printf("客户端 %s 读取数据失败：%v\n", conn.RemoteAddr(), err)
	}
}

// Stop 停止服务器
func (s *TCPServer) Stop() {
	close(s.stopChan)
}

// -------------------------- 指令处理器实现 --------------------------
// 处理查询指令
func handleQuery(req *Request) *Response {
	if req.Args == "" {
		return &Response{Success: false, Msg: "query指令缺少参数（如 query: select * from user）"}
	}
	// 模拟查询逻辑
	return &Response{
		Success: true,
		Msg:     fmt.Sprintf("执行查询成功：%s，返回结果：\nid | name  | age\n1  | zhangsan | 20\n2  | lisi   | 25", req.Args),
	}
}

// 处理插入指令
func handleInsert(req *Request) *Response {
	if req.Args == "" {
		return &Response{Success: false, Msg: "insert指令缺少参数（如 insert: insert into user values (1, 'zhangsan')）"}
	}
	// 模拟插入逻辑
	return &Response{
		Success: true,
		Msg:     fmt.Sprintf("执行插入成功：%s，影响行数：1", req.Args),
	}
}

// 处理退出指令
func handleExit(req *Request) *Response {
	return &Response{Success: true, Msg: "即将断开连接！"}
}

// 服务器主函数
func main() {
	server := NewTCPServer(":8888")

	// 捕获退出信号（Ctrl+C）
	go func() {
		<-make(chan os.Signal, 1) // 简化处理，实际可监听syscall.SIGINT
		server.Stop()
	}()

	// 启动服务器
	if err := server.Start(); err != nil {
		fmt.Printf("服务器启动失败：%v\n", err)
		os.Exit(1)
	}
}
