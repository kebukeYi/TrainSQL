package main

import (
	"bufio"
	"fmt"
	"github.com/kebukeYi/TrainSQL/sql"
	"github.com/kebukeYi/TrainSQL/storage"
	"io"
	"net"
	"os"
	"strings"
)

type CmdHandler func(req *Request, session *sql.Session) *Response

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
	server.registerHandler(CmdSQL, handleSQL)
	server.registerHandler(CmdExit, handleExit)

	return server
}

func (s *TCPServer) registerHandler(cmd string, handler CmdHandler) {
	s.handlers[cmd] = handler
}

func (s *TCPServer) Start() error {
	// 1.本地服务先起来;
	memoryStorage := storage.NewMemoryStorage()
	serverManager := sql.NewServer(memoryStorage)
	defer serverManager.Close()
	// 2.监听TCP端口;
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("监听失败;%v", err)
	}
	s.listener = listener
	fmt.Printf("TrainSQL服务器启动成功, 监听地址:%s\n", s.addr)
	fmt.Println("等待客户端连接...")

	// 处理停止信号;
	go func() {
		<-s.stopChan
		serverManager.Close()
		s.listener.Close()
		fmt.Println("服务器已停止")
	}()

	// 循环接受客户端连接;
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopChan:
				return nil
			default:
				fmt.Printf("接受连接失败: %v\n", err)
				continue
			}
		}
		session := serverManager.Session()
		// 每个客户端连接启动独立goroutine处理;
		go s.handleClient(conn, session)
	}
}

// 处理单个客户端连接
func (s *TCPServer) handleClient(conn net.Conn, session *sql.Session) {
	// 延迟关闭连接
	defer func() {
		conn.Close()
		fmt.Printf("客户端 %s 断开连接\n", conn.RemoteAddr())
	}()

	welcomeMsg := "trainSQL>>"
	conn.Write([]byte(welcomeMsg))

	// 读取客户端输入;
	reader := bufio.NewReader(conn)
	buf := make([]byte, 10240)
	for {
		buf = buf[:cap(buf)]
		// 按字节读取, 直到遇到EOF;
		n, err := reader.Read(buf)
		fmt.Println("接收到客户端数据长度:", n)
		if err != nil {
			if err == io.EOF {
				fmt.Println("\n与客户端的连接已断开")
			} else {
				fmt.Printf("\n读取客户端输入失败: %v\n", err)
			}
			return
		}
		if n < 4 {
			resp := &Response{Success: false, Msg: "请输入有效指令"}
			conn.Write([]byte(resp.Serialize()))
			conn.Write([]byte(welcomeMsg))
			continue
		}
		// 如果是exit指令, 断开连接;
		if string(buf[0:4]) == CmdExit {
			break
		}
		data := string(buf[:n])
		// 解析请求;
		req, err := ParseRequest(data)
		if err != nil {
			resp := &Response{Success: false, Msg: err.Error()}
			conn.Write([]byte(resp.Serialize()))
			conn.Write([]byte(welcomeMsg))
			continue
		}

		// 分发请求: 根据指令类型找到对应的处理器;
		handler, ok := s.handlers[req.Cmd]
		if !ok {
			resp := &Response{
				Success: false,
				Msg:     fmt.Sprintf("不支持的指令：%s，支持的指令：%s", req.Cmd, strings.Join([]string{CmdSQL, CmdExit}, ", ")),
			}
			conn.Write([]byte(resp.Serialize()))
			conn.Write([]byte(welcomeMsg))
			continue
		}

		// 执行处理器并返回响应;
		resp := handler(req, session)
		conn.Write([]byte(resp.Serialize()))
		conn.Write([]byte(welcomeMsg))

		buf = buf[:0]
	}
}

// Stop 停止服务器
func (s *TCPServer) Stop() {
	close(s.stopChan)
}

// -------------------------- 指令处理器实现 --------------------------
// 处理查询指令
func handleSQL(req *Request, session *sql.Session) *Response {
	if req.Args == "" {
		return &Response{Success: false, Msg: "指令缺少参数（如 query: select * from user）"}
	}
	resultSet := session.Execute(req.Args)
	// 模拟查询逻辑
	return &Response{
		Success: true,
		Msg:     resultSet.ToString(),
	}
}

// 处理退出指令
func handleExit(_ *Request, _ *sql.Session) *Response {
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
