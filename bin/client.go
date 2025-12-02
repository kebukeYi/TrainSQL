package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
)

// TCPClient 客户端结构体
type TCPClient struct {
	addr string   // 服务器地址（如 127.0.0.1:8888）
	conn net.Conn // TCP连接
}

// NewTCPClient 创建客户端实例
func NewTCPClient(addr string) *TCPClient {
	return &TCPClient{addr: addr}
}

// Connect 连接服务器
func (c *TCPClient) Connect() error {
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return fmt.Errorf("连接服务器失败：%v", err)
	}
	c.conn = conn
	// fmt.Printf("成功连接到服务器 %s\n", c.addr)
	return nil
}

// Interact 交互逻辑（读取服务器响应 + 发送指令）
func (c *TCPClient) Interact() {
	// 启动goroutine读取服务器响应
	go func() {
		reader := bufio.NewReader(c.conn)
		for {
			// 按字节读取,直到遇到EOF;
			buf := make([]byte, 1024)
			n, err := reader.Read(buf)
			if err != nil {
				if err == io.EOF {
					fmt.Println("\n与服务器的连接已断开")
				} else {
					fmt.Printf("\n读取服务器响应失败：%v\n", err)
				}
				os.Exit(0)
			}
			// 直接打印原始字节（保留所有换行符）
			fmt.Print(string(buf[:n]))
		}
	}()

	// 读取用户输入并发送给服务器
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		if input == "" {
			continue
		}
		// 发送指令（追加换行符，符合协议）
		_, err := c.conn.Write([]byte(input + "\n"))
		if err != nil {
			fmt.Printf("发送指令失败：%v\n", err)
			break
		}
		// 如果是exit指令，退出循环
		if trimSpace(input) == CmdExit {
			break
		}
	}

	// 关闭连接
	c.conn.Close()
}

// 客户端主函数
func main() {
	client := NewTCPClient("127.0.0.1:8888")

	// 连接服务器
	if err := client.Connect(); err != nil {
		fmt.Printf("客户端启动失败：%v\n", err)
		os.Exit(1)
	}

	// 开始交互
	client.Interact()
}
