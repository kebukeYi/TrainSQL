package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

type TCPClient struct {
	addr string   // 服务器地址（如 127.0.0.1:8888）
	conn net.Conn // TCP连接
}

func NewTCPClient(addr string) *TCPClient {
	return &TCPClient{addr: addr}
}

func (c *TCPClient) Connect() error {
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return fmt.Errorf("连接服务器失败：%v", err)
	}
	c.conn = conn
	return nil
}

// Interact 交互逻辑（读取服务器响应 + 发送指令）
func (c *TCPClient) Interact() {
	defer c.conn.Close()
	// 启动goroutine读取服务器响应;
	buf := make([]byte, 1024)
	go func() {
		reader := bufio.NewReader(c.conn)
		// 按字节读取,直到遇到EOF;
		buf = buf[:cap(buf)]
		for {
			buf = buf[:cap(buf)]
			n, err := reader.Read(buf)
			if err != nil {
				if err == io.EOF {
					fmt.Println("\n与服务器的连接已断开")
				} else {
					fmt.Printf("\n读取服务器响应失败: %v\n", err)
				}
				os.Exit(0)
			}
			fmt.Printf("\n服务器响应长度: %d \n", n)
			// 直接打印原始字节内容(保留所有换行符)
			fmt.Print(string(buf[:n]))
			buf = buf[:0]
		}
	}()

	// 读取用户输入并发送给服务器;
	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		text := scan.Text()
		if strings.TrimSpace(text) == CmdExit {
			fmt.Println("收到退出指令")
			return
		}
		fmt.Println("用户输入的长度: ", len(text))
		// 发送指令
		_, err := c.conn.Write([]byte(text))
		if err != nil {
			fmt.Printf("发送指令失败：%v\n", err)
			break
		}
	}
}

func main() {
	var serverAddr string
	flag.StringVar(&serverAddr, "s", "127.0.0.1:8888", "服务器地址")
	client := NewTCPClient(serverAddr)

	if err := client.Connect(); err != nil {
		fmt.Printf("客户端启动失败：%v\n", err)
		os.Exit(1)
	}

	client.Interact()
}
