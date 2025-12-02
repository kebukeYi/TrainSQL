package main

import "fmt"

// 定义指令类型（模仿MySQL的指令）
const (
	CmdQuery  = "query"  // 查询指令
	CmdInsert = "insert" // 插入指令
	CmdExit   = "exit"   // 退出指令
)

// Request 解析后的客户端请求
type Request struct {
	Cmd  string // 指令类型（query/insert/exit）
	Args string // 指令参数（如 "select * from user"）
}

// Response 服务器响应
type Response struct {
	Success bool   // 是否成功
	Msg     string // 响应信息
}

// Serialize 序列化响应（发给客户端）
func (r *Response) Serialize() string {
	if r.Success {
		return fmt.Sprintf("OK: %s\n", r.Msg)
	}
	return fmt.Sprintf("ERROR: %s\n", r.Msg)
}

// ParseRequest 解析客户端请求（从字符串解析为Request）
func ParseRequest(data string) (*Request, error) {
	// 切割指令和参数（格式：cmd: args）
	sepIdx := -1
	for i, c := range data {
		if c == ':' {
			sepIdx = i
			break
		}
	}
	if sepIdx == -1 {
		return nil, fmt.Errorf("无效指令格式，正确格式：cmd: 参数（如 query: select * from user）")
	}

	cmd := data[:sepIdx]
	args := data[sepIdx+1:]
	// 去除首尾空格
	cmd = trimSpace(cmd)
	args = trimSpace(args)

	if cmd == "" {
		return nil, fmt.Errorf("指令类型不能为空")
	}

	return &Request{
		Cmd:  cmd,
		Args: args,
	}, nil
}

// 辅助函数：去除字符串首尾空格和换行
func trimSpace(s string) string {
	r := []rune(s)
	start := 0
	for start < len(r) && (r[start] == ' ' || r[start] == '\n' || r[start] == '\r') {
		start++
	}
	end := len(r) - 1
	for end >= 0 && (r[end] == ' ' || r[end] == '\n' || r[end] == '\r') {
		end--
	}
	if start > end {
		return ""
	}
	return string(r[start : end+1])
}
