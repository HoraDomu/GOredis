package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

type Server struct {
	addr    string
	kv      map[string][]byte
	lock    sync.RWMutex
	clients sync.WaitGroup
}

func NewServer(addr string) *Server {
	return &Server{
		addr: addr,
		kv:   make(map[string][]byte),
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		data, err := s.readRequest(reader)
		if err != nil {
			return
		}

		resp := s.executeCommand(data)
		s.writeResponse(conn, resp)
	}
}

func (s *Server) readRequest(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')

	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, fmt.Errorf("empty request")
	}

	if line[0] == '*' {
		num, _ := strconv.Atoi(line[1:])
		args := make([]string, num)
		for i := 0; i < num; i++ {
			reader.ReadString('\n') // skip $len
			arg, _ := reader.ReadString('\n')
			args[i] = strings.TrimSpace(arg)
		}
		return args, nil
	}

	return strings.Fields(line), nil
}

func (s *Server) executeCommand(args []string) interface{} {
	if len(args) == 0 {
		return fmt.Errorf("missing command")
	}

	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "GET":
		s.lock.RLock()
		defer s.lock.RUnlock()
		return s.kv[args[1]]
	case "SET":
		s.lock.Lock()
		defer s.lock.Unlock()
		s.kv[args[1]] = []byte(args[2])
		return 1

	case "DEL":
		s.lock.Lock()
		defer s.lock.Unlock()
		if _, ok := s.kv[args[1]]; ok {
			delete(s.kv, args[1])
			return 1
		}
		return 0
	case "FLUSH":
		s.lock.Lock()
		defer s.lock.Unlock()
		count := len(s.kv)
		s.kv = make(map[string][]byte)
		return count

	case "MGET":
		s.lock.RLock()
		defer s.lock.Unlock()
		for i := 0; i < len(args); i += 2 {
			s.kv[args[i]] = []byte(args[i+1])
		}

		return (len(args) - 1) / 2
	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
}

func (s *Server) writeResponse(conn net.Conn, data interface{}) {
	buf := &bytes.Buffer{}
	switch v := data.(type) {
	case int:
		buf.WriteString(fmt.Sprintf(":%d\r\n", v))
	case string:
		buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
	case []byte:
		buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
	case []interface{}:
		buf.WriteString(fmt.Sprintf("*%d\r\n", len(v)))
		for _, item := range v {
			if item == nil {
				buf.WriteString("$-1\r\n")
			} else if b, ok := item.([]byte); ok {
				buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(b), b))
			}
		}
	case error:
		buf.WriteString(fmt.Sprintf("-%s\r\n", v.Error()))
	default:
		buf.WriteString("$-1\r\n")
	}
	conn.Write(buf.Bytes())
}
func (s *Server) Run() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	fmt.Println("Server listeting on", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go s.handleConnection(conn)
	}
}

func main() {
	server := NewServer("127.0.0.1:31337")
	server.Run()
}
