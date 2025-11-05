package main

import (
	"bufio"
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
	}
}
