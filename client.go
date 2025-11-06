package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
}

func NewClient(host, port string) (*Client, error) {
	c, err := net.Dial("tcp", host+":"+port)
	if err != nil {
		return nil, err
	}
	return &Client{conn: c, reader: bufio.NewReader(c)}, nil
}

func (c *Client) execute(args ...string) (string, error) {
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	_, err := c.conn.Write(buf.Bytes())
	if err != nil {
		return "", err
	}

	line, err := c.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)

	if strings.HasPrefix(line, "-") {
		return "", fmt.Errorf(line[1:])
	}
	if strings.HasPrefix(line, ":") {
		return line[1:], nil
	}
	if strings.HasPrefix(line, "$") {
		size, _ := strconv.Atoi(line[1:])
		if size == -1 {
			return "", nil
		}
		data := make([]byte, size+2)
		c.reader.Read(data)
		return string(data[:size]), nil
	}
	return line, nil
}

func (c *Client) Close() {
	c.conn.Close()
}

func main() {
	fmt.Println("Welcome to Go Environment, Type 'exit' or 'quit' to quit")
	client, err := NewClient("127.0.0.1", "31337")
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}
	defer client.Close()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("GORedis: ")
		if !scanner.Scan() {
			fmt.Println("\nExiting...")
			break
		}
		cmdLine := strings.TrimSpace(scanner.Text())
		if cmdLine == "" {
			continue
		}
		if strings.ToLower(cmdLine) == "exit" || strings.ToLower(cmdLine) == "quit" {
			fmt.Println("Goodbye!")
			break
		}

		parts := strings.Fields(cmdLine)
		resp, err := client.execute(parts...)
		if err != nil {
			fmt.Println("(error)", err)
		} else {
			fmt.Println(resp)
		}
	}
}
