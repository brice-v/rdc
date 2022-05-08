package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/chzyer/readline"
)

// Delimeter is is used to determine the end of a command transmission
const Delimeter = "\r\n"

// check will print the error message (if not nil) and exit gracefully
func check(err error) {
	if err == nil {
		return
	}
	fmt.Printf("Fatal Client Error: %v\n", err)
	os.Exit(1)
}

// RedisClient will allow us to connect to a RedisServer and send commands
// via the command line
//
// In the future these requests and responses will be encoded in RESP
// https://redis.io/topics/protocol
type RedisClient struct {
	c  net.Conn
	rl *readline.Instance
}

// NewRedisClient will return a pointer to a RedisClient and dial the
// loopback address with the specified port (this port must match the redis
// server)
// It will also instantiate the readline prompt for cli messages
func NewRedisClient(port string) *RedisClient {
	conn, err := net.Dial("tcp", "localhost"+port)
	check(err)
	rl, err := readline.New("rdc" + port + "> ")
	check(err)
	return &RedisClient{c: conn, rl: rl}
}

func readBulkString(r *bufio.Reader) string {
	return ""
}

// processResponse will read from the net.Conn and emit output to stdout for the
// client using the command line interface
func (rc *RedisClient) processResponse() {
	reader := bufio.NewReader(rc.c)
	message, err := reader.ReadString('\r')
	check(err)
	b, err := reader.ReadByte()
	check(err)
	if b != '\n' {
		log.Printf("Read Delimeter Error: Message did not end in `\\r\\n`\n")
		return
	}

	// remove \r from our message
	removedCR := message[:len(message)-1]
	t := message[0]
	m := removedCR[1:]

	switch t {
	case '+':
		// If its a simple string just display as is to the user
		fmt.Printf("(SS) %s\n", m)
	case '-':
		// If its an error just display as is to the user
		fmt.Printf("(ERROR) %s\n", m)
	case ':':
		// If its an int, tell the user in the parentheses and remove the ':'
		fmt.Printf("(INTEGER) %s\n", m)
	case '$':
		bufSize, err := strconv.Atoi(m)
		check(err)
		// Need to add 2 to account for /r/n
		buf := make([]byte, bufSize+2)
		_, err = reader.Read(buf)
		check(err)
		if buf[bufSize+1] != '\n' || buf[bufSize] != '\r' {
			log.Fatal("Improper resp response read from server")
		}
		fmt.Printf("(STRING) %s\n", buf[:bufSize])
	case '*':
		// arraySize, err := strconv.Atoi(m)
		// check(err)
		// array := make([]string, arraySize)
	case 0:
		log.Fatal("ERROR: null byte received")
	default:
		fmt.Println(message)
	}
}

// mbrr - make bulk resp request
// takes a string with space separated command+args? and
// retunrs the correct resp byte slice
func mbrr(req string) []byte {
	splitWs := strings.Split(req, " ")
	var sb strings.Builder
	header := fmt.Sprintf("*%d%s", len(splitWs), Delimeter)
	sb.WriteString(header)
	for _, v := range splitWs {
		l := len(v)
		sb.WriteString(fmt.Sprintf("$%d%s%s%s", l, Delimeter, v, Delimeter))
	}
	// log.Printf("%q", sb.String())
	return []byte(sb.String())
}

func main() {
	client := NewRedisClient(":8081")
	defer client.c.Close()
	for {
		message, err := client.rl.Readline()
		check(err)
		_, err = client.c.Write(mbrr(message))
		check(err)
		if strings.Contains(strings.ToUpper(message), "QUIT") {
			return
		}
		client.processResponse()
	}
}
