package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Delimeter is used to denote the end of a reply/request
const Delimeter = "\r\n"

// NumDBs is the number of databases for the server to hold
const NumDBs = 10

// ServerVersion is the current version of the software
const ServerVersion = "0.1.0"

// check will print the error message (if not nil) and exit gracefully
func check(err error) {
	if err == nil {
		return
	}
	fmt.Printf("Fatal Server Error: %v\n", err)
	os.Exit(1)
}

// RedisServer is the container object for the server and its connections
type RedisServer struct {
	port string
	addr string
	l    net.Listener
	// sp is the pointer into the store (the select pointer [or store pointer]) that we are referring to
	sp    int64
	store [NumDBs]*DB

	lock sync.Mutex

	conns    map[int]net.Conn
	lastsave int64

	totalConnsReceived uint64
	commandsProcessed  uint64

	timeStarted int64
}

// NewRedisServer returns a pointer to a RedisServer object
func NewRedisServer(port string) *RedisServer {
	// listen on all interfaces
	ln, err := net.Listen("tcp", port)
	check(err)
	fmt.Printf("Listening on Port %s\n", port)

	var store [NumDBs]*DB

	rs := &RedisServer{
		l:           ln,
		port:        port,
		addr:        "localhost",
		store:       store,
		conns:       make(map[int]net.Conn),
		timeStarted: time.Now().Unix(),
	}

	rs.flushall()
	return rs
}

// Listen on the specified tcp port and handle incoming client connections
func (rs *RedisServer) Listen() {
	i := 0
	for {
		// accept connection on port
		conn, err := rs.l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Printf("Listener Temp Accept Error: %v\n", ne)
				continue
			}

			log.Printf("Listener Accept Error: %v\n", err)
			return
		}
		rs.lock.Lock()
		rs.conns[i] = conn
		rs.lock.Unlock()
		atomic.AddUint64(&rs.totalConnsReceived, 1)
		go rs.handleClient(i)
		i++
	}
}

// ExecuteCommand takes a connection a command string and a variable number of args
// the command will be performed on the server and a reply will be written to the
// connection
// Note: this could also use varargs
func (rs *RedisServer) ExecuteCommand(c io.WriteCloser, connIndex int, command string, args []string) bool {
	argsLen := len(args)
	switch command {
	case "PING":
		if argsLen == 0 {
			return replySimpleString(c, "PONG")
		}
		if argsLen == 1 {
			return replySimpleString(c, args[0])
		}
		return replyInvalidNumberOfArgsError(c, command)
	case "QUIT":
		if argsLen != 0 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		c.Close()
		rs.lock.Lock()
		delete(rs.conns, connIndex)
		rs.lock.Unlock()
		return true
	case "INFO":
		if argsLen != 0 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		return replyMultiBulkString(c, rs.info())
	// Persistent Control Commands
	case "SAVE":
		if argsLen != 0 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		rs.save()
		return replyOK(c)
	case "BGSAVE":
		if argsLen != 0 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		go rs.save()
		return replyOK(c)
	case "LASTSAVE":
		return replyInteger(c, fmt.Sprintf("%d", rs.lastsave))
	case "SHUTDOWN":
		rs.lock.Lock()
		defer rs.lock.Unlock()
		rs.save()
		for _, conn := range rs.conns {
			conn.Close()
		}
		rs.l.Close()
		return false
	// Commands Operating on Key Space
	case "KEYS":
		if argsLen != 1 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		val, ok := rs.keys(args[0])
		if !ok {
			return replyInvalidGlobPatternError(c, args[0])
		}
		if len(val) == 0 {
			return replyEmptySetOrList(c)
		}
		return replyMultiBulkString(c, val)
	case "RANDOMKEY":
		if argsLen != 0 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		return replyBulkString(c, rs.random_key())
	case "RENAME":
		if argsLen != 2 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		if args[0] == args[1] {
			return replySimpleError(c, "Keys Must be Different")
		}
		rs.rename(args[0], args[1])
		return replyOK(c)
	case "RENAMENX":
		if argsLen != 2 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		return replyInteger(c, rs.rename_nx(args[0], args[1]))
	case "DBSIZE":
		if argsLen != 0 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		return replyInteger(c, rs.dbsize())
	// Commands Operating on DB
	case "SELECT":
		if argsLen != 1 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		index, err := strconv.Atoi(args[0])
		if err != nil {
			return replyInvalidTypeIntegerError(c)
		}
		if index > 9 || index < 0 {
			return replyInvalidTypeIntegerError(c)
		}
		atomic.StoreInt64(&rs.sp, int64(index))
		return replyOK(c)
	case "MOVE":
		if argsLen != 2 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		dbIndex, err := strconv.Atoi(args[1])
		if err != nil {
			return replyInvalidTypeIntegerError(c)
		}
		return replyInteger(c, rs.move(args[0], dbIndex))
	case "FLUSHDB":
		rs.flushDB()
		return replyOK(c)
	case "FLUSHALL":
		rs.flushall()
		return replyOK(c)
	// Commands Operating on Strings
	case "SET":
		if argsLen != 2 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		// with 2 args we know that we have the correct amount to set a key to a value
		rs.set(args[0], args[1])
		return replyOK(c)
	case "SETNX":
		if argsLen != 2 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		// with 2 args we know that we have the correct amount to set a key to a value
		if _, ok := rs.get(args[0]); !ok {
			rs.set(args[0], args[1])
			return replyInteger(c, "1")
		}
		return replyInteger(c, "0")
	case "GET":
		if argsLen != 1 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		if rs.getDBType(args[0]) != "string" {
			return replyWrongTypeOperationError(c)
		}
		val, _ := rs.get(args[0])
		return replyBulkString(c, val)
	case "EXISTS":
		// TODO: Eventually support variable number of args
		if argsLen != 1 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		t := rs.getDBType(args[0])
		if t != "none" {
			return replyInteger(c, "1")
		}
		return replyInteger(c, "0")
	case "INCR":
		if argsLen != 1 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		typ := rs.getDBType(args[0])
		if typ == "list" || typ == "set" {
			return replyWrongTypeOperationError(c)
		}
		v, ok := rs.get(args[0])
		if !ok {
			rs.set(args[0], "0")
			return replyInteger(c, "0")
		}
		val, err := strconv.Atoi(v)
		if err != nil {
			return replyInvalidTypeIntegerError(c)
		}
		val++
		vs := fmt.Sprintf("%d", val)
		rs.set(args[0], vs)
		return replyInteger(c, vs)
	case "INCRBY":
		if argsLen != 2 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		typ := rs.getDBType(args[0])
		if typ == "list" || typ == "set" {
			return replyWrongTypeOperationError(c)
		}
		v, ok := rs.get(args[0])
		if !ok {
			vv, err := strconv.Atoi(args[1])
			if err != nil {
				return replyInvalidTypeIntegerError(c)
			}
			vs := fmt.Sprintf("%d", vv)
			rs.set(args[0], vs)
			return replyInteger(c, vs)
		}
		val, err := strconv.Atoi(v)
		if err != nil {
			return replyInvalidTypeIntegerError(c)
		}
		vv, err := strconv.Atoi(args[1])
		if err != nil {
			return replyInvalidTypeIntegerError(c)
		}
		val += vv
		vs := fmt.Sprintf("%d", val)
		rs.set(args[0], vs)
		return replyInteger(c, vs)
	case "DECR":
		if argsLen != 1 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		typ := rs.getDBType(args[0])
		if typ == "list" || typ == "set" {
			return replyWrongTypeOperationError(c)
		}
		v, ok := rs.get(args[0])
		if !ok {
			rs.set(args[0], "-1")
			replyInteger(c, "-1")
		}
		val, err := strconv.Atoi(v)
		if err != nil {
			return replyInvalidTypeIntegerError(c)
		}
		val--
		vs := fmt.Sprintf("%d", val)
		rs.set(args[0], vs)
		return replyInteger(c, vs)
	case "DECRBY":
		if argsLen != 2 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		typ := rs.getDBType(args[0])
		if typ == "list" || typ == "set" {
			return replyWrongTypeOperationError(c)
		}
		if v, ok := rs.get(args[0]); ok {
			val, err := strconv.Atoi(v)
			if err != nil {
				return replyInvalidTypeIntegerError(c)
			}
			vv, err := strconv.Atoi(args[1])
			if err != nil {
				return replyInvalidTypeIntegerError(c)
			}
			val -= vv
			vs := fmt.Sprintf("%d", val)
			rs.set(args[0], vs)
			return replyInteger(c, vs)
		}
		vv, err := strconv.Atoi(args[1])
		if err != nil {
			return replyInvalidTypeIntegerError(c)
		}
		vs := fmt.Sprintf("%d", -vv)
		rs.set(args[0], vs)
		return replyInteger(c, vs)
	case "DEL":
		if argsLen != 1 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		exists := rs.del(args[0])
		if exists {
			return replyInteger(c, "1")
		}
		return replyInteger(c, "0")
	case "TYPE":
		if argsLen != 1 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		t := rs.getDBType(args[0])
		c.Write([]byte(t + Delimeter))
		return true
	// Commands Operating on Lists
	case "LPUSH":
		if argsLen != 2 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		typ := rs.getDBType(args[0])
		if typ == "none" || typ == "list" {
			val := rs.lpush(args[0], args[1])
			return replyInteger(c, val)
		}
		return replyWrongTypeOperationError(c)
	case "RPUSH":
		if argsLen != 2 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		typ := rs.getDBType(args[0])
		if typ == "none" || typ == "list" {
			val := rs.rpush(args[0], args[1])
			return replyInteger(c, val)
		}
		return replyWrongTypeOperationError(c)
	case "LLEN":
		if argsLen != 1 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		typ := rs.getDBType(args[0])
		if typ == "none" {
			// 0 if the key doesnt exist
			return replyInteger(c, "0")
		}
		if typ != "list" {
			return replyWrongTypeOperationError(c)
		}
		val := rs.llen(args[0])
		return replyInteger(c, val)
	case "LRANGE":
		if argsLen != 3 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		typ := rs.getDBType(args[0])
		if typ == "string" || typ == "set" {
			return replyWrongTypeOperationError(c)
		}
		if typ == "none" {
			return replyEmptySetOrList(c)
		}
		start, err := strconv.Atoi(args[1])
		if err != nil {
			return replyInvalidTypeIntegerError(c)
		}
		end, err := strconv.Atoi(args[2])
		if err != nil {
			return replyInvalidTypeIntegerError(c)
		}
		val := rs.lrange(args[0], start, end)
		if len(val) == 0 {
			return replyEmptySetOrList(c)
		}
		return replyMultiBulkString(c, val)
	case "LINDEX":
		if argsLen != 2 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		typ := rs.getDBType(args[0])
		if typ == "string" || typ == "set" {
			return replyWrongTypeOperationError(c)
		}
		if typ == "none" {
			return replyEmptyBulkString(c)
		}
		index, err := strconv.Atoi(args[1])
		if err != nil {
			return replyInvalidTypeIntegerError(c)
		}
		val := rs.lindex(args[0], index)
		if val == emptyBulkString {
			return replyEmptyBulkString(c)
		}
		return replyBulkString(c, val)
	case "LPOP":
		if argsLen != 1 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		typ := rs.getDBType(args[0])
		if typ == "string" || typ == "set" {
			return replyWrongTypeOperationError(c)
		}
		if typ == "none" {
			return replyEmptyBulkString(c)
		}
		return replyBulkString(c, rs.lpop(args[0]))
	case "RPOP":
		if argsLen != 1 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		typ := rs.getDBType(args[0])
		if typ == "string" || typ == "set" {
			return replyWrongTypeOperationError(c)
		}
		if typ == "none" {
			return replyEmptyBulkString(c)
		}
		return replyBulkString(c, rs.rpop(args[0]))
	case "LTRIM":
		if argsLen != 3 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		typ := rs.getDBType(args[0])
		if typ == "string" || typ == "set" {
			return replyWrongTypeOperationError(c)
		}
		if typ == "none" {
			return replyEmptySetOrList(c)
		}
		start, err := strconv.Atoi(args[1])
		if err != nil {
			return replyInvalidTypeIntegerError(c)
		}
		end, err := strconv.Atoi(args[2])
		if err != nil {
			return replyInvalidTypeIntegerError(c)
		}
		ok := rs.ltrim(args[0], start, end)
		if !ok {
			// delete the key because the indexes resulted in an empty list
			rs.del(args[0])
			return replyEmptySetOrList(c)
		}
		return replyOK(c)
	case "LSET":
		if argsLen != 3 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		typ := rs.getDBType(args[0])
		if typ == "string" || typ == "set" {
			return replyWrongTypeOperationError(c)
		}
		if typ == "none" {
			return replyNoSuchKey(c)
		}
		index, err := strconv.Atoi(args[1])
		if err != nil {
			return replyInvalidTypeIntegerError(c)
		}
		ok := rs.lset(args[0], index, args[2])
		if !ok {
			return replyInvalidTypeIntegerError(c)
		}
		return replyOK(c)
	case "LREM":
		if argsLen != 3 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		typ := rs.getDBType(args[0])
		if typ == "string" || typ == "set" {
			return replyInteger(c, "-2")
		}
		if typ == "none" {
			return replyInteger(c, "-1")
		}
		count, err := strconv.Atoi(args[1])
		if err != nil {
			return replyInvalidTypeIntegerError(c)
		}
		return replyInteger(c, rs.lrem(args[0], count, args[2]))
	// Commands Operating on Sets
	case "SADD":
		if argsLen != 2 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		val := rs.sadd(args[0], args[1])
		return replyInteger(c, val)
	case "SREM":
		if argsLen != 2 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		return replyInteger(c, rs.srem(args[0], args[1]))
	case "SCARD":
		if argsLen != 1 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		return replyInteger(c, rs.scard(args[0]))
	case "SISMEMBER":
		if argsLen != 2 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		return replyInteger(c, rs.sismember(args[0], args[1]))
	case "SINTER":
		if argsLen == 0 {
			return replyInvalidNumberOfArgsError(c, command)
		}

		// Check if any have the wrong type
		for _, v := range args {
			if rs.getDBType(v) == "none" {
				return replyEmptySetOrList(c)
			}
			if rs.getDBType(v) != "set" {
				return replyWrongTypeOperationError(c)
			}
		}

		val := rs.sinter(args...)
		if len(val) == 0 {
			return replyEmptySetOrList(c)
		}
		return replyMultiBulkString(c, val)
	case "SINTERSTORE":
		if argsLen < 2 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		if rs.getDBType(args[0]) != "none" || rs.getDBType(args[0]) == "set" {
			return replyWrongTypeOperationError(c)
		}
		// Check if any have the wrong type
		for _, v := range args[1:] {
			if rs.getDBType(v) == "none" {
				return replyEmptySetOrList(c)
			}
			if rs.getDBType(v) != "set" {
				return replyWrongTypeOperationError(c)
			}
		}
		rs.sinterstore(args[0], args[1:]...)
		return replyOK(c)
	case "SMEMBERS":
		if argsLen != 1 {
			return replyInvalidNumberOfArgsError(c, command)
		}
		val, ok := rs.smembers(args[0])
		if !ok {
			return replyEmptySetOrList(c)
		}
		return replyMultiBulkString(c, val)
	// TODO: Commands Operating on Hashes
	// TODO: Commands Operating on Pub/Sub
	// TODO: Commands Operating on Streams
	default:
		return replyInvalidCommandError(c)
	}
}

func (rs *RedisServer) handleClient(connIndex int) {
	rs.lock.Lock()
	c := rs.conns[connIndex]
	rs.lock.Unlock()
	for {
		commandAndArgs, err := readCommand(c)
		if err != nil {
			// log.Printf("Failed to Read Command: %v\n", err)
			ok := replyInvalidCommandError(c)
			if !ok {
				return
			}
		}
		if len(commandAndArgs) == 0 {
			// continue if nothing came through
			continue
		}
		command := strings.ToUpper(commandAndArgs[0])
		ok := rs.ExecuteCommand(c, connIndex, command, commandAndArgs[1:])
		if !ok {
			// This should only be false from a shutdown command so return then
			return
		}
		atomic.AddUint64(&rs.commandsProcessed, 1)
	}
}

func main() {
	s := NewRedisServer(":8081")
	s.Listen()
	s.l.Close()
}
