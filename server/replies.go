package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

const invalidCommandError = "-ERR Invalid Command" + Delimeter

const integerOutOfRangeError = "-ERR value is not an integer or out of range" + Delimeter

const wrongTypeError = "-WRONGTYPE Operation against a key holding the wrong kind of value" + Delimeter

const okStatus = "+OK" + Delimeter

const emptySetOrList = "*-1" + Delimeter

const emptyBulkString = "$-1" + Delimeter

const noSuchKeyError = "-ERR no such key" + Delimeter

func isNil(err error) bool {
	return err == nil
}

func replyOK(c io.Writer) bool {
	_, err := c.Write([]byte(okStatus))
	return isNil(err)
}

func replySimpleString(c io.Writer, val string) bool {
	// If all else fails send them an error
	_, err := c.Write([]byte("+" + val + Delimeter))
	return isNil(err)
}

func replySimpleError(c io.Writer, val string) bool {
	// If all else fails send them an error
	_, err := c.Write([]byte("-" + val + Delimeter))
	return isNil(err)
}

func replyBulkString(c io.Writer, val string) bool {
	_, err := c.Write([]byte("$" + fmt.Sprintf("%d", len(val)) + Delimeter + val + Delimeter))
	return isNil(err)
}

func replyMultiBulkString(c io.Writer, val []string) bool {
	sb := strings.Builder{}
	_, err := sb.WriteString(fmt.Sprintf("*%d\r\n", len(val)))
	if !isNil(err) {
		return false
	}
	for _, v := range val {
		_, err := sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
		if !isNil(err) {
			return false
		}
	}
	_, err = c.Write([]byte(sb.String()))
	return isNil(err)
}

func replyInteger(c io.Writer, val string) bool {
	integer, err := strconv.Atoi(val)
	if !isNil(err) {
		return false
	}
	_, err = c.Write([]byte(":" + fmt.Sprintf("%d", integer) + Delimeter))
	return isNil(err)
}

func replyInvalidCommandError(c io.Writer) bool {
	// If all else fails send them an error
	_, err := c.Write([]byte(invalidCommandError))
	return isNil(err)
}

func replyInvalidNumberOfArgsError(c io.Writer, command string) bool {
	_, err := c.Write([]byte("-ERR Invalid Number of Args for '" + command + "'" + Delimeter))
	return isNil(err)
}

func replyInvalidTypeIntegerError(c io.Writer) bool {
	_, err := c.Write([]byte(integerOutOfRangeError))
	return isNil(err)
}

func replyInvalidGlobPatternError(c io.Writer, pattern string) bool {
	_, err := c.Write([]byte("-ERR Invalid Glob Pattern '" + pattern + "'"))
	return isNil(err)
}

func replyWrongTypeOperationError(c io.Writer) bool {
	_, err := c.Write([]byte(wrongTypeError))
	return isNil(err)
}

func replyEmptySetOrList(c io.Writer) bool {
	_, err := c.Write([]byte(emptySetOrList))
	return isNil(err)
}

func replyEmptyBulkString(c io.Writer) bool {
	_, err := c.Write([]byte(emptyBulkString))
	return isNil(err)
}

func replyNoSuchKey(c io.Writer) bool {
	_, err := c.Write([]byte(noSuchKeyError))
	return isNil(err)
}
