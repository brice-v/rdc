package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
)

func readInteger(r *bufio.Reader) (string, error) {
	message, err := r.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("Read Integer: Failed to Read '\\n'. Error: %v", err)
	}
	ml := len(message)
	if message[ml-2] != '\r' {
		return "", fmt.Errorf("Wrong Format: Missing '\\r' at 2nd to last index")
	}
	num, err := strconv.Atoi(message[:ml-2])
	if err != nil {
		return "", fmt.Errorf("Read Integer: Got invalid integer. Error: %v", err)
	}
	return fmt.Sprintf("%d", num), nil
}

func readBulkString(r *bufio.Reader) (string, error) {
	message, err := r.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("Bulk String: Failed to Read '\\n'. Error: %v", err)
	}
	ml := len(message)
	if message[ml-2] != '\r' {
		return "", fmt.Errorf("Wrong Format: Missing '\\r' at 2nd to last index")
	}
	length, err := strconv.Atoi(message[:ml-2])
	if err != nil {
		return "", fmt.Errorf("Number of Elements in Array was not int. Error: %v", err)
	}
	// add 2 to length for the \r\n bytes
	buf := make([]byte, length+2)
	bytesRead, err := r.Read(buf)
	if err != nil || bytesRead != length+2 {
		return "", fmt.Errorf("Failed to Read Bulk String. Error: %v. Buffer Length: %d, Bytes Read: %d", err, length, bytesRead)
	}
	if buf[length] != '\r' {
		return "", fmt.Errorf("Wrong Format: Missing '\\r' at 2nd to last index")
	}
	if buf[length+1] != '\n' {
		return "", fmt.Errorf("Wrong Format: Missing '\\n' at last index")
	}
	return string(buf)[:length], nil
}

func readArray(r *bufio.Reader) ([]string, error) {
	message, err := r.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("Bulk Command Array: Failed to Read '\\n'. Error: %v", err)
	}
	ml := len(message)
	if message[ml-2] != '\r' {
		return nil, fmt.Errorf("Wrong Format: Missing '\\r' at 2nd to last index")
	}
	numElems, err := strconv.Atoi(message[:ml-2])
	if err != nil {
		return nil, fmt.Errorf("Number of Elements in Array was not int. Error: %v", err)
	}
	returnVals := make([]string, 0)
	for i := 0; i < numElems; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		switch b {
		case '*':
			readArray(r)
		case ':':
			message, err := readInteger(r)
			if err != nil {
				return nil, err
			}
			returnVals = append(returnVals, message)
		// case '+':
		// 	readSimpleString(r)
		case '$':
			message, err := readBulkString(r)
			if err != nil {
				return nil, err
			}
			returnVals = append(returnVals, message)
		default:
			return nil, fmt.Errorf("Did not get '*' or ':' or '+' or '$' as first byte of RESP response")
		}
	}
	return returnVals, nil
}

func readCommand(c net.Conn) ([]string, error) {
	// will listen for message to process ending in carriage return (\r)
	reader := bufio.NewReader(c)
	b, err := reader.ReadByte()
	if b != '*' {
		return nil, fmt.Errorf("First Byte was not '*' -- Currently Only Supporting Bulk Commands")
	}
	// because we expect bulk commands only readArray should pass
	elems, err := readArray(reader)
	if err != nil {
		return nil, err
	}
	return elems, nil
}
