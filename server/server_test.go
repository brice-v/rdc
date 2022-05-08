package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
)

const PORT = ":8081"

func init() {
	s := NewRedisServer(PORT)
	os.Remove("save.db")
	go func() {
		s.Listen()
	}()
}

func TestConnectToServer(t *testing.T) {
	conn, err := net.Dial("tcp", PORT)
	if err != nil {
		t.Error("could not connect to server: ", err)
	}
	defer conn.Close()
}

func TestBulkCommands(t *testing.T) {
	tt := []struct {
		test    string
		payload []byte
		want    []byte
	}{
		{
			"PING with no args",
			[]byte("*1\r\n$4\r\nPING\r\n"),
			[]byte("+PONG\r\n"),
		},
		{
			"PING with 1 arg",
			[]byte("*2\r\n$4\r\nPING\r\n$12\r\nHello World!\r\n"),
			[]byte("+Hello World!\r\n"),
		},
		{
			"PING with too many args",
			[]byte("*3\r\n$4\r\nPING\r\n$12\r\nHello World!\r\n$5\r\nhello\r\n"),
			[]byte("-ERR Invalid Number of Args for 'PING'\r\n"),
		},
		{
			"SET with too many args",
			[]byte("*4\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$3\r\nfoo\r\n:2\r\n"),
			[]byte("-ERR Invalid Number of Args for 'SET'\r\n"),
		},
		{
			"SET with too few args",
			[]byte("*2\r\n$3\r\nSET\r\n$5\r\nmykey\r\n"),
			[]byte("-ERR Invalid Number of Args for 'SET'\r\n"),
		},
		{
			"GET with too many args",
			[]byte("*3\r\n$3\r\nGET\r\n$5\r\nmykey\r\n$3\r\nfoo\r\n"),
			[]byte("-ERR Invalid Number of Args for 'GET'\r\n"),
		},
		{
			"GET with too few args",
			[]byte("*1\r\n$3\r\nGET\r\n"),
			[]byte("-ERR Invalid Number of Args for 'GET'\r\n"),
		},
		{
			"SET a string (uppercase command)",
			[]byte("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$3\r\nfoo\r\n"),
			[]byte("+OK\r\n"),
		},
		{
			"SET a string (lowercase command)",
			[]byte("*3\r\n$3\r\nset\r\n$6\r\nmykey1\r\n$3\r\nbar\r\n"),
			[]byte("+OK\r\n"),
		},
		{
			"GET a string (uppercase command)",
			[]byte("*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n"),
			[]byte("$3\r\nfoo\r\n"),
		},
		{
			"GET a string (lowercase command)",
			[]byte("*2\r\n$3\r\nget\r\n$6\r\nmykey1\r\n"),
			[]byte("$3\r\nbar\r\n"),
		},
		{
			"SET an integer (uppercase command)",
			[]byte("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n:1\r\n"),
			[]byte("+OK\r\n"),
		},
		{
			"SET an integer (lowercase command)",
			[]byte("*3\r\n$3\r\nset\r\n$6\r\nmykey1\r\n:2\r\n"),
			[]byte("+OK\r\n"),
		},
		{
			"GET an integer (uppercase)",
			[]byte("*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n"),
			[]byte("$1\r\n1\r\n"),
		},
		{
			"GET an integer (lowercase)",
			[]byte("*2\r\n$3\r\nget\r\n$6\r\nmykey1\r\n"),
			[]byte("$1\r\n2\r\n"),
		},
		{
			"EXISTS on a key that exists",
			[]byte("*2\r\n$6\r\nEXISTS\r\n$6\r\nmykey1\r\n"),
			[]byte(":1\r\n"),
		},
		{
			"EXISTS on a key that does not exist",
			[]byte("*2\r\n$6\r\nEXISTS\r\n$9\r\nmykey1234\r\n"),
			[]byte(":0\r\n"),
		},
		{
			"EXISTS with too few args",
			[]byte("*1\r\n$6\r\nEXISTS\r\n"),
			[]byte("-ERR Invalid Number of Args for 'EXISTS'\r\n"),
		},
		{
			"SETNX on a key that exists",
			[]byte("*3\r\n$5\r\nsetnx\r\n$6\r\nmykey1\r\n$3\r\nfoo\r\n"),
			[]byte(":0\r\n"),
		},
		{
			"SETNX on a key that does not exist",
			[]byte("*3\r\n$5\r\nsetnx\r\n$6\r\nASDFGQ\r\n$3\r\nfoo\r\n"),
			[]byte(":1\r\n"),
		},
		{
			"SETNX on a key that exists",
			mbrr("setnx mykey1 foo"),
			// []byte("*3\r\n$5\r\nsetnx\r\n$6\r\nmykey1\r\n$3\r\nfoo\r\n"),
			[]byte(":0\r\n"),
		},
		{
			"SETNX with too many args",
			mbrr("setnx abc efg higkl"),
			[]byte("-ERR Invalid Number of Args for 'SETNX'\r\n"),
		},
		{
			"SETNX with too few args",
			mbrr("setnx abc"),
			[]byte("-ERR Invalid Number of Args for 'SETNX'\r\n"),
		},
		{
			"INCR a key that does not exist",
			mbrr("incr hello"),
			[]byte(":0\r\n"),
		},
		{
			"INCR a key that does exist",
			mbrr("incr hello"),
			[]byte(":1\r\n"),
		},
		{
			"set a value to be used in incr by a string key test",
			mbrr("set heywork something"),
			[]byte("+OK\r\n"),
		},
		{
			"INCR a key that is set to a string",
			mbrr("incr heywork"),
			[]byte("-ERR value is not an integer or out of range\r\n"),
		},
		{
			"INCR with too many args",
			mbrr("incr hello world"),
			[]byte("-ERR Invalid Number of Args for 'INCR'\r\n"),
		},
		{
			"INCR with too few args",
			mbrr("incr"),
			[]byte("-ERR Invalid Number of Args for 'INCR'\r\n"),
		},
		{
			"INCRBY a key that does not exist",
			mbrr("incrby asfdlkjsdf 3"),
			[]byte(":3\r\n"),
		},
		{
			"INCRBY a key that does exist",
			mbrr("incrby hello 3"),
			[]byte(":4\r\n"),
		},
		{
			"INCRBY a key that is set to a string",
			mbrr("incrby heywork 2"),
			[]byte("-ERR value is not an integer or out of range\r\n"),
		},
		{
			"INCRBY with too many args",
			mbrr("incrby hello 123 abc 213"),
			[]byte("-ERR Invalid Number of Args for 'INCRBY'\r\n"),
		},
		{
			"INCRBY with too few args",
			mbrr("incrby"),
			[]byte("-ERR Invalid Number of Args for 'INCRBY'\r\n"),
		},
		{
			"GET an integer from a string key",
			mbrr("get hello"),
			[]byte("$1\r\n4\r\n"),
		},
		{
			"DECR a key that does not exist",
			mbrr("decr hello12345"),
			[]byte(":-1\r\n"),
		},
		{
			"DECR a key that does exist",
			mbrr("decr hello"),
			[]byte(":3\r\n"),
		},
		{
			"set a value to be used in decr by a string key test",
			mbrr("set heywork1 something"),
			[]byte("+OK\r\n"),
		},
		{
			"DECR a key that is set to a string",
			mbrr("decr heywork1"),
			[]byte("-ERR value is not an integer or out of range\r\n"),
		},
		{
			"DECR with too many args",
			mbrr("decr hello world"),
			[]byte("-ERR Invalid Number of Args for 'DECR'\r\n"),
		},
		{
			"DECR with too few args",
			mbrr("decr"),
			[]byte("-ERR Invalid Number of Args for 'DECR'\r\n"),
		},
		{
			"DECRBY a key that does not exist",
			mbrr("decrby asfdlkjsdf1 3"),
			[]byte(":-3\r\n"),
		},
		{
			"DECRBY a key that does exist",
			mbrr("decrby hello 3"),
			[]byte(":0\r\n"),
		},
		{
			"DECRBY a key that is set to a string",
			mbrr("decrby heywork 2"),
			[]byte("-ERR value is not an integer or out of range\r\n"),
		},
		{
			"DECRBY with too many args",
			mbrr("decrby hello 123 abc 213"),
			[]byte("-ERR Invalid Number of Args for 'DECRBY'\r\n"),
		},
		{
			"DECRBY with too few args",
			mbrr("decrby"),
			[]byte("-ERR Invalid Number of Args for 'DECRBY'\r\n"),
		},
		{
			"GET an integer from a string key",
			mbrr("get hello"),
			[]byte("$1\r\n0\r\n"),
		},
		{
			"DEL deletes a key that exists",
			mbrr("del hello"),
			[]byte(":1\r\n"),
		},
		{
			"DEL deletes a key that does not exist",
			mbrr("del fsakjkfsajkjfsa"),
			[]byte(":0\r\n"),
		},
		{
			"DEL with too many args",
			mbrr("del saf fsakj"),
			[]byte("-ERR Invalid Number of Args for 'DEL'\r\n"),
		},
		{
			"DEL with too few args",
			mbrr("del"),
			[]byte("-ERR Invalid Number of Args for 'DEL'\r\n"),
		},
		{
			"TYPE of key that exists",
			mbrr("type heywork"),
			[]byte("string\r\n"),
		},
		{
			"TYPE of key that does not exists",
			mbrr("type fsawrqsafrwq"),
			[]byte("none\r\n"),
		},
		{
			"TYPE with too many args",
			mbrr("type"),
			[]byte("-ERR Invalid Number of Args for 'TYPE'\r\n"),
		},
		{
			"TYPE with too few args",
			mbrr("type safkj fsakjfsa asf"),
			[]byte("-ERR Invalid Number of Args for 'TYPE'\r\n"),
		},
		{
			"DEL key that exists, so that we can check that TYPE will return none on it",
			mbrr("del heywork"),
			[]byte(":1\r\n"),
		},
		{
			"TYPE on the key that we just deleted (need to make sure it is deleted from tstore",
			mbrr("type heywork"),
			[]byte("none\r\n"),
		},
		{
			"SET another variable to use to test wrongtype check",
			mbrr("set STRINGTYPE x"),
			[]byte(okStatus),
		},
		{
			"LPUSH gets wrongtype error on string key",
			mbrr("lpush STRINGTYPE abc"),
			[]byte(wrongTypeError),
		},
		{
			"RPUSH gets wrongtype error on string key",
			mbrr("rpush STRINGTYPE abc"),
			[]byte(wrongTypeError),
		},
		{
			"LPUSH a string value",
			mbrr("lpush list1 val"),
			[]byte(":1\r\n"),
		},
		{
			"LPUSH with too many args",
			mbrr("lpush list1 val fsa fsa"),
			[]byte("-ERR Invalid Number of Args for 'LPUSH'\r\n"),
		},
		{
			"LPUSH with too few args",
			mbrr("lpush list1"),
			[]byte("-ERR Invalid Number of Args for 'LPUSH'\r\n"),
		},
		{
			"LPUSH with too few args",
			mbrr("lpush"),
			[]byte("-ERR Invalid Number of Args for 'LPUSH'\r\n"),
		},
		{
			"RPUSH a string value",
			mbrr("rpush list1 var"),
			[]byte(":2\r\n"),
		},
		{
			"RPUSH with too many args",
			mbrr("rpush list1 val fsa fsa"),
			[]byte("-ERR Invalid Number of Args for 'RPUSH'\r\n"),
		},
		{
			"RPUSH with too few args",
			mbrr("rpush list1"),
			[]byte("-ERR Invalid Number of Args for 'RPUSH'\r\n"),
		},
		{
			"RPUSH with too few args",
			mbrr("rpush"),
			[]byte("-ERR Invalid Number of Args for 'RPUSH'\r\n"),
		},
		{
			"LLEN on a list key that exists",
			mbrr("llen list1"),
			[]byte(":2\r\n"),
		},
		{
			"LLEN on a string key that exists",
			mbrr("llen STRINGTYPE"),
			// NOTE: the 1.0 docs say that it returns a -2 if the specified
			// key does not contain a list type
			// I use a wrong type error because that is what redis shows now
			[]byte(wrongTypeError),
		},
		{
			"LLEN on a key that does not exist",
			mbrr("llen fsaojfsa3958"),
			[]byte(":0\r\n"),
		},
		{
			"LLEN with too many args",
			mbrr("llen fsa fsaljfsa"),
			[]byte("-ERR Invalid Number of Args for 'LLEN'\r\n"),
		},
		{
			"LLEN with too few args",
			mbrr("llen"),
			[]byte("-ERR Invalid Number of Args for 'LLEN'\r\n"),
		},
		{
			"INCR on list type to get wrongtype error",
			mbrr("incr list1"),
			[]byte(wrongTypeError),
		},
		{
			"INCRBY on list type to get wrongtype error",
			mbrr("incrby list1 2"),
			[]byte(wrongTypeError),
		},
		{
			"DECR on list type to get wrongtype error",
			mbrr("decr list1"),
			[]byte(wrongTypeError),
		},
		{
			"DECRBY on list type to get wrongtype error",
			mbrr("decrby list1 2"),
			[]byte(wrongTypeError),
		},
		{
			"LRANGE on a key that does not exist",
			mbrr("lrange fsajfsa08qwt 0 0"),
			[]byte(emptySetOrList),
		},
		{
			"LRANGE with too many args",
			mbrr("lrange list1 fsa fsa fsa sfa"),
			[]byte("-ERR Invalid Number of Args for 'LRANGE'\r\n"),
		},
		{
			"LRANGE with too few args",
			mbrr("lrange list1 fsa"),
			[]byte("-ERR Invalid Number of Args for 'LRANGE'\r\n"),
		},
		{
			"LRANGE of simple list with simple indexes",
			mbrr("lrange list1 0 1"),
			[]byte("*2\r\n$3\r\nval\r\n$3\r\nvar\r\n"),
		},
		{
			"LRANGE of simple list with 0 as start and -1 as end index (this should retreive all items)",
			mbrr("lrange list1 0 -1"),
			[]byte("*2\r\n$3\r\nval\r\n$3\r\nvar\r\n"),
		},
		{
			"LRANGE of simple list with 0 as start and -2 as end index",
			mbrr("lrange list1 0 -2"),
			[]byte("*1\r\n$3\r\nval\r\n"),
		},
		{
			"LRANGE of simple list with 1 as start and -1 as end index",
			mbrr("lrange list1 1 -1"),
			[]byte("*1\r\n$3\r\nvar\r\n"),
		},
		{
			"LRANGE of simple list with -1 as start and -1 as end index",
			mbrr("lrange list1 -1 -1"),
			[]byte("*1\r\n$3\r\nvar\r\n"),
		},
		{
			"LRANGE on a key that does exist but with start and end indices that dont match any elems",
			mbrr("lrange list1 -5 -10"),
			[]byte(emptySetOrList),
		},
		{
			"LINDEX with too many args",
			mbrr("lindex x 24 24 24"),
			[]byte("-ERR Invalid Number of Args for 'LINDEX'\r\n"),
		},
		{
			"LINDEX with too few args",
			mbrr("lindex x"),
			[]byte("-ERR Invalid Number of Args for 'LINDEX'\r\n"),
		},
		{
			"LINDEX on the wrong type of key",
			mbrr("lindex STRINGTYPE 52"),
			[]byte(wrongTypeError),
		},
		{
			"LINDEX on a list key with 0 as index",
			mbrr("lindex list1 0"),
			[]byte("$3\r\nval\r\n"),
		},
		{
			"LINDEX on a list key with 1 as index",
			mbrr("lindex list1 1"),
			[]byte("$3\r\nvar\r\n"),
		},
		{
			"LINDEX on a list key with -1 as index",
			mbrr("lindex list1 -1"),
			[]byte("$3\r\nvar\r\n"),
		},
		{
			"LINDEX on a list key with -2 as index",
			mbrr("lindex list1 -2"),
			[]byte("$3\r\nval\r\n"),
		},
		{
			"LINDEX on a list key with invalid index",
			mbrr("lindex list1 -5"),
			[]byte(emptyBulkString),
		},
		{
			"LINDEX on a key that does not exist",
			mbrr("lindex keythatdoesntexist 1"),
			[]byte(emptyBulkString),
		},
		{
			"LPOP with too many args",
			mbrr("lpop sfa fsa fsa fsa"),
			[]byte("-ERR Invalid Number of Args for 'LPOP'\r\n"),
		},
		{
			"LPOP on a key that does not exist",
			mbrr("lpop keythatdoesnotexist"),
			[]byte(emptyBulkString),
		},
		{
			"LPOP on a string key",
			mbrr("lpop STRINGTYPE"),
			[]byte(wrongTypeError),
		},
		{
			"RPOP with too many args",
			mbrr("rpop sfa fsa fsa fsa"),
			[]byte("-ERR Invalid Number of Args for 'RPOP'\r\n"),
		},
		{
			"RPOP on a key that does not exist",
			mbrr("rpop keythatdoesnotexist"),
			[]byte(emptyBulkString),
		},
		{
			"RPOP on a string key",
			mbrr("rpop STRINGTYPE"),
			[]byte(wrongTypeError),
		},
		{
			"LPOP with too few args",
			mbrr("lpop"),
			[]byte("-ERR Invalid Number of Args for 'LPOP'\r\n"),
		},
		{
			"LPOP with too few args",
			mbrr("rpop"),
			[]byte("-ERR Invalid Number of Args for 'RPOP'\r\n"),
		},
		{
			"LPOP on a list key",
			mbrr("lpop list1"),
			[]byte("$3\r\nval\r\n"),
		},
		{
			"RPOP on a list key",
			mbrr("Rpop list1"),
			[]byte("$3\r\nvar\r\n"),
		},
		{
			"LLEN on list1 key should now be 0",
			mbrr("llen llist1"),
			[]byte(":0\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 zero"),
			[]byte(":1\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 one"),
			[]byte(":2\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 two"),
			[]byte(":3\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 three"),
			[]byte(":4\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 four"),
			[]byte(":5\r\n"),
		},
		{
			"LPOP on the list to see changes with lrange",
			mbrr("lpop list1"),
			[]byte("$4\r\nzero\r\n"),
		},
		{
			"RPOP on the list to see changes with lrange",
			mbrr("rpop list1"),
			[]byte("$4\r\nfour\r\n"),
		},
		{
			"LRANGE on the remaining elements",
			mbrr("lrange list1 0 -1"),
			[]byte("*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n"),
		},
		{
			"LRANGE on the same list with 0 as start and 3 as end",
			mbrr("lrange list1 0 3"),
			[]byte("*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n"),
		},
		{
			"LLEN on list1 returns 3 after the LPOP and RPOP",
			mbrr("llen list1"),
			[]byte(":3\r\n"),
		},
		// RPUSH some values onto the list to test LTRIM
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 bbbb"),
			[]byte(":4\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 aaaaa"),
			[]byte(":5\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 hhhhh"),
			[]byte(":6\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 1234asf"),
			[]byte(":7\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 lalala"),
			[]byte(":8\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 zzzz"),
			[]byte(":9\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 hey"),
			[]byte(":10\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 something"),
			[]byte(":11\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 hj1234"),
			[]byte(":12\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 jfkd"),
			[]byte(":13\r\n"),
		},
		// Now for LTRIM tests
		{
			"LTRIM with too many args",
			mbrr("ltrim fsa fsa afs fsa"),
			[]byte("-ERR Invalid Number of Args for 'LTRIM'\r\n"),
		},
		{
			"LTRIM with too few args",
			mbrr("ltrim"),
			[]byte("-ERR Invalid Number of Args for 'LTRIM'\r\n"),
		},
		{
			"LTRIM wrongtype error",
			mbrr("ltrim STRINGTYPE 0 1"),
			[]byte(wrongTypeError),
		},
		{
			"LTRIM with non int 1st index",
			mbrr("ltrim list1 asf 0"),
			[]byte(integerOutOfRangeError),
		},
		{
			"LTRIM with non int 2nd index",
			mbrr("ltrim list1 1 fsa"),
			[]byte(integerOutOfRangeError),
		},
		{
			"LTRIM on valid list with positive indices",
			mbrr("ltrim list1 0 13"),
			[]byte(okStatus),
		},
		{
			"LLEN on list1 should be 13 after not trimming any values",
			mbrr("llen list1"),
			[]byte(":13\r\n"),
		},
		{
			"LTRIM on valid list with negative second index",
			mbrr("ltrim list1 0 -1"),
			[]byte(okStatus),
		},
		{
			"LLEN on list1 should be 13 after not trimming any values",
			mbrr("llen list1"),
			[]byte(":13\r\n"),
		},
		{
			"LTRIM on valid list with large second index to keep all values",
			mbrr("ltrim list1 0 100"),
			[]byte(okStatus),
		},
		{
			"LLEN on list1 should be 13 after not trimming any values",
			mbrr("llen list1"),
			[]byte(":13\r\n"),
		},
		{
			"LTRIM on valid list and only keep first element",
			mbrr("ltrim list1 0 0"),
			[]byte(okStatus),
		},
		{
			"LLEN on list1 should be 1 after leaving first element",
			mbrr("llen list1"),
			[]byte(":1\r\n"),
		},
		{
			"LTRIM should delete all and the key if start index is greater than end index",
			mbrr("ltrim list1 100 0"),
			[]byte(emptySetOrList),
		},
		{
			"LLEN on list1 should be 0 as the key does not exist",
			mbrr("llen list1"),
			[]byte(":0\r\n"),
		},
		{
			"Validate that list1 key has been deleted",
			mbrr("exists list1"),
			[]byte(":0\r\n"),
		},
		// RPUSH some values onto the list to test LSET
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 aaaa"),
			[]byte(":1\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 bbb"),
			[]byte(":2\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 CCCC"),
			[]byte(":3\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 ddddd"),
			[]byte(":4\r\n"),
		},
		{
			"RPUSH a value on to the list for continued list testing",
			mbrr("rpush list1 lalala"),
			[]byte(":5\r\n"),
		},
		// Now testing LSET
		{
			"LSET with too many args",
			mbrr("lset x fa afs fsa"),
			[]byte("-ERR Invalid Number of Args for 'LSET'\r\n"),
		},
		{
			"LSET with too few args",
			mbrr("lset x fa"),
			[]byte("-ERR Invalid Number of Args for 'LSET'\r\n"),
		},
		{
			"LSET on key that does not exist",
			mbrr("lset fsaojfsafas 0 a"),
			[]byte(noSuchKeyError),
		},
		{
			"LSET on key of the wrong type",
			mbrr("lset STRINGTYPE 0 1"),
			[]byte(wrongTypeError),
		},
		{
			"LSET with non integer index value",
			mbrr("lset list1 abc 1"),
			[]byte(integerOutOfRangeError),
		},
		{
			"LSET a valid index",
			mbrr("lset list1 0 1"),
			[]byte(okStatus),
		},
		{
			"LRANGE on the same list to see the inserted value",
			mbrr("lrange list1 0 -1"),
			[]byte("*6\r\n$1\r\n1\r\n$4\r\naaaa\r\n$3\r\nbbb\r\n$4\r\nCCCC\r\n$5\r\nddddd\r\n$6\r\nlalala\r\n"),
		},
		{
			"LLEN on the same list returns 6",
			mbrr("llen list1"),
			[]byte(":6\r\n"),
		},
		{
			"LREM with too many args",
			mbrr("lrem list1 x x x"),
			[]byte("-ERR Invalid Number of Args for 'LREM'\r\n"),
		},
		{
			"LREM with too few args",
			mbrr("lrem list1"),
			[]byte("-ERR Invalid Number of Args for 'LREM'\r\n"),
		},
		{
			"LREM on a non list key type",
			mbrr("lrem STRINGTYPE 0 val"),
			[]byte(":-2\r\n"),
		},
		{
			"LREM on a key that does not exist",
			mbrr("lrem STRINGTYPEafsfsafsa 0 val"),
			[]byte(":-1\r\n"),
		},
		{
			"LREM with a count that is not an integer",
			mbrr("lrem list1 val val"),
			[]byte(integerOutOfRangeError),
		},
		{
			"LREM on list1 that doesnt match anything",
			mbrr("lrem list1 0 afsojfsa"),
			[]byte(":0\r\n"),
		},
		{
			"LREM a val from list1 using a negative count",
			mbrr("lrem list1 -1 lalala"),
			[]byte(":1\r\n"),
		},
		{
			"LREM a val from list1 using a positive count",
			mbrr("lrem list1 1 1"),
			[]byte(":1\r\n"),
		},
		{
			"LREM a val from list1 using 0 as count",
			mbrr("lrem list1 0 bbb"),
			[]byte(":1\r\n"),
		},
		{
			"LRANGE to see that the correct elements are left",
			mbrr("lrange list1 0 -1"),
			[]byte("*3\r\n$4\r\naaaa\r\n$4\r\nCCCC\r\n$5\r\nddddd\r\n"),
		},
		{
			"KEYS with too many args",
			mbrr("KEYS sfa fsa fs fsa fsa"),
			[]byte("-ERR Invalid Number of Args for 'KEYS'\r\n"),
		},
		{
			"KEYS with too few args",
			mbrr("Keys"),
			[]byte("-ERR Invalid Number of Args for 'KEYS'\r\n"),
		},
		{
			"KEYS with * glob to see all keys",
			mbrr("keys *"),
			[]byte("*9\r\n$6\r\nASDFGQ\r\n$10\r\nSTRINGTYPE\r\n$10\r\nasfdlkjsdf\r\n$11\r\nasfdlkjsdf1\r\n$10\r\nhello12345\r\n$8\r\nheywork1\r\n$5\r\nlist1\r\n$5\r\nmykey\r\n$6\r\nmykey1\r\n"),
		},
		{
			"KEYS with pattern that matches nothing",
			mbrr("keys somepatternthatwillmatchnothing"),
			[]byte(emptySetOrList),
		},
		{
			"DBSIZE with too many args",
			mbrr("dbsize 14114"),
			[]byte("-ERR Invalid Number of Args for 'DBSIZE'\r\n"),
		},
		{
			"DBSIZE command test",
			mbrr("dbsize"),
			[]byte(":9\r\n"),
		},
		{
			"RANDOMKEY with too many args",
			mbrr("randomkey 141 r 1"),
			[]byte("-ERR Invalid Number of Args for 'RANDOMKEY'\r\n"),
		},
		{
			"RENAME with too many args",
			mbrr("rename 141 r 1"),
			[]byte("-ERR Invalid Number of Args for 'RENAME'\r\n"),
		},
		{
			"RENAMENX with too many args",
			mbrr("renamenx 141 r 1"),
			[]byte("-ERR Invalid Number of Args for 'RENAMENX'\r\n"),
		},
		{
			"RENAME with too few args",
			mbrr("rename 1"),
			[]byte("-ERR Invalid Number of Args for 'RENAME'\r\n"),
		},
		{
			"RENAMENX with too few args",
			mbrr("renamenx 1"),
			[]byte("-ERR Invalid Number of Args for 'RENAMENX'\r\n"),
		},
		{
			"SELECT with too many args",
			mbrr("select 141 r 1"),
			[]byte("-ERR Invalid Number of Args for 'SELECT'\r\n"),
		},
		{
			"SELECT with too few args",
			mbrr("select"),
			[]byte("-ERR Invalid Number of Args for 'SELECT'\r\n"),
		},
		{
			"SELECT with index that is not an integer",
			mbrr("select abc"),
			[]byte(integerOutOfRangeError),
		},
		{
			"SELECT with index that is greater than 9",
			mbrr("select 10"),
			[]byte(integerOutOfRangeError),
		},
		{
			"SELECT with index that is less than 0",
			mbrr("select -1"),
			[]byte(integerOutOfRangeError),
		},
		{
			"SELECT DB 1",
			mbrr("select 1"),
			[]byte(okStatus),
		},
		{
			"Check that no keys exist in DB 1",
			mbrr("keys *"),
			[]byte(emptySetOrList),
		},
		{
			"SELECT DB 0 again",
			mbrr("select 0"),
			[]byte(okStatus),
		},
		{
			"RENAME list1 to list2",
			mbrr("rename list1 list2"),
			[]byte(okStatus),
		},
		{
			"LRANGE on list2 to see that it is still there",
			mbrr("lrange list2 0 -1"),
			[]byte("*3\r\n$4\r\naaaa\r\n$4\r\nCCCC\r\n$5\r\nddddd\r\n"),
		},
		{
			"KEYS with `list1` as pattern to see if it exists. It shouldn't",
			mbrr("keys list1"),
			[]byte(emptySetOrList),
		},
		{
			"LLEN on list1 should return 0",
			mbrr("llen list1"),
			[]byte(":0\r\n"),
		},
		{
			"LLEN on list2 should be 3",
			mbrr("llen list2"),
			[]byte(":3\r\n"),
		},
		{
			"RENAMENX with key list2 to check successful response",
			mbrr("renamenx list2 listabc"),
			[]byte(":1\r\n"),
		},
		{
			"LRANGE on listabc to see that it is still there",
			mbrr("lrange listabc 0 -1"),
			[]byte("*3\r\n$4\r\naaaa\r\n$4\r\nCCCC\r\n$5\r\nddddd\r\n"),
		},
		{
			"KEYS with `list2` as pattern to see if it exists. It shouldn't",
			mbrr("keys list2"),
			[]byte(emptySetOrList),
		},
		{
			"LLEN on list2 should return 0",
			mbrr("llen list2"),
			[]byte(":0\r\n"),
		},
		{
			"LLEN on listabc should be 3",
			mbrr("llen listabc"),
			[]byte(":3\r\n"),
		},
		{
			"LPUSH to a new list to create it for more RENAMENX tests",
			mbrr("lpush LISTTYPE 1"),
			[]byte(":1\r\n"),
		},
		{
			"TYPE on LISTTYPE should return 'list'",
			mbrr("type LISTTYPE"),
			[]byte("list\r\n"),
		},
		{
			"TYPE on list2 should return 'none'",
			mbrr("type list2"),
			[]byte("none\r\n"),
		},
		{
			"RENAMENX but the target key already exists",
			mbrr("renamenx listabc LISTTYPE"),
			[]byte(":0\r\n"),
		},
		{
			"RENAMENX but the source key does not exist",
			mbrr("renamenx doesnotexistkey123 something_new"),
			[]byte(":-1\r\n"),
		},
		{
			"RENAMENX but the source key and target key are the same",
			mbrr("renamenx LISTTYPE LISTTYPE"),
			[]byte(":-3\r\n"),
		},
		{
			"SADD with too many args",
			mbrr("sadd 0 1 1 1 1 34"),
			mial("sadd"),
		},
		{
			"SADD with too few args",
			mbrr("sadd 0"),
			mial("sadd"),
		},
		{
			"SREM with too many args",
			mbrr("SREM 0 1 1 1 1 34"),
			mial("SREM"),
		},
		{
			"SREM with too few args",
			mbrr("SREM 0"),
			mial("SREM"),
		},
		{
			"SCARD with too many args",
			mbrr("SCARD 0 1 1 1 1 34"),
			mial("SCARD"),
		},
		{
			"SCARD with too few args",
			mbrr("SCARD"),
			mial("SCARD"),
		},
		{
			"SISMEMBER with too many args",
			mbrr("SISMEMBER 0 1 1 1 1 34"),
			mial("SISMEMBER"),
		},
		{
			"SISMEMBER with too few args",
			mbrr("SISMEMBER 0"),
			mial("SISMEMBER"),
		},
		{
			"SMEMBERS with too many args",
			mbrr("SMEMBERS 0 1 1 1 1 34"),
			mial("SMEMBERS"),
		},
		{
			"SMEMBERS with too few args",
			mbrr("SMEMBERS"),
			mial("SMEMBERS"),
		},
		{
			"SINTER with too few args",
			mbrr("sinter"),
			mial("sinter"),
		},
		{
			"SINTERSTORE with too few args",
			mbrr("sinterstore"),
			mial("sinterstore"),
		},
		{
			"SADD with a key and member value",
			mbrr("sadd set1 value"),
			[]byte(":1\r\n"),
		},
		{
			"SADD with a key and same member value",
			mbrr("sadd set1 value"),
			[]byte(":0\r\n"),
		},
		{
			"SADD with a string key",
			mbrr("sadd STRINGTYPE value"),
			[]byte(":-2\r\n"),
		},
		{
			"SADD with set key and another value",
			mbrr("sadd set1 another"),
			[]byte(":1\r\n"),
		},
		{
			"SMEMBERS with non set key",
			mbrr("smembers STRINGTYPE"),
			[]byte(emptySetOrList),
		},
		{
			"SMEMBERS on set key",
			mbrr("smembers set1"),
			[]byte("*2\r\n$7\r\nanother\r\n$5\r\nvalue\r\n"),
		},
		{
			"SREM on non set type",
			mbrr("srem STRINGTYPE val"),
			[]byte(":-2\r\n"),
		},
		{
			"SREM on set type with member that does not exist",
			mbrr("srem set1 val"),
			[]byte(":0\r\n"),
		},
		{
			"SREM on set type with member that does exist",
			mbrr("srem set1 value"),
			[]byte(":1\r\n"),
		},
		{
			"SMEMBERS on set after member removed",
			mbrr("smembers set1"),
			[]byte("*1\r\n$7\r\nanother\r\n"),
		},
		{
			"SCARD on set1",
			mbrr("scard set1"),
			[]byte(":1\r\n"),
		},
		// Add value back to set to test scard
		{
			"SADD on set1 with value",
			mbrr("sadd set1 value"),
			[]byte(":1\r\n"),
		},
		{
			"SCARD on non set key",
			mbrr("scard STRINGTYPE"),
			[]byte(":-2\r\n"),
		},
		{
			"SCARD on set key",
			mbrr("scard set1"),
			[]byte(":2\r\n"),
		},
		{
			"SISMEMBER on string key",
			mbrr("sismember STRINGTYPE member"),
			[]byte(":-2\r\n"),
		},
		{
			"SISMEMBER on a set key with valid member",
			mbrr("sismember set1 value"),
			[]byte(":1\r\n"),
		},
		{
			"SISMEMBER on a set key with invalid member",
			mbrr("sismember set1 value1234"),
			[]byte(":0\r\n"),
		},
		{
			"SINTER on single key that does not exist",
			mbrr("sinter fsaljfasljfsalj"),
			[]byte(emptySetOrList),
		},
		// Set up some values for SINTER and SINTERSTORE commands
		{
			"SADD set-1 1",
			mbrr("sadd set-1 1"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-1 2",
			mbrr("sadd set-1 2"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-1 3",
			mbrr("sadd set-1 3"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-1 4",
			mbrr("sadd set-1 4"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-1 5",
			mbrr("sadd set-1 5"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-1 6",
			mbrr("sadd set-1 6"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-1 7",
			mbrr("sadd set-1 7"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-1 8",
			mbrr("sadd set-1 8"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-1 9",
			mbrr("sadd set-1 9"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-2 0",
			mbrr("sadd set-2 0"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-2 2",
			mbrr("sadd set-2 2"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-2 4",
			mbrr("sadd set-2 4"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-2 6",
			mbrr("sadd set-2 6"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-2 8",
			mbrr("sadd set-2 8"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-3 a",
			mbrr("sadd set-3 a"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-3 b",
			mbrr("sadd set-3 b"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-3 c",
			mbrr("sadd set-3 c"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-3 d",
			mbrr("sadd set-3 d"),
			[]byte(":1\r\n"),
		},
		{
			"SADD set-3 e",
			mbrr("sadd set-3 e"),
			[]byte(":1\r\n"),
		},
		// Tests for SINTER and SINTERSTORE
		{
			"SINTER with set-1 and set-2, Should get even #'s",
			mbrr("sinter set-1 set-2"),
			mbrr("2 4 6 8"),
		},
		{
			"SINTER that results in empty list",
			mbrr("sinter set-1 set-2 set-3"),
			[]byte(emptySetOrList),
		},
		{
			"SINTER on a single set (should be equivalent of SMEMBERS)",
			mbrr("sinter set-1"),
			mbrr("1 2 3 4 5 6 7 8 9"),
		},
		{
			"SINTER on a single set (should be equivalent of SMEMBERS)",
			mbrr("sinter set-2"),
			mbrr("0 2 4 6 8"),
		},
		{
			"SINTER on a single set (should be equivalent of SMEMBERS)",
			mbrr("sinter set-3"),
			mbrr("a b c d e"),
		},
		{
			"SINTERSTORE on a new key with valid set keys",
			mbrr("sinterstore set123 set-1 set-2"),
			[]byte(okStatus),
		},
		{
			"SMEMBERS on newly created set to see all the members",
			mbrr("smembers set123"),
			mbrr("2 4 6 8"),
		},
		{
			"TYPE on newly created set to see 'set'",
			mbrr("type set123"),
			[]byte("set\r\n"),
		},
		{
			"SINTERSTORE with stringtype as the dest key",
			mbrr("sinterstore STRINGTYPE 2134"),
			[]byte(wrongTypeError),
		},
		{
			"MOVE command with too many args",
			mbrr("move fa fsa fsa  fsa"),
			mial("move"),
		},
		{
			"MOVE command with too few args",
			mbrr("move fa"),
			mial("move"),
		},
		{
			"MOVE with string index",
			mbrr("move key fsa"),
			[]byte(integerOutOfRangeError),
		},
		{
			"MOVE with integer index but out of range (pos)",
			mbrr("move key 11"),
			[]byte(":-4\r\n"),
		},
		{
			"MOVE with integer index but out of range (neg)",
			mbrr("move key -1"),
			[]byte(":-4\r\n"),
		},
		{
			"MOVE with valid integer index but invalid key",
			mbrr("move safsagagsagsags 1"),
			[]byte(":0\r\n"),
		},
		{
			"MOVE but the index is the same as the source db we are currently on 0",
			mbrr("move somekey 0"),
			[]byte(":-3\r\n"),
		},
		{
			"MOVE with valid key and valid db",
			mbrr("move set123 1"),
			[]byte(":1\r\n"),
		},
		{
			"SELECT DB 1 so we can check that the key exists",
			mbrr("select 1"),
			[]byte(okStatus),
		},
		{
			"KEYS * command on db so we can see that the key exists",
			mbrr("keys *"),
			mbrr("set123"),
		},
		{
			"SMEMBERS on the set to validate its contents",
			mbrr("smembers set123"),
			mbrr("2 4 6 8"),
		},
		{
			"SELECT DB 0 so we can check that the key is no longer in the 1st db",
			mbrr("select 0"),
			[]byte(okStatus),
		},
		{
			"keys set123 to see that it is no longer there",
			mbrr("keys set123"),
			[]byte(emptySetOrList),
		},
		{
			"SMEMBERS on the set to validate it returns empty string",
			mbrr("smembers set123"),
			[]byte(emptySetOrList),
		},
		{
			"SADD to make new set that we just put in the other db",
			mbrr("sadd set123 1"),
			[]byte(":1\r\n"),
		},
		{
			"MOVE on the key we just put into the other db with the valid key here to see a 0 response",
			mbrr("move set123 1"),
			[]byte(":0\r\n"),
		},
		{
			"LASTSAVE before saving should be 0 because the DB has not yet been saved",
			mbrr("lastsave"),
			[]byte(":0\r\n"),
		},
		{
			"SAVE command",
			mbrr("save"),
			[]byte(okStatus),
		},
		{
			"FLUSHDB command (never fails)",
			mbrr("flushdb"),
			[]byte(okStatus),
		},
		{
			"KEYS * command to make sure that nothing is here",
			mbrr("keys *"),
			[]byte(emptySetOrList),
		},
		{
			"SELECT DB 1 to see that the key there still exists",
			mbrr("select 1"),
			[]byte(okStatus),
		},
		{
			"KEYS * command to make sure set123 is still there",
			mbrr("keys *"),
			mbrr("set123"),
		},
		{
			"SELECT DB 0 so we can test flushall",
			mbrr("select 0"),
			[]byte(okStatus),
		},
		{
			"FLUSHALL",
			mbrr("flushall"),
			[]byte(okStatus),
		},
		{
			"KEYS * to verify there are still no keys",
			mbrr("keys *"),
			[]byte(emptySetOrList),
		},
		{
			"SELECT DB 1 to verify keys are gone",
			mbrr("select 1"),
			[]byte(okStatus),
		},
		{
			"KEYS * to verify there are still no keys",
			mbrr("keys *"),
			[]byte(emptySetOrList),
		},
		{
			"SAVE with too many args",
			mbrr("save afs afs "),
			mial("save"),
		},
		{
			"QUIT with too many args",
			mbrr("quit fsa fsa fsa"),
			mial("quit"),
		},
		// {
		// 	"LASTSAVE should return an integer",
		// 	mbrr("lastsave"),
		// 	[]byte(":123456789\r\n"),
		// },
	}

	for _, tc := range tt {
		t.Run(tc.test, func(t *testing.T) {
			conn, err := net.Dial("tcp", PORT)
			if err != nil {
				t.Error("connection error: ", err)
			}
			defer conn.Close()

			if _, err := conn.Write(tc.payload); err != nil {
				t.Error("write error:", err)
			}

			buf := make([]byte, len(tc.want))
			if out, err := bufio.NewReader(conn).Read(buf); err == nil {
				if bytes.Compare(buf, tc.want) != 0 {
					t.Errorf("actual did not match expected.\nActual:   %q\nExpected: %q", string(buf), tc.want)
				}
				if out != len(tc.want) {
					t.Errorf("num bytes read does not match num bytes wanted.\nActual: %d\nExpected: %d", out, len(tc.want))
				}
			} else {
				t.Error("read error: ", err)
			}
		})
	}
}

func BenchmarkExecuteCommand(b *testing.B) {
	s := NewRedisServer(":15615")
	defer s.l.Close()
	f, err := os.Open(os.DevNull)
	check(err)
	defer f.Close()

	for i := 0; i < b.N; i++ {
		s.ExecuteCommand(f, 0, "SADD", []string{"mykey1234", fmt.Sprintf("%d", i)})
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

// mial - make invalid args length error response
// takes the command string as input
func mial(c string) []byte {
	upc := strings.ToUpper(c)
	return []byte("-ERR Invalid Number of Args for '" + upc + "'" + Delimeter)
}
