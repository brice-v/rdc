package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"sc/list"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gobwas/glob"
	"github.com/google/uuid"

	_ "github.com/mattn/go-sqlite3"
)

type dbTyp string

const (
	// tList is the list database type
	tList dbTyp = "list"
	// tSet is the set database type
	tSet dbTyp = "set"
	// tString is the string database type
	tString dbTyp = "string"
	// tNone is the none database type
	tNone dbTyp = "none"
)

// DB is the core object of datatypes available on the server via commands
type DB struct {
	// kv is our key value store
	kv map[string]string
	// s is our set store
	s map[string]map[string]struct{}
	// ll is our doubly linked list for our list store
	ll map[string]*list.List

	// tstore contains the database type for each of the keys in the database
	tstore map[string]dbTyp
}

// NewDB returns a db object with all fields initialized
func NewDB() *DB {
	return &DB{
		kv:     make(map[string]string),
		s:      make(map[string]map[string]struct{}),
		ll:     make(map[string]*list.List),
		tstore: make(map[string]dbTyp),
	}
}

// DB Commands for String based commands
func (rs *RedisServer) set(key, value string) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.store[rs.sp].kv[key] = value
	// set our type so we know what type its associated with
	rs.store[rs.sp].tstore[key] = tString
}

func (rs *RedisServer) get(key string) (string, bool) {
	// rs.lock.Lock()
	// defer rs.lock.Unlock()
	// log.Printf("key = %q, val = %q", key, rs.store[rs.sp][key])
	val, ok := rs.store[rs.sp].kv[key]
	if !ok {
		return "-1", false
	}
	return val, true
}

// del supports deleting any key no matter the type and
// will return the proper response depending on whether it exists
func (rs *RedisServer) del(key string) bool {
	delete(rs.store[rs.sp].tstore, key)
	_, okkv := rs.get(key)
	if okkv {
		delete(rs.store[rs.sp].kv, key)
		return okkv
	}
	_, oks := rs.store[rs.sp].s[key]
	if oks {
		delete(rs.store[rs.sp].s, key)
		return oks
	}
	_, okll := rs.store[rs.sp].ll[key]
	if okll {
		delete(rs.store[rs.sp].ll, key)
		return okll
	}
	return false
}

func (rs *RedisServer) getDBType(key string) dbTyp {
	val, exists := rs.store[rs.sp].tstore[key]
	if exists {
		return val
	}
	return tNone
}

// Methods for operating on list portion of db

func (rs *RedisServer) lpush(key, value string) string {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	// set our type so we know what type its associated with
	rs.store[rs.sp].tstore[key] = tList

	_, ok := rs.store[rs.sp].ll[key]
	if !ok {
		rs.store[rs.sp].ll[key] = list.New().Init()
	}

	rs.store[rs.sp].ll[key].PushFront(value)
	size := rs.store[rs.sp].ll[key].Len()
	return strconv.Itoa(size)
}

func (rs *RedisServer) rpush(key, value string) string {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	// set our type so we know what type its associated with
	rs.store[rs.sp].tstore[key] = tList

	_, ok := rs.store[rs.sp].ll[key]
	if !ok {
		rs.store[rs.sp].ll[key] = list.New().Init()
	}

	rs.store[rs.sp].ll[key].PushBack(value)
	size := rs.store[rs.sp].ll[key].Len()
	return strconv.Itoa(size)
}

func (rs *RedisServer) llen(key string) string {
	return strconv.Itoa(rs.store[rs.sp].ll[key].Len())
}

func (rs *RedisServer) lrange(key string, start, end int) []string {
	// We know the key exists and start and end are valid ints
	l := make([]string, 0)
	i := 0
	size := rs.store[rs.sp].ll[key].Len()

	_start := 0
	if start < 0 {
		_start = start * -1
	} else {
		_start = start
	}

	_end := 0
	if end < 0 {
		// if its negative we want to add it to the size to get the proper
		// offset.  Otherwise just use it as the normal end
		_end = size + end
	} else {
		_end = end
	}

	for e := rs.store[rs.sp].ll[key].Front(); e != nil; e = e.Next() {
		if i >= _start && i <= _end {
			l = append(l, e.Value)
		}
		i++
	}
	return l
}

func (rs *RedisServer) lindex(key string, index int) string {
	size := rs.store[rs.sp].ll[key].Len()
	end := 0
	if index < 0 {
		end = size + index
	} else {
		end = index
	}

	i := 0
	for e := rs.store[rs.sp].ll[key].Front(); e != nil; e = e.Next() {
		if i == end {
			return e.Value
		}
		i++
	}
	return emptyBulkString
}

func (rs *RedisServer) ltrim(key string, start, end int) bool {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	i := 0
	size := rs.store[rs.sp].ll[key].Len()

	_start := 0
	if start < 0 {
		_start = start * -1
	} else {
		_start = start
	}

	_end := 0
	if end < 0 {
		// if its negative we want to add it to the size to get the proper
		// offset.  Otherwise just use it as the normal end
		_end = size + end
	} else {
		_end = end
	}

	// if start is larger than the end of the list, or start > end,
	// the result will be an empty list (which causes key to be removed).
	if _start > _end {
		// return false so the key will be deleted (gc should do the rest)
		return false
	}

	for e := rs.store[rs.sp].ll[key].Front(); e != nil; e = e.Next() {
		if i >= _start && i <= _end {
			i++
			continue
		}
		// save prev otherwise the next pointer will be nil (after removing)
		// prev will be the one we were just before so we wont skip over
		// any elements (using Next() will)
		next := e.Prev()
		rs.store[rs.sp].ll[key].Remove(e)
		e = next
		i++
	}
	return true
}

func (rs *RedisServer) lpop(key string) string {
	return rs.store[rs.sp].ll[key].Remove(rs.store[rs.sp].ll[key].Front())
}

func (rs *RedisServer) rpop(key string) string {
	return rs.store[rs.sp].ll[key].Remove(rs.store[rs.sp].ll[key].Back())
}

func (rs *RedisServer) lset(key string, index int, val string) bool {
	i := 0
	if i > rs.store[rs.sp].ll[key].Len() {
		return false
	}

	for e := rs.store[rs.sp].ll[key].Front(); e != nil; e = e.Next() {
		if i == index {
			rs.store[rs.sp].ll[key].InsertBefore(val, e)
			return true
		}
		i++
	}
	return false
}

func (rs *RedisServer) lrem(key string, count int, val string) string {
	// if count is negative we want to delete elements in reverse
	elemsDeleted := 0
	totToDelete := 0
	if count < 0 {
		totToDelete = count * -1
		for e := rs.store[rs.sp].ll[key].Back(); e != nil; e = e.Prev() {
			if e.Value == val {
				next := e.Next()
				rs.store[rs.sp].ll[key].Remove(e)
				e = next
				elemsDeleted++
				if elemsDeleted == totToDelete {
					break
				}
			}
		}
	} else if count > 0 {
		totToDelete = count
		for e := rs.store[rs.sp].ll[key].Front(); e != nil; e = e.Next() {
			if e.Value == val {
				next := e.Prev()
				rs.store[rs.sp].ll[key].Remove(e)
				e = next
				elemsDeleted++
				if elemsDeleted == totToDelete {
					break
				}
			}
		}
	} else {
		for e := rs.store[rs.sp].ll[key].Front(); e != nil; e = e.Next() {
			if e.Value == val {
				next := e.Prev()
				rs.store[rs.sp].ll[key].Remove(e)
				e = next
				elemsDeleted++
			}
		}
	}
	return strconv.Itoa(elemsDeleted)
}

func (rs *RedisServer) keys(pattern string) ([]string, bool) {
	var g glob.Glob
	g, err := glob.Compile(pattern)
	if err != nil {
		return nil, false
	}

	result := make([]string, 0)
	for s := range rs.store[rs.sp].tstore {
		if g.Match(s) {
			result = append(result, s)
		}
	}
	sort.Strings(result)
	return result, true
}

func (rs *RedisServer) random_key() string {
	for s := range rs.store[rs.sp].tstore {
		return s
	}
	return ""
}

func (rs *RedisServer) rename(oldkey, newkey string) {
	t := rs.getDBType(oldkey)
	if t == "none" {
		return
	}
	delete(rs.store[rs.sp].tstore, oldkey)
	rs.store[rs.sp].tstore[newkey] = dbTyp(t)
	switch t {
	case "string":
		if v, ok := rs.store[rs.sp].kv[oldkey]; ok {
			rs.store[rs.sp].kv[newkey] = v
			delete(rs.store[rs.sp].kv, oldkey)
			return
		}
	case "list":
		if v, ok := rs.store[rs.sp].ll[oldkey]; ok {
			rs.store[rs.sp].ll[newkey] = v
			delete(rs.store[rs.sp].ll, oldkey)
			return
		}
	case "set":
		if v, ok := rs.store[rs.sp].s[oldkey]; ok {
			rs.store[rs.sp].s[newkey] = v
			delete(rs.store[rs.sp].s, oldkey)
			return
		}
	}
}

func (rs *RedisServer) rename_nx(oldkey, newkey string) string {
	t := rs.getDBType(oldkey)
	t1 := rs.getDBType(newkey)
	if oldkey == newkey {
		return "-3"
	}
	if t1 != "none" {
		return "0"
	}
	if t == "none" {
		return "-1"
	}
	rs.rename(oldkey, newkey)
	return "1"
}

func (rs *RedisServer) dbsize() string {
	return strconv.Itoa(len(rs.store[rs.sp].tstore))
}

// Set Operations

func (rs *RedisServer) sadd(key, member string) string {
	t := rs.getDBType(key)
	if t != "none" && t != "set" {
		return "-2"
	}
	rs.lock.Lock()
	defer rs.lock.Unlock()
	// set our type so we know what type its associated with
	rs.store[rs.sp].tstore[key] = tSet

	_, ok := rs.store[rs.sp].s[key]
	if !ok {
		rs.store[rs.sp].s[key] = make(map[string]struct{})
	}

	if _, ok := rs.store[rs.sp].s[key][member]; ok {
		return "0"
	}

	rs.store[rs.sp].s[key][member] = struct{}{}
	return "1"
}

func (rs *RedisServer) smembers(key string) ([]string, bool) {
	_, ok := rs.store[rs.sp].s[key]
	if !ok {
		return nil, false
	}

	result := make([]string, 0)

	for v := range rs.store[rs.sp].s[key] {
		result = append(result, v)
	}
	sort.Strings(result)
	return result, true
}

func (rs *RedisServer) srem(key, member string) string {
	t := rs.getDBType(key)
	if t != "none" && t != "set" {
		return "-2"
	}
	rs.lock.Lock()
	defer rs.lock.Unlock()

	_, ok := rs.store[rs.sp].s[key][member]
	if ok {
		delete(rs.store[rs.sp].s[key], member)
		return "1"
	}
	return "0"
}

func (rs *RedisServer) scard(key string) string {
	t := rs.getDBType(key)
	if t != "none" && t != "set" {
		return "-2"
	}
	return strconv.Itoa(len(rs.store[rs.sp].s[key]))
}

func (rs *RedisServer) sismember(key, member string) string {
	t := rs.getDBType(key)
	if t != "none" && t != "set" {
		return "-2"
	}

	_, ok := rs.store[rs.sp].s[key][member]
	if !ok {
		return "0"
	}
	return "1"
}

func (rs *RedisServer) sinter(keys ...string) []string {
	result := make([]string, 0)

	// TODO: This is probably super slow and maybe not great on mem
	set := make(map[string]int)

	for _, key := range keys {
		for k := range rs.store[rs.sp].s[key] {
			set[k]++
		}
	}

	for member, i := range set {
		if i == len(keys) {
			result = append(result, member)
		}
	}

	sort.Strings(result)
	return result
}

func (rs *RedisServer) sinterstore(dstKey string, keys ...string) {
	set := make(map[string]int)
	for _, key := range keys {
		for k := range rs.store[rs.sp].s[key] {
			set[k]++
		}
	}

	newSet := make(map[string]struct{})

	rs.lock.Lock()
	defer rs.lock.Unlock()

	for member, i := range set {
		if i == len(keys) {
			newSet[member] = struct{}{}
		}
	}

	rs.store[rs.sp].tstore[dstKey] = tSet
	rs.store[rs.sp].s[dstKey] = newSet
}

func (rs *RedisServer) move(key string, dbIndex int) string {
	// target db index matches the current db index
	if int64(dbIndex) == rs.sp {
		return "-3"
	}
	if dbIndex < 0 || int64(dbIndex) > NumDBs {
		// db index is out of range
		return "-4"
	}

	// If it doesnt exist in our db or does exist in the target db return 0
	typValue, existsInOurDB := rs.store[rs.sp].tstore[key]
	_, existsInTargetDB := rs.store[dbIndex].tstore[key]
	if !existsInOurDB || existsInTargetDB {
		return "0"
	}

	rs.lock.Lock()
	defer rs.lock.Unlock()

	switch typValue {
	case tList:
		value := rs.store[rs.sp].ll[key]
		delete(rs.store[rs.sp].ll, key)
		rs.store[dbIndex].ll[key] = value
	case tSet:
		value := rs.store[rs.sp].s[key]
		delete(rs.store[rs.sp].s, key)
		rs.store[dbIndex].s[key] = value
	case tString:
		value := rs.store[rs.sp].kv[key]
		delete(rs.store[rs.sp].kv, key)
		rs.store[dbIndex].kv[key] = value
	}

	rs.store[dbIndex].tstore[key] = typValue
	delete(rs.store[rs.sp].tstore, key)
	return "1"
}

func (rs *RedisServer) flushDB() {
	rs.store[rs.sp] = NewDB()
}

func (rs *RedisServer) flushall() {
	rs.store[0] = NewDB()
	rs.store[1] = NewDB()
	rs.store[2] = NewDB()
	rs.store[3] = NewDB()
	rs.store[4] = NewDB()
	rs.store[5] = NewDB()
	rs.store[6] = NewDB()
	rs.store[7] = NewDB()
	rs.store[8] = NewDB()
	rs.store[9] = NewDB()
}

func createSaveDBTablesIfNotExists(saveDb *sql.DB) {
	typeStoreTableSQL := `CREATE TABLE IF NOT EXISTS typeStore(
		"ID" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"dbID" INTEGER NOT NULL,
		"key" TEXT NOT NULL,
		"typ" TEXT NOT NULL,
		"saveID" TEXT NOT NULL
	);`

	kvStoreTableSQL := `CREATE TABLE IF NOT EXISTS kvStore(
		"ID" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"dbID" INTEGER NOT NULL,
		"key" TEXT NOT NULL,
		"val" TEXT NOT NULL,
		"saveID" TEXT NOT NULL
	);`

	setStoreTableSQL := `CREATE TABLE IF NOT EXISTS setStore(
		"ID" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"dbID" INTEGER NOT NULL,
		"key" TEXT NOT NULL,
		"val" TEXT NOT NULL,
		"saveID" TEXT NOT NULL
	);`

	listStoreTableSQL := `CREATE TABLE IF NOT EXISTS listStore(
		"ID" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"dbID" INTEGER NOT NULL,
		"key" TEXT NOT NULL,
		"elemIndex" INTEGER NOT NULL,
		"val" TEXT NOT NULL,
		"saveID" TEXT NOT NULL
	);`

	lastSaveTableSQL := `CREATE TABLE IF NOT EXISTS lastsave(
		"saveID" TEXT NOT NULL PRIMARY KEY,
		"lastsave" INTEGER NOT NULL
	);`

	prepTypeStore, err := saveDb.Prepare(typeStoreTableSQL)
	check(err)
	defer prepTypeStore.Close()

	prepKvStore, err := saveDb.Prepare(kvStoreTableSQL)
	check(err)
	defer prepKvStore.Close()

	prepSetStore, err := saveDb.Prepare(setStoreTableSQL)
	check(err)
	defer prepSetStore.Close()

	prepListStore, err := saveDb.Prepare(listStoreTableSQL)
	check(err)
	defer prepListStore.Close()

	prepLastSave, err := saveDb.Prepare(lastSaveTableSQL)
	check(err)
	defer prepLastSave.Close()

	_, err = prepTypeStore.Exec()
	check(err)
	_, err = prepKvStore.Exec()
	check(err)
	_, err = prepSetStore.Exec()
	check(err)
	_, err = prepListStore.Exec()
	check(err)
	_, err = prepLastSave.Exec()
	check(err)

	log.Printf("Created DB Tables for Save")
}

type dbFile struct {
	db *sql.DB
	f  *os.File
}

func createSaveDBIfNotExists() *dbFile {
	// Put this into separate file and make a struct with it
	// then this wont be messed up
	dbName := "save.db"
	_, err := os.Stat(dbName)
	if err != nil {
		file, err := os.Create(dbName)
		check(err)
		file.Close()
	}

	file, err := os.Open(dbName)
	check(err)

	saveDb, err := sql.Open("sqlite3", "./save.db")
	check(err)

	return &dbFile{db: saveDb, f: file}
}

// Serializing in memory db to physical db using sqlite
func (rs *RedisServer) save() {
	saveDb := createSaveDBIfNotExists()

	createSaveDBTablesIfNotExists(saveDb.db)
	defer saveDb.db.Close()
	defer saveDb.f.Close()

	insertTypeStoreSQL := `INSERT INTO typeStore(dbID, key, typ, saveID) VALUES (?, ?, ?, ?);`
	prepTypeStore, err := saveDb.db.Prepare(insertTypeStoreSQL)
	check(err)
	defer prepTypeStore.Close()

	insertKvStoreSQL := `INSERT INTO kvStore(dbID, key, val, saveID) VALUES (?, ?, ?, ?);`
	prepKvStore, err := saveDb.db.Prepare(insertKvStoreSQL)
	check(err)
	defer prepKvStore.Close()

	inserSetStoreSQL := `INSERT INTO setStore(dbID, key, val, saveID) VALUES (?, ?, ?, ?);`
	prepSetStore, err := saveDb.db.Prepare(inserSetStoreSQL)
	check(err)
	defer prepSetStore.Close()

	insertListStoreSQL := `INSERT INTO listStore(dbID, key, elemIndex, val, saveID) VALUES (?, ?, ?, ?, ?);`
	prepListStore, err := saveDb.db.Prepare(insertListStoreSQL)
	check(err)
	defer prepListStore.Close()

	saveID := uuid.New().String()
	for dbIndex := 0; dbIndex < NumDBs; dbIndex++ {
		dbi := fmt.Sprintf("%d", dbIndex)
		for key, val := range rs.store[dbIndex].tstore {
			_, err := prepTypeStore.Exec(dbi, key, string(val), saveID)
			check(err)
		}
		for key, val := range rs.store[dbIndex].kv {
			_, err := prepKvStore.Exec(dbi, key, val, saveID)
			check(err)
		}
		for key, val := range rs.store[dbIndex].s {
			for k := range val {
				_, err := prepSetStore.Exec(dbi, key, k, saveID)
				check(err)
			}
		}
		for key, val := range rs.store[dbIndex].ll {
			i := 0
			for e := val.Front(); e != nil; e = e.Next() {
				_, err := prepListStore.Exec(dbi, key, i, e.Value, saveID)
				check(err)
				i++
			}
		}
	}

	// Update Lastsave
	// write lastsave val and uuid to new table that will be our mapping
	lastSave := time.Now().Unix()
	lastSaveInsertSQL := fmt.Sprintf(`INSERT INTO lastsave(saveID, lastsave) VALUES ("%s", %d);`, saveID, lastSave)
	prepLastSaveInsert, err := saveDb.db.Prepare(lastSaveInsertSQL)
	check(err)
	defer prepLastSaveInsert.Close()
	_, err = prepLastSaveInsert.Exec()
	check(err)

	atomic.StoreInt64(&rs.lastsave, lastSave)
}

func (rs *RedisServer) info() []string {
	// redis_version:0.07
	// connected_clients:1
	// used_memory:3187
	// last_save_time:1237655729
	// total_connections_received:1
	// total_commands_processed:1
	// uptime_in_seconds:25
	// uptime_in_days:0
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	now := time.Now().Unix()
	uptimeInSecs := now - rs.timeStarted
	//							   min  hr   day
	uptimeInDays := uptimeInSecs / 60 / 60 / 24

	versionString := fmt.Sprintf("server_version:%s\n", ServerVersion)
	connsString := fmt.Sprintf("connected_clients:%d\n", len(rs.conns))
	usedMemString := fmt.Sprintf("used_memory:%d\n", m.Alloc)
	lastSaveString := fmt.Sprintf("last_save_time:%d\n", rs.lastsave)
	totConnRecv := fmt.Sprintf("total_connections_received:%d\n", rs.totalConnsReceived)
	totCommProc := fmt.Sprintf("total_commands_processed:%d\n", rs.commandsProcessed)
	uptInSecString := fmt.Sprintf("uptime_in_seconds:%d\n", uptimeInSecs)
	uptInDayString := fmt.Sprintf("uptime_in_days:%d\n", uptimeInDays)

	return []string{
		versionString,
		connsString,
		usedMemString,
		lastSaveString,
		totConnRecv,
		totCommProc,
		uptInSecString,
		uptInDayString,
	}
}
