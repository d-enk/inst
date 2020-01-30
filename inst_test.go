package inst

import (
	"context"
	"errors"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
)

var testPrefix = "test"

func newServer() {

}

func newConnect() *clientv3.Client {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	return etcdCli
}

func erase(cl *clientv3.Client) {
	clientCtx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	_, err := cl.Delete(clientCtx, testPrefix, clientv3.WithPrefix())
	if err != nil {
		log.Fatal(err.Error())
	}
	cancel()
}

func count(cl *clientv3.Client, key string) int64 {
	clientCtx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	p, _ := cl.Get(clientCtx, testPrefix+key, clientv3.WithPrefix())
	cancel()
	return p.Count
}

func get(cl *clientv3.Client, key string) *clientv3.GetResponse {
	clientCtx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	p, _ := cl.Get(clientCtx, testPrefix+key, clientv3.WithPrefix())
	cancel()
	return p
}

func checkMaps(ns *NameSpaceMaps, l int) error {

	m := ns.maps.Load().(maps).strToID
	k := ns.maps.Load().(maps).idToStr

	// total number of keys in local structs
	if len(m) != l || len(k) != l+1 {
		log.Println(len(m), len(k))
		return errors.New("not correct lens")
	}

	// check that key -> id correct
	for key, id := range m {
		log.Println(key, id)

		checkID, err1 := ns.Key(key)
		if err1 != nil {
			return err1
		}

		if checkID != id {
			return errors.New("not correct id from Key func")
		}

		if *k[id] != key {
			return errors.New("not correct id by key")
		}
	}

	// check that id -> key correct
	for id, key := range k[1:] {
		if key == nil {
			return errors.New("no key by id")
		}

		checkKey, err2 := ns.ID(uint32(id + 1))
		if err2 != nil {
			return err2
		}

		if *checkKey != *key {
			return errors.New("not correct key from ID func")
		}

		if m[*key] != uint32(id+1) {
			return errors.New("not correct key by id")
		}

	}
	return nil
}

func TestNew(t *testing.T) {
	cl := newConnect()
	defer cl.Close()
	erase(cl)
	//	defer erase(cl)

	// create new namespace 000
	New(cl, testPrefix, "000")

	if count(cl, "") != 3 { //NS counter, NameSpace and keys counter
		t.Error()
	}

	New(cl, testPrefix, "000") // local init (not create new records in etcd)
	New(cl, testPrefix, "001")

	if count(cl, "") != 5 { // NS counter, 2 NameSpaces(000,001) and 2 keys counters
		t.Error()
	}

	New(cl, testPrefix, "002")
	New(cl, testPrefix, "003")

	if count(cl, "") != 9 { // NS counter, 4 NameSpaces and 4 keys counter
		t.Error()
	}

}

// thread-safety check for creating new namespaces
func TestSTnew(t *testing.T) {
	cl := newConnect()
	defer cl.Close()
	erase(cl)

	//defer erase(cl)

	log.Println("test 2 before")

	var wg sync.WaitGroup
	f := func(b, e int) {
		defer wg.Done()
		for i := b; i < e; i++ {
			New(cl, testPrefix, strconv.Itoa(i))
			// log.Println("in")
		}
	}

	wg.Add(2)
	go f(0, 25)
	go f(10, 50)

	wg.Wait()
	log.Println("test 2 after")

	if c := count(cl, ""); c != 101 { // 1 NS counter, 100 NameSpace and 100 keys counter
		log.Println(c)
		t.Error()
	}
}

// cheking corectly key & id work
func Test3(t *testing.T) {
	cl := newConnect()
	defer cl.Close()
	defer erase(cl)

	// defer erase(cl)
	log.Println("test 3 before")

	s := New(cl, testPrefix, "0001")
	log.Println("test 3 after New")

	ns := s.Choose("0001")

	ns.Key("1") // write new keys
	ns.Key("3")
	ns.Key("5")

	// cheking key in etcd
	if c := count(cl, uint32ToString(ns.nameSpaceID)); c != 4 { // 3 key and IDs counter
		t.Error()
	}

	if c := count(cl, uint32ToString(ns.nameSpaceID)+uint32ToString(typeKEY)); c != 3 { // 1 key
		t.Error()
	}

	if p := get(cl, uint32ToString(ns.nameSpaceID)+uint32ToString(typeKEY)+"1"); string(p.Kvs[0].Value) != uint32ToString(1) {
		t.Error()
	}

	if p := get(cl, uint32ToString(ns.nameSpaceID)+uint32ToString(typeKEY)+"3"); string(p.Kvs[0].Value) != uint32ToString(2) {
		t.Error()
	}

	if p := get(cl, uint32ToString(ns.nameSpaceID)+uint32ToString(typeKEY)+"5"); string(p.Kvs[0].Value) != uint32ToString(3) {
		t.Error()
	}

	if err := checkMaps(ns, 3); err != nil {
		t.Error(err)
	}

	log.Println("test 3 after")
}

// checking that init correctly
func TestInit(t *testing.T) {
	cl := newConnect()
	defer cl.Close()
	defer erase(cl)

	s1 := New(cl, testPrefix, "0001")

	f := func(s *Inst, b, e int) {
		for i := b; i < e; i++ {
			log.Println(i)
			s.Choose("0001").Key(strconv.Itoa(i))
		}
	}

	log.Println("first write")
	f(s1, 1000, 1010)
	// checking localWrite
	if err := checkMaps(s1.Choose("0001"), 10); err != nil {
		t.Error(err)
	}

	// standart init
	s2 := New(cl, testPrefix, "0001") // should load keys from etcd
	if err := checkMaps(s2.Choose("0001"), 10); err != nil {
		t.Error(err)
	}
}

// checking that watcher work correctly
func TestWatcher(t *testing.T) {
	cl := newConnect()
	defer cl.Close()
	defer erase(cl)

	s1 := New(cl, testPrefix, "0001")

	f := func(s *Inst, b, e int) {
		for i := b; i < e; i++ {
			s.Choose("0001").Key(strconv.Itoa(i))
		}
	}

	// s1 should get new recoreds from s2
	log.Println("watcher")
	s2 := New(cl, testPrefix, "0001")
	f(s2, 1010, 1020) // s2 write new records at same namespace as s1

	time.Sleep(100 * time.Millisecond) // wait while s1 watcher work

	if err := checkMaps(s1.Choose("0001"), 10); err != nil {
		t.Error(err)
	}

}

// checking thread-safety watcher work
func TestTSwatcher(t *testing.T) {
	cl := newConnect()
	defer cl.Close()
	defer erase(cl)

	// defer erase(cl)
	log.Println("test 6 start")

	var wg sync.WaitGroup
	s1 := New(cl, testPrefix, "0001")
	s2 := New(cl, testPrefix, "0001")

	f := func(b, e int) {
		for i := b; i < e; i++ {
			s1.Choose("0001").Key(strconv.Itoa(i))
			s2.Choose("0001").Key(strconv.Itoa(i))
			// log.Println("test 5 in")
		}
		wg.Done()
	}
	wg.Add(2)
	go f(0, 1000)
	go f(500, 1500)

	wg.Wait()
	time.Sleep(1000 * time.Millisecond) //wait wathers

	log.Println("test 6 end")

	if c := count(cl, ""); c != 1503 { // 1500 keys and 1 ID counter, 1 NameSpace and NS counter
		log.Println(c)
		t.Error()
	}

	if c := count(cl, uint32ToString(s1.Choose("0001").nameSpaceID)+uint32ToString(typeKEY)); c != 1500 { // 1500 keys
		t.Error()
	}

	if c := count(cl, uint32ToString(s2.Choose("0001").nameSpaceID)+uint32ToString(typeKEY)); c != 1500 { // 1500 keys
		t.Error()
	}

	if err := checkMaps(s1.Choose("0001"), 1500); err != nil {
		t.Error(err)
	}

	if err := checkMaps(s2.Choose("0001"), 1500); err != nil {
		t.Error(err)
	}
}

// stress test thread-safety key records
func TestTSkey(t *testing.T) {
	cl := newConnect()
	defer cl.Close()
	defer erase(cl)

	var wg sync.WaitGroup
	s1 := New(cl, testPrefix, "0001")

	f := func(s *Inst, b, e int) {
		for i := b; i < e; i++ {
			s.Choose("0001").Key(strconv.Itoa(i))
		}
		wg.Done()
	}

	wg.Add(7)
	go f(s1, 1000, 2500)
	go f(s1, 1000, 2500)
	go f(s1, 3000, 4500)
	go f(s1, 2000, 3500)
	go f(s1, 4000, 5500)
	go f(s1, 0, 1000)
	go f(s1, 3000, 4500)

	wg.Wait()

	// total number of keys in etcd
	if count(cl, uint32ToString(s1.Choose("0001").nameSpaceID)+uint32ToString(typeKEY)) != 5500 {
		t.Error()
	}

	if err := checkMaps(s1.Choose("0001"), 5500); err != nil {
		t.Error(err)
	}
}

func TestAll(t *testing.T) {
	{
		cl := newConnect()
		defer cl.Close()
		erase(cl)
		//	defer erase(cl)
	
		// create new namespace 000
		New(cl, testPrefix, "000")
	
		if count(cl, "") != 3 { //NS counter, NameSpace and keys counter
			t.Error()
		}
	
		New(cl, testPrefix, "000") // local init (not create new records in etcd)
		New(cl, testPrefix, "001")
	
		if count(cl, "") != 5 { // NS counter, 2 NameSpaces(000,001) and 2 keys counters
			t.Error()
		}
	
		New(cl, testPrefix, "002")
		New(cl, testPrefix, "003")
	
		if count(cl, "") != 9 { // NS counter, 4 NameSpaces and 4 keys counter
			t.Error()
		}
	
	}
	
	// thread-safety check for creating new namespaces
	{
		cl := newConnect()
		defer cl.Close()
		erase(cl)
	
		//defer erase(cl)
	
		log.Println("test 2 before")
	
		var wg sync.WaitGroup
		f := func(b, e int) {
			defer wg.Done()
			for i := b; i < e; i++ {
				New(cl, testPrefix, strconv.Itoa(i))
				// log.Println("in")
			}
		}
	
		wg.Add(2)
		go f(0, 25)
		go f(10, 50)
	
		wg.Wait()
		log.Println("test 2 after")
	
		if c := count(cl, ""); c != 101 { // 1 NS counter, 100 NameSpace and 100 keys counter
			log.Println(c)
			t.Error()
		}
	}
	
	// cheking corectly key & id work
	{
		cl := newConnect()
		defer cl.Close()
		erase(cl)
	
		// defer erase(cl)
		log.Println("test 3 before")
	
		s := New(cl, testPrefix, "0001")
		log.Println("test 3 after New")
	
		ns := s.Choose("0001")
	
		ns.Key("1") // write new keys
		ns.Key("3")
		ns.Key("5")
	
		// cheking key in etcd
		if c := count(cl, uint32ToString(ns.nameSpaceID)); c != 4 { // 3 key and IDs counter
			t.Error()
		}
	
		if c := count(cl, uint32ToString(ns.nameSpaceID)+uint32ToString(typeKEY)); c != 3 { // 1 key
			t.Error()
		}
	
		if p := get(cl, uint32ToString(ns.nameSpaceID)+uint32ToString(typeKEY)+"1"); string(p.Kvs[0].Value) != uint32ToString(1) {
			t.Error()
		}
	
		if p := get(cl, uint32ToString(ns.nameSpaceID)+uint32ToString(typeKEY)+"3"); string(p.Kvs[0].Value) != uint32ToString(2) {
			t.Error()
		}
	
		if p := get(cl, uint32ToString(ns.nameSpaceID)+uint32ToString(typeKEY)+"5"); string(p.Kvs[0].Value) != uint32ToString(3) {
			t.Error()
		}
	
		if err := checkMaps(ns, 3); err != nil {
			t.Error(err)
		}
	
		log.Println("test 3 after")
	}
	
	// checking that init correctly
	{
		cl := newConnect()
		defer cl.Close()
		erase(cl)
	
		s1 := New(cl, testPrefix, "0001")
	
		f := func(s *Inst, b, e int) {
			for i := b; i < e; i++ {
				log.Println(i)
				s.Choose("0001").Key(strconv.Itoa(i))
			}
		}
	
		log.Println("first write")
		f(s1, 1000, 1010)
		// checking localWrite
		if err := checkMaps(s1.Choose("0001"), 10); err != nil {
			t.Error(err)
		}
	
		// standart init
		s2 := New(cl, testPrefix, "0001") // should load keys from etcd
		if err := checkMaps(s2.Choose("0001"), 10); err != nil {
			t.Error(err)
		}
	}
	
	// checking that watcher work correctly
	{
		cl := newConnect()
		defer cl.Close()
		erase(cl)
	
		s1 := New(cl, testPrefix, "0001")
	
		f := func(s *Inst, b, e int) {
			for i := b; i < e; i++ {
				s.Choose("0001").Key(strconv.Itoa(i))
			}
		}
	
		// s1 should get new recoreds from s2
		log.Println("watcher")
		s2 := New(cl, testPrefix, "0001")
		f(s2, 1010, 1020) // s2 write new records at same namespace as s1
	
		time.Sleep(100 * time.Millisecond) // wait while s1 watcher work
	
		if err := checkMaps(s1.Choose("0001"), 10); err != nil {
			t.Error(err)
		}
	
	}
	
	// checking thread-safety watcher work
	{
		cl := newConnect()
		defer cl.Close()
		erase(cl)
	
		// defer erase(cl)
		log.Println("test 6 start")
	
		var wg sync.WaitGroup
		s1 := New(cl, testPrefix, "0001")
		s2 := New(cl, testPrefix, "0001")
	
		f := func(b, e int) {
			for i := b; i < e; i++ {
				s1.Choose("0001").Key(strconv.Itoa(i))
				s2.Choose("0001").Key(strconv.Itoa(i))
				// log.Println("test 5 in")
			}
			wg.Done()
		}
		wg.Add(2)
		go f(0, 1000)
		go f(500, 1500)
	
		wg.Wait()
		time.Sleep(1000 * time.Millisecond) //wait wathers
	
		log.Println("test 6 end")
	
		if c := count(cl, ""); c != 1503 { // 1500 keys and 1 ID counter, 1 NameSpace and NS counter
			log.Println(c)
			t.Error()
		}
	
		if c := count(cl, uint32ToString(s1.Choose("0001").nameSpaceID)+uint32ToString(typeKEY)); c != 1500 { // 1500 keys
			t.Error()
		}
	
		if c := count(cl, uint32ToString(s2.Choose("0001").nameSpaceID)+uint32ToString(typeKEY)); c != 1500 { // 1500 keys
			t.Error()
		}
	
		if err := checkMaps(s1.Choose("0001"), 1500); err != nil {
			t.Error(err)
		}
	
		if err := checkMaps(s2.Choose("0001"), 1500); err != nil {
			t.Error(err)
		}
	}
	
	// stress test thread-safety key records
	{
		cl := newConnect()
		defer cl.Close()
		erase(cl)
	
		var wg sync.WaitGroup
		s1 := New(cl, testPrefix, "0001")
	
		f := func(s *Inst, b, e int) {
			for i := b; i < e; i++ {
				s.Choose("0001").Key(strconv.Itoa(i))
			}
			wg.Done()
		}
	
		wg.Add(7)
		go f(s1, 1000, 2500)
		go f(s1, 1000, 2500)
		go f(s1, 3000, 4500)
		go f(s1, 2000, 3500)
		go f(s1, 4000, 5500)
		go f(s1, 0, 1000)
		go f(s1, 3000, 4500)
	
		wg.Wait()
	
		// total number of keys in etcd
		if count(cl, uint32ToString(s1.Choose("0001").nameSpaceID)+uint32ToString(typeKEY)) != 5500 {
			t.Error()
		}
	
		if err := checkMaps(s1.Choose("0001"), 5500); err != nil {
			t.Error(err)
		}
	}
}
