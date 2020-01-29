package inst

import (
	"context"
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
	cl.Delete(clientCtx, testPrefix, clientv3.WithPrefix())
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

func Test1(t *testing.T) {
	// t.Error()
	cl := newConnect()
	defer cl.Close()
	erase(cl)
	//defer erase(cl)
	New(cl, testPrefix, "000")

	if count(cl, "") != 3 { //NS counter, NameSpace and keys counter
		t.Error()
	}
}

func Test2(t *testing.T) {
	cl := newConnect()
	defer cl.Close()
	erase(cl)
	//	defer erase(cl)

	New(cl, testPrefix, "000")
	New(cl, testPrefix, "001")

	if count(cl, "") != 5 { // NS counter, 2 NameSpaces and 2 keys counters
		t.Error()
	}

	New(cl, testPrefix, "002")
	New(cl, testPrefix, "003")
	// time.Sleep(10 * time.Millisecond)

	if count(cl, "") != 9 { // NS counter, 4 NameSpaces and 4 keys counter
		t.Error()
	}
	time.Sleep(2000)
}

func Test3(t *testing.T) {
	cl := newConnect()
	defer cl.Close()
	erase(cl)

	//defer erase(cl)

	var wg sync.WaitGroup
	f := func(b, e int) {
		defer wg.Done()
		for i := b; i < e; i++ {
			New(cl, testPrefix, strconv.Itoa(i))
		}
	}

	wg.Add(2)
	f(0, 50)
	f(10, 100)

	// time.Sleep(100)
	wg.Wait()

	if c := count(cl, ""); c != 201 { // 1 NS counter, 1000 NameSpace and 1000 keys counter
		log.Println(c)
		t.Error()
	}
	time.Sleep(2000)
}

func Test4(t *testing.T) {
	cl := newConnect()
	defer cl.Close()
	erase(cl)

	// defer erase(cl)

	s := New(cl, testPrefix, "0001")
	s.Choose("0001").Key("1")

	if c := count(cl, uint32ToString(s.Choose("0001").nameSpaceID)); c != 2 { // 1 key and IDs counter
		t.Error()
	}

	if c := count(cl, uint32ToString(s.Choose("0001").nameSpaceID)+uint32ToString(typeKEY)); c != 1 { // 1 key
		t.Error()
	}

	if p := get(cl, uint32ToString(s.Choose("0001").nameSpaceID)+uint32ToString(typeKEY)+"1"); string(p.Kvs[0].Value) != uint32ToString(1) {
		t.Error()
	}

}

func Test5(t *testing.T) {
	cl := newConnect()
	defer cl.Close()
	erase(cl)

	// defer erase(cl)

	var wg sync.WaitGroup
	s := New(cl, testPrefix, "0001")

	f := func(b, e int) {
		for i := b; i < e; i++ {
			s.Choose("0001").Key(strconv.Itoa(i))
		}
		wg.Done()
	}
	wg.Add(2)
	go f(0, 1000)
	go f(500, 1500)

	wg.Wait()

	if c := count(cl, ""); c != 1503 { // 1500 keys and 1 ID counter,1 NameSpace and NS counter
		log.Println(c)
		t.Error()
	}

	if c := count(cl, uint32ToString(s.Choose("0001").nameSpaceID)+uint32ToString(typeKEY)); c != 1500 { // 1500 keys
		t.Error()
	}

}

func Test6(t *testing.T) {
	cl := newConnect()
	defer cl.Close()
	erase(cl)

	// defer erase(cl)

	var wg sync.WaitGroup
	s1 := New(cl, testPrefix, "0001")
	s2 := New(cl, testPrefix, "0002")

	f := func(b, e int) {
		for i := b; i < e; i++ {
			s1.Choose("0001").Key(strconv.Itoa(i))
			s2.Choose("0002").Key(strconv.Itoa(i))
		}
		wg.Done()
	}
	wg.Add(2)
	go f(0, 1000)
	go f(500, 1500)

	wg.Wait()

	if c := count(cl, ""); c != 3005 { // 2x1500 keys and 2 ID counter, 2 NameSpace and NS counter
		t.Error()
	}

	if c := count(cl, uint32ToString(s1.Choose("0001").nameSpaceID)+uint32ToString(typeKEY)); c != 1500 { // 1500 keys
		t.Error()
	}

	if c := count(cl, uint32ToString(s2.Choose("0002").nameSpaceID)+uint32ToString(typeKEY)); c != 1500 { // 1500 keys
		t.Error()
	}
}

func Test7(t *testing.T) {
	cl := newConnect()
	defer cl.Close()
	erase(cl)

	// defer erase(cl)

	s1 := New(cl, testPrefix, "0001")

	f := func(s *Inst, b, e int) {
		for i := b; i < e; i++ {
			s.Choose("0001").Key(strconv.Itoa(i))
		}
	}

	f(s1, 1000, 1010)
	// localWrite
	m := s1.Choose("0001").Load().(maps).strToID
	k := s1.Choose("0001").Load().(maps).idToStr

	if m["1000"] != 1 || *k[1] != "1000" {
		t.Error()
	}
	if m["1002"] != 3 || *k[3] != "1002" {
		t.Error()
	}

	// standart init
	s1 = New(cl, testPrefix, "0001")
	m = s1.Choose("0001").Load().(maps).strToID
	k = s1.Choose("0001").Load().(maps).idToStr

	if m["1000"] != 1 || *k[1] != "1000" {
		t.Error()
	}
	if m["1002"] != 3 || *k[3] != "1002" {
		t.Error()
	}
	if m["1006"] != 7 || *k[7] != "1006" {
		t.Error()
	}
	if m["1009"] != 10 || *k[10] != "1009" {
		t.Error()
	}

	// watcher check
	s2 := New(cl, testPrefix, "0001")
	f(s2, 1010, 1020)
	time.Sleep(10 * time.Millisecond)
	m = s1.Choose("0001").Load().(maps).strToID
	k = s1.Choose("0001").Load().(maps).idToStr

	id, _ := s1.Choose("0001").Key("1010")
	st, _ := s1.Choose("0001").ID(11)
	if m["1010"] != 11 || *k[11] != "1010" || id != 11 || *st != "1010" {
		t.Error()
	}

	if m["1015"] != 16 || *k[16] != "1015" {
		t.Error()
	}
}

func Test8(t *testing.T) {
	cl := newConnect()
	defer cl.Close()
	erase(cl)

	// defer erase(cl)

	var wg sync.WaitGroup
	s1 := New(cl, testPrefix, "0001")

	f := func(s *Inst, b, e int) {
		for i := b; i < e; i++ {
			s.Choose("0001").Key(strconv.Itoa(i))
		}
		wg.Done()
	}

	wg.Add(9)
	go f(s1, 1000, 2500)
	go f(s1, 1000, 2500)
	go f(s1, 3000, 4500)
	go f(s1, 2000, 3500)
	go f(s1, 2000, 5500)
	go f(s1, 2000, 5500)
	go f(s1, 0, 1000)
	go f(s1, 5000, 10000)
	go f(s1, 3000, 4500)

	wg.Wait()
	// localWrite
	m := s1.Choose("0001").Load().(maps).strToID
	k := s1.Choose("0001").Load().(maps).idToStr

	if len(m) != 10000 || len(k) != 10001 {
		log.Println(len(m), len(k))
		t.Error()
	}

	for i, v := range m {
		if *k[v] != i {
			t.Error()
		}
	}

	for i, v := range k[1:] {
		if v == nil {
			t.Error()
		}

		if m[*v] != uint32(i+1) {
			t.Error()
		}

	}
}

func TestAll(t *testing.T) {
	{
		// Test 1
		cl := newConnect()
		defer cl.Close()
		erase(cl)
		//defer erase(cl)
		New(cl, testPrefix, "000")

		if count(cl, "") != 3 { //NS counter, NameSpace and keys counter
			t.Error()
		}
	}

	{
		// Test 2
		cl := newConnect()
		defer cl.Close()
		erase(cl)
		//	defer erase(cl)

		New(cl, testPrefix, "000")
		New(cl, testPrefix, "001")

		if count(cl, "") != 5 { // NS counter, 2 NameSpaces and 2 keys counters
			t.Error()
		}

		New(cl, testPrefix, "002")
		New(cl, testPrefix, "003")
		// time.Sleep(10 * time.Millisecond)

		if count(cl, "") != 9 { // NS counter, 4 NameSpaces and 4 keys counter
			t.Error()
		}
		time.Sleep(2000)
	}

	{
		// Test 3
		cl := newConnect()
		defer cl.Close()
		erase(cl)

		//defer erase(cl)

		var wg sync.WaitGroup
		f := func(b, e int) {
			defer wg.Done()
			for i := b; i < e; i++ {
				New(cl, testPrefix, strconv.Itoa(i))
			}
		}

		wg.Add(2)
		f(0, 50)
		f(10, 100)

		// time.Sleep(100)
		wg.Wait()

		if c := count(cl, ""); c != 201 { // 1 NS counter, 1000 NameSpace and 1000 keys counter
			log.Println(c)
			t.Error()
		}
		time.Sleep(2000)
	}

	{
		// Test 4
		cl := newConnect()
		defer cl.Close()
		erase(cl)

		defer erase(cl)

		s := New(cl, testPrefix, "0001")
		s.Choose("0001").Key("1")

		if c := count(cl, uint32ToString(s.Choose("0001").nameSpaceID)); c != 2 { // 1 key and IDs counter
			t.Error()
		}

		if c := count(cl, uint32ToString(s.Choose("0001").nameSpaceID)+uint32ToString(typeKEY)); c != 1 { // 1 key
			t.Error()
		}

		if p := get(cl, uint32ToString(s.Choose("0001").nameSpaceID)+uint32ToString(typeKEY)+"1"); string(p.Kvs[0].Value) != uint32ToString(1) {
			t.Error()
		}

	}

	{
		// Test 5
		cl := newConnect()
		defer cl.Close()
		erase(cl)

		// defer erase(cl)

		var wg sync.WaitGroup
		s := New(cl, testPrefix, "0001")

		f := func(b, e int) {
			for i := b; i < e; i++ {
				s.Choose("0001").Key(strconv.Itoa(i))
			}
			wg.Done()
		}
		wg.Add(2)
		go f(0, 1000)
		go f(500, 1500)

		wg.Wait()

		if c := count(cl, ""); c != 1503 { // 1500 keys and 1 ID counter,1 NameSpace and NS counter
			log.Println(c)
			t.Error()
		}

		if c := count(cl, uint32ToString(s.Choose("0001").nameSpaceID)+uint32ToString(typeKEY)); c != 1500 { // 1500 keys
			t.Error()
		}

	}

	{
		// Test 6
		cl := newConnect()
		defer cl.Close()
		erase(cl)

		// defer erase(cl)

		var wg sync.WaitGroup
		s1 := New(cl, testPrefix, "0001")
		s2 := New(cl, testPrefix, "0002")

		f := func(b, e int) {
			defer wg.Done()
			for i := b; i < e; i++ {
				s1.Choose("0001").Key(strconv.Itoa(i))
				s2.Choose("0002").Key(strconv.Itoa(i))
			}
		}
		wg.Add(2)
		go f(0, 1000)
		go f(500, 1500)

		wg.Wait()

		if c := count(cl, ""); c != 3005 { // 2x1500 keys and 2 ID counter, 2 NameSpace and NS counter
			t.Error()
		}

		if c := count(cl, uint32ToString(s1.Choose("0001").nameSpaceID)+uint32ToString(typeKEY)); c != 1500 { // 1500 keys
			t.Error()
		}

		if c := count(cl, uint32ToString(s2.Choose("0002").nameSpaceID)+uint32ToString(typeKEY)); c != 1500 { // 1500 keys
			t.Error()
		}
	}

	{
		// Test 7
		cl := newConnect()
		defer cl.Close()
		erase(cl)

		// defer erase(cl)

		s1 := New(cl, testPrefix, "0001")

		f := func(s *Inst, b, e int) {
			for i := b; i < e; i++ {
				s.Choose("0001").Key(strconv.Itoa(i))
			}
		}

		f(s1, 1000, 1010)
		// localWrite
		m := s1.Choose("0001").Load().(maps).strToID
		k := s1.Choose("0001").Load().(maps).idToStr

		if m["1000"] != 1 || *k[1] != "1000" {
			t.Error()
		}
		if m["1002"] != 3 || *k[3] != "1002" {
			t.Error()
		}

		// standart init
		s1 = New(cl, testPrefix, "0001")
		m = s1.Choose("0001").Load().(maps).strToID
		k = s1.Choose("0001").Load().(maps).idToStr

		if m["1000"] != 1 || *k[1] != "1000" {
			t.Error()
		}
		if m["1002"] != 3 || *k[3] != "1002" {
			t.Error()
		}
		if m["1006"] != 7 || *k[7] != "1006" {
			t.Error()
		}
		if m["1009"] != 10 || *k[10] != "1009" {
			t.Error()
		}

		// watcher check
		s2 := New(cl, testPrefix, "0001")
		f(s2, 1010, 1020)
		time.Sleep(10 * time.Millisecond)
		m = s1.Choose("0001").Load().(maps).strToID
		k = s1.Choose("0001").Load().(maps).idToStr

		id, _ := s1.Choose("0001").Key("1010")
		st, _ := s1.Choose("0001").ID(11)
		if m["1010"] != 11 || *k[11] != "1010" || id != 11 || *st != "1010" {
			t.Error()
		}

		if m["1015"] != 16 || *k[16] != "1015" {
			t.Error()
		}
	}

	{
		// Test 8
		cl := newConnect()
		defer cl.Close()
		erase(cl)

		// defer erase(cl)

		var wg sync.WaitGroup
		s1 := New(cl, testPrefix, "0001")

		f := func(s *Inst, b, e int) {
			defer wg.Done()
			for i := b; i < e; i++ {
				s.Choose("0001").Key(strconv.Itoa(i))
			}
		}

		wg.Add(9)
		go f(s1, 1000, 2500)
		go f(s1, 1000, 2500)
		go f(s1, 3000, 4500)
		go f(s1, 2000, 3500)
		go f(s1, 2000, 5500)
		go f(s1, 2000, 5500)
		go f(s1, 0, 1000)
		go f(s1, 5000, 10000)
		go f(s1, 3000, 4500)

		wg.Wait()
		// localWrite
		m := s1.Choose("0001").Load().(maps).strToID
		k := s1.Choose("0001").Load().(maps).idToStr

		if len(m) != 10000 || len(k) != 10001 {
			log.Println(len(m), len(k))
			t.Error()
		}

		for i, v := range m {
			if *k[v] != i {
				t.Error()
			}
		}

		for i, v := range k[1:] {
			if v == nil {
				t.Error()
			}

			if m[*v] != uint32(i+1) {
				t.Error()
			}
		}
	}
}
