package inst

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

const (
	typeKEY    uint32 = 0
	typeID     uint32 = 1
	typeNextID uint32 = 2
)

var ccc int32

// Inst /
type Inst struct {
	nameSpaces map[string]*NameSpaceMaps
}

// New NameSpaceMaps with ... string
func New(etcdClient *clientv3.Client, prefix string, nameSpace ...string) *Inst {
	s := &Inst{make(map[string]*NameSpaceMaps, len(nameSpace))}

	NScounter := join(prefix, uint32ToString(typeNextID))
	clientCtx, cancel := context.WithTimeout(context.Background(), 10000000*time.Millisecond)
	etcdClient.Txn(clientCtx).
		If(clientv3.Compare(clientv3.CreateRevision(NScounter), "=", 0)).
		Then(clientv3.OpPut(NScounter, uint32ToString(1))).Commit()
	cancel()

	for i := range nameSpace {
		s.nameSpaces[nameSpace[i]] = newInst(etcdClient, prefix, nameSpace[i])
	}

	return s
}

// Choose /
func (s *Inst) Choose(nameSpace string) *NameSpaceMaps {
	return s.nameSpaces[nameSpace]
}

// Shutdown close all
func (s *Inst) Shutdown() {
	for _, v := range s.nameSpaces {
		v.Close()
	}
}

// NameSpaceMaps /
type NameSpaceMaps struct {
	etcdClient  *clientv3.Client
	prefix      string
	nameSpace   string
	nameSpaceID uint32

	stopCntChan chan struct{}
	stopKeyChan chan struct{}
	counter     localCounter

	maps atomic.Value
	m sync.Mutex
}

type maps struct {
	strToID map[string]uint32
	idToStr []*string
}

type localCounter struct {
	num uint32
	rev int64
	sync.Mutex
}

// New version 2
func newInst(etcdClient *clientv3.Client, prefix, nameSpace string) *NameSpaceMaps {
	this := NameSpaceMaps{
		etcdClient:  etcdClient,
		prefix:      prefix,
		nameSpace:   nameSpace,
		nameSpaceID: 0,
	}

	this.checkNameSpaceID()
	this.getNameSpaceValues()
	return &this
}

func (s *NameSpaceMaps) checkNameSpaceID() {
	NSname := join(s.prefix, s.nameSpace)
	NScounter := join(s.prefix, uint32ToString(typeNextID))

	clientCtx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	txnResp, err := s.etcdClient.Txn(clientCtx).
		If(clientv3.Compare(clientv3.CreateRevision(NSname), "!=", 0)).
		Then(clientv3.OpGet(NSname)).
		Else(clientv3.OpGet(NScounter)).Commit()
	cancel()
	if err != nil {
		log.Println(err)
	}

	s.nameSpaceID = byteToUInt32(txnResp.Responses[0].GetResponseRange().GetKvs()[0].Value)
	if !txnResp.Succeeded {
		for {
			IDcounter := join(s.prefix, uint32ToString(s.nameSpaceID), uint32ToString(typeNextID))
			modRevision := txnResp.Responses[0].GetResponseRange().GetKvs()[0].ModRevision

			clientCtx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
			txnResp, err = s.etcdClient.Txn(clientCtx).
				If(clientv3.Compare(clientv3.CreateRevision(NSname), "=", 0),
					clientv3.Compare(clientv3.ModRevision(NScounter), "=", modRevision)).
				Then(clientv3.OpPut(NSname, uint32ToString(s.nameSpaceID)),
					clientv3.OpPut(NScounter, uint32ToString(s.nameSpaceID+1)),
					clientv3.OpPut(IDcounter, uint32ToString(1))).
				Else(clientv3.OpGet(NScounter),
					clientv3.OpGet(NSname)).Commit()
			cancel()
			if err != nil {
				log.Println(err)
			}

			if txnResp.Succeeded {
				break
			} else if r := txnResp.Responses[1].GetResponseRange(); r.Count == 1 {
				s.nameSpaceID = byteToUInt32(r.GetKvs()[0].Value)
				break
			}
			s.nameSpaceID = byteToUInt32(txnResp.Responses[0].GetResponseRange().GetKvs()[0].Value)
		}
	}
}

// Key string -> uint32
func (s *NameSpaceMaps) Key(key string) (uint32, error) {
	m := s.maps.Load().(maps)
	if m.strToID[key] == 0 {
		return s.addKey(&key)
	}
	return m.strToID[key], nil
}

// ID uint32 -> string
func (s *NameSpaceMaps) ID(id uint32) (*string, error) {
	m := s.maps.Load().(maps)
	if id >= uint32(len(m.idToStr)) {
		IDcounter := join(s.prefix, uint32ToString(s.nameSpaceID), uint32ToString(typeNextID))
		clientCtx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
		getResp, err := s.etcdClient.Get(clientCtx, IDcounter)
		cancel()
		if err != nil {
			return nil, err
		}
		if byteToUInt32(getResp.Kvs[0].Value) <= id {
			return nil, errors.New("out of range")
		}
		time.Sleep(20 * time.Millisecond)
		m := s.maps.Load().(maps)
		if id >= uint32(len(m.idToStr)) {
			return nil, errors.New("timed out")
		}
	}
	return m.idToStr[id], nil
}

// added key in etcd
func (s *NameSpaceMaps) addKey(key *string) (uint32, error) {
	KeyWithPrefix := join(s.prefix, uint32ToString(s.nameSpaceID), uint32ToString(typeKEY), *key)
	IDcounter := join(s.prefix, uint32ToString(s.nameSpaceID), uint32ToString(typeNextID))

	for {
		s.counter.Lock()
		ident := s.counter.num
		modRevision := s.counter.rev
		s.counter.Unlock()

		clientCtx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
		txnResp, err := s.etcdClient.Txn(clientCtx).
			If(clientv3.Compare(clientv3.CreateRevision(KeyWithPrefix), "=", 0),
				clientv3.Compare(clientv3.ModRevision(IDcounter), "=", modRevision)).
			Then(clientv3.OpPut(KeyWithPrefix, uint32ToString(ident)),
				clientv3.OpPut(IDcounter, uint32ToString(ident+1))).
			Else(clientv3.OpGet(IDcounter),
				clientv3.OpGet(KeyWithPrefix)).Commit()
		cancel()
		if err != nil {
			log.Println(err)
			return 0, err
		}

		if txnResp.Succeeded {
			s.localWrite(key, ident)
			s.counter.Lock()
			if ident+1 > s.counter.num {
				s.counter.num = ident + 1
				s.counter.rev = txnResp.Responses[1].GetResponsePut().Header.Revision
			}
			s.counter.Unlock()

			return ident + 1, nil

			// key found
		} else if r := txnResp.Responses[1].GetResponseRange(); r.Count == 1 {
			ident = byteToUInt32(r.GetKvs()[0].Value)
			return ident, nil
		}

		s.counter.Lock()
		if byteToUInt32(txnResp.Responses[0].GetResponseRange().GetKvs()[0].Value) > s.counter.num {
			s.counter.num = byteToUInt32(txnResp.Responses[0].GetResponseRange().GetKvs()[0].Value)
			s.counter.rev = txnResp.Responses[0].GetResponseRange().GetKvs()[0].ModRevision
		}
		s.counter.Unlock()
	}

}

func (s *NameSpaceMaps) localWrite(str *string, id uint32) {
	s.m.Lock()

	m1 := s.maps.Load().(maps)

	l := uint32(len(m1.idToStr))
	if l <= id {
		l = id + 1
	}

	m2 := maps{make(map[string]uint32, len(m1.strToID)+1), make([]*string, l)}

	for k, v := range m1.strToID {
		m2.strToID[k] = v
	}
	m2.strToID[*str] = id

	copy(m2.idToStr[:len(m1.idToStr)], m1.idToStr)
	m2.idToStr[id] = str

	s.maps.Store(m2)

	s.m.Unlock()
}

// GetNameSpaceValues /
func (s *NameSpaceMaps) getNameSpaceValues() {
	Key := join(s.prefix, uint32ToString(s.nameSpaceID), uint32ToString(typeKEY))
	IDcounter := join(s.prefix, uint32ToString(s.nameSpaceID), uint32ToString(typeNextID))

	newKeyChan := s.etcdClient.Watch(context.Background(), Key, clientv3.WithPrefix())
	newCntChan := s.etcdClient.Watch(context.Background(), IDcounter)

	clientCtx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	resp, err := s.etcdClient.Txn(clientCtx).
		Then(clientv3.OpGet(Key, clientv3.WithPrefix()),
			clientv3.OpGet(IDcounter)).Commit()
	cancel()
	if err != nil {
		log.Println(err)
	}

	r := resp.Responses[0].GetResponseRange()
	mp := maps{make(map[string]uint32, r.Count), make([]*string, r.Count+1)}
	for _, v := range r.Kvs {
		str := string(v.Key[len(Key):])
		id := byteToUInt32(v.Value)
		mp.strToID[str] = id
		mp.idToStr[id] = &str
	}

	r = resp.Responses[1].GetResponseRange()

	s.counter = localCounter{
		num: byteToUInt32(r.GetKvs()[0].Value),
		rev: r.GetKvs()[0].ModRevision,
	}

	s.maps.Store(mp)

	go func() {
		for {
			select {
			case wresp := <-newKeyChan:
				for _, ev := range wresp.Events {
					if ev.Type == mvccpb.PUT {
						id := byteToUInt32(ev.Kv.Value)
						sl := s.maps.Load().(maps)
						if !(uint32(len(sl.idToStr)) > id && sl.idToStr[id] != nil) {
							str := string(ev.Kv.Key[len(Key):])
							s.localWrite(&str, id)
						}
					}
				}

			case <-s.stopKeyChan:
				close(s.stopKeyChan)
				return

			}
		}
	}()

	go func() {
		for {
			select {
			case wresp := <-newCntChan:
				for _, ev := range wresp.Events {
					if ev.Type == mvccpb.PUT {
						s.counter.Lock()
						if byteToUInt32(ev.Kv.Value) > s.counter.num {
							s.counter.num = byteToUInt32(ev.Kv.Value)
							s.counter.rev = ev.Kv.ModRevision
						}
						s.counter.Unlock()
					}
				}

			case <-s.stopCntChan:
				close(s.stopCntChan)
				return

			}
		}
	}()
}

// Close /
func (s *NameSpaceMaps) Close() {
	s.stopCntChan <- struct{}{}
	s.stopKeyChan <- struct{}{}
}

func uint32ToString(num uint32) string {
	buf := &bytes.Buffer{}

	err := binary.Write(buf, binary.BigEndian, num)
	if err != nil {
		log.Fatal(err)
		return ""
	}

	return buf.String()
}

func byteToUInt32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func join(strs ...string) string {
	var sb strings.Builder
	for _, str := range strs {
		sb.WriteString(str)
	}
	return sb.String()
}
