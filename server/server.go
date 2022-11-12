package server

import (
	"context"
	"errors"
	"github.com/robaho/leveldb"
	pb "github.com/robaho/leveldbr/internal/proto"
	"log"
	"path/filepath"
	"sync"
)

type openDatabase struct {
	refcount int
	db       *leveldb.Database
	fullpath string
}

type connstate struct {
	db        *openDatabase
	itrs      map[uint64]leveldb.LookupIterator
	snapshots map[uint64]*leveldb.Snapshot
	next      uint64 // next iterator id
}

type Server struct {
	sync.Mutex
	pb.UnimplementedLeveldbServer
	path   string
	opendb map[string]*openDatabase
}

func NewServer(dbpath string) *Server {
	s := Server{path: dbpath, opendb: make(map[string]*openDatabase)}
	return &s
}

func (s *Server) Remove(ctx context.Context, in *pb.RemoveRequest) (*pb.RemoveReply, error) {
	s.Lock()
	defer s.Unlock()

	log.Println("remove database", in)

	fullpath := filepath.Join(s.path, in.GetDbname())

	err := leveldb.Remove(fullpath)
	errs := ""
	if err != nil {
		errs = err.Error()
	}

	reply := &pb.RemoveReply{Error: errs}

	return reply, nil
}

func (s *Server) Connection(conn pb.Leveldb_ConnectionServer) error {

	state := connstate{itrs: make(map[uint64]leveldb.LookupIterator), snapshots: make(map[uint64]*leveldb.Snapshot)}

	defer s.closedb(&state)

	for {
		msg, err := conn.Recv()

		if err != nil {
			return err
		}

		switch msg.Request.(type) {
		case *pb.InMessage_Open:
			err = s.open(conn, &state, msg.GetOpen())
		case *pb.InMessage_Close:
			err = s.closedb(&state)
			reply := &pb.OutMessage_Close{Close: &pb.CloseReply{Error: toErrS(err)}}
			err = conn.Send(&pb.OutMessage{Reply: reply})
		case *pb.InMessage_Get:
			err = s.get(conn, &state, msg.GetGet())
		case *pb.InMessage_Put:
			err = s.put(conn, &state, msg.GetPut())
		case *pb.InMessage_Write:
			err = s.write(conn, &state, msg.GetWrite())
		case *pb.InMessage_Lookup:
			err = s.lookup(conn, &state, msg.GetLookup())
		case *pb.InMessage_Next:
			err = s.lookupNext(conn, &state, msg.GetNext())
		case *pb.InMessage_Snapshot:
			err = s.snapshot(conn, &state, msg.GetSnapshot())
		}

		if err != nil {
			return err
		}
	}
}

func toErrS(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// clean up database references
func (s *Server) closedb(state *connstate) error {
	s.Lock()
	defer s.Unlock()

	if state.db == nil {
		return nil // already closed or never opened
	}

	fullpath := state.db.fullpath
	state.db = nil

	opendb, ok := s.opendb[fullpath]
	if !ok || opendb.refcount == 0 {
		return errors.New("database is not open")
	}

	log.Println("closing database", fullpath)
	opendb.refcount--
	if opendb.refcount == 0 {
		err := opendb.db.Close()
		delete(s.opendb, fullpath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) open(conn pb.Leveldb_ConnectionServer, state *connstate, in *pb.OpenRequest) error {
	s.Lock()
	defer s.Unlock()

	log.Println("open database", in)

	fullpath := filepath.Join(s.path, in.GetDbname())

	opendb, ok := s.opendb[fullpath]
	if !ok {
		db, err := leveldb.Open(fullpath, leveldb.Options{CreateIfNeeded: in.Create})

		if err != nil {
			return err
		}

		opendb = &openDatabase{refcount: 1, db: db, fullpath: fullpath}
		s.opendb[fullpath] = opendb
	} else {
		opendb.refcount++
		log.Println("database already open, returning ref", in)
	}

	state.db = opendb

	reply := &pb.OutMessage_Open{Open: &pb.OpenReply{Error: ""}}
	return conn.Send(&pb.OutMessage{Reply: reply})
}

func (s *Server) get(conn pb.Leveldb_ConnectionServer, state *connstate, in *pb.GetRequest) error {

	var err error
	var value []byte

	if in.GetSnapshot() == 0 {
		value, err = state.db.db.Get(in.Key)
		reply := &pb.OutMessage_Get{Get: &pb.GetReply{Value: value, Error: toErrS(err)}}
		return conn.Send(&pb.OutMessage{Reply: reply})
	} else {
		snapshot, ok := state.snapshots[in.GetSnapshot()]
		if !ok {
			err = errors.New("invalid snapshot id")
		} else {
			value, err = snapshot.Get(in.Key)
		}
		reply := &pb.OutMessage_Get{Get: &pb.GetReply{Value: value, Error: toErrS(err)}}
		return conn.Send(&pb.OutMessage{Reply: reply})
	}
}

func (s *Server) put(conn pb.Leveldb_ConnectionServer, state *connstate, in *pb.PutRequest) error {

	err := state.db.db.Put(in.Key, in.Value)

	reply := &pb.OutMessage_Put{Put: &pb.PutReply{Error: toErrS(err)}}
	return conn.Send(&pb.OutMessage{Reply: reply})
}

func (s *Server) write(conn pb.Leveldb_ConnectionServer, state *connstate, in *pb.WriteRequest) error {

	wb := leveldb.WriteBatch{}

	for _, e := range in.Entries {
		wb.Put(e.Key, e.Value)
	}
	err := state.db.db.Write(wb)

	reply := &pb.OutMessage_Write{Write: &pb.WriteReply{Error: toErrS(err)}}
	return conn.Send(&pb.OutMessage{Reply: reply})
}

func (s *Server) lookup(conn pb.Leveldb_ConnectionServer, state *connstate, in *pb.LookupRequest) error {

	var err error
	var id uint64

	var itr leveldb.LookupIterator
	var err0 error

	if in.GetSnapshot() == 0 {
		itr, err0 = state.db.db.Lookup(in.Lower, in.Upper)
	} else {
		snapshot, ok := state.snapshots[in.GetSnapshot()]
		if !ok {
			err0 = errors.New("invalid snapshot id")
		} else {
			itr, err0 = snapshot.Lookup(in.Lower, in.Upper)
		}
	}

	if err0 == nil {
		state.next++
		id = state.next
		state.itrs[id] = itr
	}
	err = err0

	reply := &pb.OutMessage_Lookup{Lookup: &pb.LookupReply{Id: id, Error: toErrS(err)}}
	return conn.Send(&pb.OutMessage{Reply: reply})
}

func (s *Server) lookupNext(conn pb.Leveldb_ConnectionServer, state *connstate, in *pb.LookupNextRequest) error {

	var err error
	var entries []*pb.KeyValue

	itr, ok := state.itrs[in.Id]
	if !ok {
		err = errors.New("invalid iterator id")
	} else {
		// read up to 64 entries
		count := 0

		entries = make([]*pb.KeyValue, 64)[:0]
		for count < 64 {
			key, value, err0 := itr.Next()
			err = err0
			if err == nil {
				kv := pb.KeyValue{Key: key, Value: value}
				entries = append(entries, &kv)
			} else {
				if count > 0 {
					err = nil
					break
				}
				delete(state.itrs, in.Id)
				break
			}
			count++
		}
	}

	reply := &pb.OutMessage_Next{Next: &pb.LookupNextReply{Entries: entries, Error: toErrS(err)}}
	return conn.Send(&pb.OutMessage{Reply: reply})
}

func (s *Server) snapshot(conn pb.Leveldb_ConnectionServer, state *connstate, in *pb.SnapshotRequest) error {

	var err error
	var id uint64

	snapshot, err0 := state.db.db.Snapshot()
	if err0 == nil {
		state.next++
		id = state.next
		state.snapshots[id] = snapshot
	}
	err = err0

	reply := &pb.OutMessage_Snapshot{Snapshot: &pb.SnapshotReply{Id: id, Error: toErrS(err)}}
	return conn.Send(&pb.OutMessage{Reply: reply})
}
