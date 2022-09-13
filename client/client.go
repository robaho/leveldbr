package client

import (
	"context"
	"errors"
	pb "github.com/robaho/leveldbr/internal/proto"
	"google.golang.org/grpc"
	"time"
)

type RemoteDatabase struct {
	dbid    int32
	client  pb.LeveldbClient
	timeout time.Duration
	stream  pb.Leveldb_ConnectionClient
}

type RemoteTransaction struct {
	txid uint64
	db   *RemoteDatabase
}

type RemoteIterator struct {
	id      uint64
	db      *RemoteDatabase
	entries []*pb.KeyValue
	index   int
}

func Open(addr string, dbname string, createIfNeeded bool, timeout int) (*RemoteDatabase, error) {
	// Set up a connection to the server.
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	client := pb.NewLeveldbClient(conn)

	//timeoutSecs := time.Second * time.Duration(timeout)

	ctx := context.Background()
	stream, err := client.Connection(ctx)

	if err != nil {
		conn.Close()
		return nil, err
	}

	db := &RemoteDatabase{client: client, stream: stream}

	request := &pb.InMessage_Open{Open: &pb.OpenRequest{Dbname: dbname, Create: createIfNeeded}}

	err = stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return nil, err
	}

	msg, err := db.stream.Recv()
	if err != nil {
		return nil, err
	}

	response := msg.GetOpen()

	if response.Error != "" {
		return nil, errors.New(response.Error)
	}

	return db, nil
}

func (db *RemoteDatabase) Close() error {

	request := &pb.InMessage_Close{Close: &pb.CloseRequest{}}

	err := db.stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return err
	}

	msg, err := db.stream.Recv()
	if err != nil {
		return err
	}

	response := msg.GetClose()

	if response.Error != "" {
		return errors.New(response.Error)
	}

	db.stream.CloseSend()

	return nil
}

func Remove(addr string, dbname string, timeout int) error {
	// Set up a connection to the server.
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}

	defer conn.Close()

	client := pb.NewLeveldbClient(conn)

	//timeoutSecs := time.Second * time.Duration(timeout)

	ctx := context.Background()

	request := &pb.RemoveRequest{}
	request.Dbname = dbname

	response, err := client.Remove(ctx, request)

	if err != nil {
		return err
	}

	if response.Error != "" {
		return errors.New(response.Error)
	}
	return nil
}

func (db *RemoteDatabase) Get(key []byte) ([]byte, error) {
	request := &pb.InMessage_Get{Get: &pb.GetRequest{Key: key}}

	err := db.stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return nil, err
	}

	msg, err := db.stream.Recv()
	if err != nil {
		return nil, err
	}

	response := msg.GetGet()

	if response.Error != "" {
		return nil, errors.New(response.Error)
	}

	return response.Value, nil
}

func (db *RemoteDatabase) Put(key []byte, value []byte) error {
	request := &pb.InMessage_Put{Put: &pb.PutRequest{Key: key, Value: value}}

	err := db.stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return err
	}
	msg, err := db.stream.Recv()
	if err != nil {
		return err
	}

	response := msg.GetPut()

	if response.Error != "" {
		return errors.New(response.Error)
	}

	return nil
}

func (db *RemoteDatabase) Write(wb WriteBatch) error {

	r := pb.WriteRequest{}

	for _, e := range wb.entries {
		r.Entries = append(r.Entries, &pb.KeyValue{Key: e.key, Value: e.value})
	}

	request := &pb.InMessage_Write{Write: &r}

	err := db.stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return err
	}
	msg, err := db.stream.Recv()
	if err != nil {
		return err
	}

	response := msg.GetWrite()

	if response.Error != "" {
		return errors.New(response.Error)
	}

	return nil
}

func (db *RemoteDatabase) Lookup(lower []byte, upper []byte) (*RemoteIterator, error) {
	request := &pb.InMessage_Lookup{Lookup: &pb.LookupRequest{Lower: lower, Upper: upper}}

	err := db.stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return nil, err
	}

	msg, err := db.stream.Recv()
	if err != nil {
		return nil, err
	}

	response := msg.GetLookup()

	if response.Error != "" {
		return nil, errors.New(response.Error)
	}

	ri := RemoteIterator{db: db, id: response.Id}

	return &ri, nil
}

func (itr *RemoteIterator) Next() (key []byte, value []byte, err error) {
	if itr.index < len(itr.entries) {
		key, value = itr.entries[itr.index].Key, itr.entries[itr.index].Value
		itr.index++
		return
	}

	request := &pb.InMessage_Next{Next: &pb.LookupNextRequest{Id: itr.id}}

	err = itr.db.stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return nil, nil, err
	}

	msg, err := itr.db.stream.Recv()
	if err != nil {
		return nil, nil, err
	}

	response := msg.GetNext()

	if response.Error != "" {
		return nil, nil, errors.New(response.Error)
	}

	itr.entries = response.Entries
	key, value = itr.entries[0].Key, itr.entries[0].Value
	itr.index = 1

	return
}

type keyValue struct {
	key   []byte
	value []byte
}

type WriteBatch struct {
	entries []keyValue
}

func (wb *WriteBatch) Put(key []byte, value []byte) {
	wb.entries = append(wb.entries, keyValue{key: key, value: value})
}

func (wb *WriteBatch) Remove(key []byte) {
	wb.entries = append(wb.entries, keyValue{key: key})
}
