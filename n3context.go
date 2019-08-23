package n3context

import (
	"fmt"
	"time"

	crdt "github.com/nsip/n3-crdt"
	deep6 "github.com/nsip/n3-deep6"
)

type N3Context struct {
	Name   string
	UserId string
	db     *deep6.Deep6DB
	crdtm  *crdt.CRDTManager
}

func NewN3Context(userId string, contextName string) (*N3Context, error) {

	// create the d6 database
	contextPath := fmt.Sprintf("./contexts/%s/%s/d6", userId, contextName)
	d6db, err := deep6.OpenFromFile(contextPath)
	if err != nil {
		return nil, err
	}
	d6db.AuditLevel = "none"

	// create the crdt handler
	crdtm, err := crdt.NewCRDTManager(userId, contextName)
	if err != nil {
		return nil, err
	}
	crdtm.AuditLevel = "none"

	// return the built context
	return &N3Context{
		Name:   contextName,
		UserId: userId,
		db:     d6db,
		crdtm:  crdtm,
	}, nil

}

//
// connects the crdtm to the streaming service
// and pipes received data into the d6 db
//
func (n3c *N3Context) Activate() error {

	defer timeTrack(time.Now(), "context Activate()")

	iterator, err := n3c.crdtm.StartReceiver()
	if err != nil {
		return err
	}

	// go func() {
	// err = n3c.db.IngestFromJSONChannel(iterator)
	// if err != nil {
	// 	log.Println("crdtm-db error:", err)
	// }
	// }()

	// count := 0
	// var buf bytes.Buffer
	// for jsonBytes := range iterator {
	// 	count++
	// 	log.Println("recieived: ", count)
	// 	buf.Write([]byte(`[`))
	// 	buf.Write(jsonBytes)
	// 	buf.Write([]byte(`]`))
	// 	err = n3c.db.IngestFromReader(&buf)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	if count == 35200 {
	// 		break
	// 	}
	// }

	// return nil
	go func() {
		n3c.db.IngestFromJSONChannel(iterator)
	}()

	// return n3c.db.IngestFromJSONChannel(iterator)
	return nil

}

//
// shut down the context cleanly
//
func (n3c *N3Context) Close() {
	n3c.crdtm.Close()
	// time.Sleep(time.Second * 3)
	n3c.db.Close()
}
