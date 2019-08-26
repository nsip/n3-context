package n3context

import (
	"fmt"
	"io"
	"log"
	"net/http"
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

	iterator, err := n3c.crdtm.StartReceiver()
	if err != nil {
		return err
	}

	go func() {
		n3c.db.IngestFromJSONChannel(iterator)
	}()

	return nil

}

//
// send data into context via crdt manager from a file
// expects payload to be array of json objects.
//
func (n3c *N3Context) PublishFromFile(fname string) error {
	defer timeTrack(time.Now(), "PublishFromFile() "+fname)
	return n3c.crdtm.SendFromFile(fname)
}

//
// send data into context via crdt manager from an http request
// expects payload to be array of json objects.
//
func (n3c *N3Context) PublishFromHTTPRequest(r *http.Request) error {
	defer timeTrack(time.Now(), "PublishFromHTTPRequest() ")
	return n3c.crdtm.SendFromHTTPRequest(r)
}

//
// send data into the context, passes through
// the crdt layer and into storage if the context is
// activated, any reader can be used
// but expects content to be array of json objects.
//
func (n3c *N3Context) Publish(r io.Reader) error {
	defer timeTrack(time.Now(), "PublishFromReader() ")
	return n3c.crdtm.SendFromReader(r)
}

//
// shut down the context cleanly
//
func (n3c *N3Context) Close() {
	n3c.crdtm.Close()
	n3c.db.Close()
	log.Println("Context: " + n3c.Name + ":" + n3c.UserId + " closed.")
}
