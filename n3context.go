package n3context

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/pkg/errors"

	crdt "github.com/nsip/n3-crdt"
	deep6 "github.com/nsip/n3-deep6"
	n3gql "github.com/nsip/n3-gql"
	graphql "github.com/playlyfe/go-graphql"
)

type N3Context struct {
	Name     string
	UserId   string
	db       *deep6.Deep6DB
	crdtm    *crdt.CRDTManager
	gqlm     *n3gql.GQLManager
	executor *graphql.Executor
	quitChan chan struct{}
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

	// create the gql schema builder
	gqlm := n3gql.NewGQLManager(userId, contextName, d6db)

	// return the built context
	return &N3Context{
		Name:     contextName,
		UserId:   userId,
		db:       d6db,
		crdtm:    crdtm,
		gqlm:     gqlm,
		quitChan: make(chan struct{}),
	}, nil

}

//
// connects the crdtm to the streaming service
// and pipes received data into the d6 db, and
// gql schema builder
//
func (n3c *N3Context) Activate() error {

	// periodically attach the db to the stream, store
	// messages while they are being produced
	// will time out when reciever closes iterator when no new messages
	// received, currently waits for 2 seconds of inactivity
	go func() {
		var wg sync.WaitGroup
		for {
			// log.Println("activating context: ", n3c.UserId, n3c.Name)
			wg.Add(1)
			go func() {
				err := runActivation(n3c.db, n3c.crdtm, n3c.gqlm)
				if err != nil {
					log.Println("error n3context.Activate():", err)
				}
				wg.Done()
				return
			}()
			wg.Wait()

			// log.Println("\t ...restarting connection in 2 seconds...")
			runtime.GC()                // force mem reclaim
			time.Sleep(2 * time.Second) // wait before reconnecting

			// listen for n3context shutdown
			select {
			case <-n3c.quitChan:
				// log.Println("closing worker goroutine")
				return
			default:
			}

		}

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
	err := n3c.crdtm.SendFromHTTPRequest(r)
	if err != nil {
		return errors.Wrap(err, "(n3context.PublishFromHTTPRequest) unknown:")
	}
	return err;

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
// simplified version of query engine for demo
//
// TODO: remove in favour of GQLQuery
//
func (n3c *N3Context) Query(startid string,
	traversal deep6.Traversal,
	filter deep6.FilterSpec) (map[string][]map[string]interface{}, error) {
	return n3c.db.TraversalWithId(startid, traversal, filter)

}

//
// passes the received query into the gql manager for resolution
//
func (n3c *N3Context) GQLQuery(query string, vars map[string]interface{}) (map[string]interface{}, error) {

	return n3c.gqlm.Query(query, vars)
}

//
// shut down the context cleanly
//
func (n3c *N3Context) Close() {
	close(n3c.quitChan)
	time.Sleep(time.Second * 5) // give time for db to drain
	n3c.crdtm.Close()
	n3c.db.Close()
	n3c.gqlm.Close()
	log.Println("Context: " + n3c.Name + ":" + n3c.UserId + " closed.")
}
