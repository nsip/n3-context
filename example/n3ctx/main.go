// main.go

package main

import (
	"log"
	"time"

	n3context "github.com/nsip/n3-context"
)

func main() {

	cm := n3context.NewN3ContextManager()
	c, err := cm.AddContext("mattf101", "context1")
	if err != nil {
		log.Fatal(err)
	}

	err = c.Activate()
	if err != nil {
		log.Fatal(err)
	}

	// send in some data, via the crdt layer

	// consume data for a time
	// time.Sleep(time.Minute)
	time.Sleep(time.Second * 10)

	// run a query

	c.Close()

}
