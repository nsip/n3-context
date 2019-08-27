// main.go

package main

import (
	"log"
	"time"

	n3context "github.com/nsip/n3-context"
)

func main() {

	// dataFile := "./sample_data/xapi/xapi.json"
	dataFile := "./sample_data/sif/sif.json"

	// create a new manager
	cm1 := n3context.NewN3ContextManager()

	// add a context
	c1, err := cm1.AddContext("mattf101", "context1")
	if err != nil {
		log.Fatal(err)
	}
	// attach the context to the streaming server
	// to recieve updates
	err = c1.Activate()
	if err != nil {
		log.Fatal(err)
	}
	// send in some data, via the crdt layer
	err = c1.PublishFromFile(dataFile)
	if err != nil {
		log.Fatal("PublishFromFile() Error: ", err)
	}

	// add another context
	c2, err := cm1.AddContext("mattf202", "context1")
	if err != nil {
		log.Fatal(err)
	}
	// attache the context to the streaming server
	// to recieve updates
	err = c2.Activate()
	if err != nil {
		log.Fatal(err)
	}
	// send in some data, via the crdt layer
	err = c2.PublishFromFile(dataFile)
	if err != nil {
		log.Fatal("PublishFromFile() Error: ", err)
	}

	// consume data for a time
	// time.Sleep(time.Minute)
	log.Println("...listening for updates")
	time.Sleep(time.Second * 10)

	// shut down the contexts, but persist details
	log.Println("Closing created contexts, and saving...")
	err = cm1.Close(true)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("...CM1 closed")

	// create a new manager & load saved contexts
	log.Println("Restoring saved contexts, and activating")
	cm2 := n3context.NewN3ContextManager()
	err = cm2.Restore()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("...fetch context from manager")
	c3, err := cm2.GetContext("mattf202", "context1")
	if err != nil {
		log.Fatal(err)
	}
	// send in some data, via the crdt layer
	err = c3.PublishFromFile(dataFile)
	if err != nil {
		log.Fatal("PublishFromFile() Error: ", err)
	}

	// consume data for a time
	// time.Sleep(time.Minute)
	log.Println("...listening for updates")
	time.Sleep(time.Second * 10)

	log.Println("Closing created contexts, and saving...")
	err = cm2.Close(true)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("...CM2 closed")

}
