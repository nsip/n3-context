// runActivation.go

package n3context

import (
	"context"

	crdt "github.com/nsip/n3-crdt"
	deep6 "github.com/nsip/n3-deep6"
	n3gql "github.com/nsip/n3-gql"
)

//
// connects to crdt stream, feeds data into db and gql schema builder
//
func runActivation(db *deep6.Deep6DB, crdtm *crdt.CRDTManager, gqlm *n3gql.GQLManager) error {

	// create context to manage pipeline
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// start the stream listener for this context
	iterator, err := crdtm.StartReceiver()
	if err != nil {
		return err
	}

	// error channels to monitor pipeline
	var errcList []<-chan error

	// create a splitter for the stream
	streamIterator1, streamIterator2, errc, err := streamSplitter(ctx, iterator)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	// create the db sink stage
	errc, err = connectDB(ctx, db, streamIterator1)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	// create the gql sink stage
	errc, err = connectGQL(ctx, gqlm, streamIterator2)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	return WaitForPipeline(errcList...)

}
