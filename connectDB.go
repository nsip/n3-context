// connectDB.go

package n3context

import (
	"context"

	deep6 "github.com/nsip/n3-deep6"
	"github.com/pkg/errors"
)

//
// connects stream iterator from crdtm to datastore
//
func connectDB(ctx context.Context, db *deep6.Deep6DB, iterator <-chan []byte) (<-chan error, error) {

	errc := make(chan error, 1)

	go func() {
		defer close(errc)

		err := db.IngestFromJSONChannel(iterator)
		if err != nil {
			errc <- errors.Wrap(err, "error attaching db to crdt stream:")
			return
		}
	}()

	return errc, nil
}
