// connectGQL.go

package n3context

import (
	"context"

	n3gql "github.com/nsip/n3-gql"
	"github.com/pkg/errors"
)

//
// connects stream iterator from crdtm to gql schema-builder
//
func connectGQL(ctx context.Context, gqlm *n3gql.GQLManager, iterator <-chan []byte) (<-chan error, error) {

	errc := make(chan error, 1)

	go func() {
		defer close(errc)

		err := gqlm.BuildSchemaFromJSONChannel(iterator)
		if err != nil {
			errc <- errors.Wrap(err, "error attaching gql-builder to crdt stream:")
			return
		}
	}()

	return errc, nil
}
