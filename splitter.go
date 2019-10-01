// splitter.go

package n3context

import (
	"context"
)

//
// splitter creates multiple (fan-out) feeds from the
// same stream reader output channel - the crdtm receiver typically
//
func streamSplitter(ctx context.Context, in <-chan []byte) (
	<-chan []byte, <-chan []byte, <-chan error, error) {
	out1 := make(chan []byte)
	out2 := make(chan []byte)
	errc := make(chan error, 1)
	go func() {
		defer close(out1)
		defer close(out2)
		defer close(errc)
		for n := range in {
			// Send the data to the output channel 1 but return early
			// if the context has been cancelled.
			select {
			case out1 <- n:
				// fmt.Printf("published msg to chan1:\n%s\n\n", n)
			case <-ctx.Done():
				return
			}

			// Send the data to the output channel 2 but return early
			// if the context has been cancelled.
			select {
			case out2 <- n:
				// fmt.Printf("published msg to chan2:\n%s\n\n", n)
			case <-ctx.Done():
				return
			}
		}
	}()
	return out1, out2, errc, nil
}
