// contextmanager.go

package n3context

import (
	"sync"
	"time"

	"github.com/pkg/errors"
)

type N3ContextKey struct {
	Name, UserId string
}

type N3ContextManager struct {
	// need to persist list of contexts/users
	// rebuild on reboot.
	contexts map[N3ContextKey]*N3Context
	sync.RWMutex
}

//
// Creates a new manager to which n3contexts can be added/removed.
//
func NewN3ContextManager() *N3ContextManager {

	return &N3ContextManager{
		contexts: make(map[N3ContextKey]*N3Context, 0),
	}

}

//
// Creates a new context and adds it to the manager.
// If successful the new context is returned, otherwise error.
//
// Contexts require a name, and an owning userid.
// If the context matching that name/userid already exists it is
// returned.
//
func (n3cm *N3ContextManager) AddContext(userId string, contextName string) (*N3Context, error) {

	defer timeTrack(time.Now(), "AddContext()")

	n3cm.Lock()
	defer n3cm.Unlock()

	// check if already exists
	key := N3ContextKey{Name: contextName, UserId: userId}
	n3ctx, ok := n3cm.contexts[key]
	if ok {
		return n3ctx, nil
	}

	// create new context
	n3ctx, err := NewN3Context(userId, contextName)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create n3context:")
	}

	// add to the manager
	n3cm.contexts[key] = n3ctx

	return n3ctx, nil
}

// Load to reinstate - from config area

// Save to persist - to config area, also on close.
