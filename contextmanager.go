// contextmanager.go

package n3context

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
)

//
// location to save & restore context information
//
var contextsFile = "./contexts/contexts.json"

type N3ContextKey struct {
	Name, UserId string
}

type N3ContextManager struct {
	contexts map[N3ContextKey]*N3Context
	sync.RWMutex
}

//
// Creates a new manager to which n3contexts can be added/removed.
//
func NewN3ContextManager() *N3ContextManager {

	newCm := &N3ContextManager{
		contexts: make(map[N3ContextKey]*N3Context, 0),
	}

	return newCm
}

//
// Loads details of contexts from file, and activates the
// specified contexts
//
func (n3cm *N3ContextManager) Restore() error {

	defer timeTrack(time.Now(), "Restore()")

	fname := contextsFile
	ctxList, err := loadContexts(fname)
	if err != nil {
		errString := "cannot parse contexts from file: " + fname
		return errors.Wrap(err, errString)
	}

	for _, k := range ctxList {
		_, err := n3cm.AddContext(k.UserId, k.Name)
		if err != nil {
			errString := "cannot create context " + k.Name
			return errors.Wrap(err, errString)
		}
	}

	return n3cm.ActivateAll()
}

//
// ActivateAll
// runs through all loaded contexts and activates them
// so they are listening for updates
//
func (n3cm *N3ContextManager) ActivateAll() error {
	n3cm.Lock()
	defer n3cm.Unlock()

	for k, c := range n3cm.contexts {
		err := c.Activate()
		if err != nil {
			return errors.Wrap(err, "unable to activate context: "+k.Name)
		}
		log.Println("context: ", k, " activated.")
	}

	return nil
}

//
// loads the list of saved contexts from file
//
func loadContexts(fname string) ([]N3ContextKey, error) {

	if !fileExists(fname) {
		return nil, errors.New("expected contexts file does not exist: " + fname)
	}
	f, err := os.Open(fname)
	if err != nil {
		return nil, errors.Wrap(err, "cannot open contexts file: "+fname)
	}

	dec := json.NewDecoder(f)
	// read open bracket
	t, err := dec.Token()
	if err != nil {
		return nil, errors.Wrap(err, "unexpected start token, should be json array [ is: "+t.(string))
	}

	// while the array contains values
	ctxList := make([]N3ContextKey, 0)
	for dec.More() {
		var nk N3ContextKey
		err := dec.Decode(&nk)
		if err != nil {
			return nil, errors.Wrap(err, "cannot decode json token: ")
		}
		ctxList = append(ctxList, nk)
	}
	// read closing bracket
	t, err = dec.Token()
	if err != nil {
		return nil, errors.Wrap(err, "unexpected end token, should be json array: ")
	}

	return ctxList, nil
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

//
// retrieve a context from the manager
//
func (n3cm *N3ContextManager) GetContext(userId string, contextName string) (*N3Context, error) {

	n3cm.Lock()
	defer n3cm.Unlock()

	key := N3ContextKey{Name: contextName, UserId: userId}
	n3ctx, ok := n3cm.contexts[key]
	if ok {
		return n3ctx, nil
	}
	return nil, errors.New("could not find existing context for " + key.Name + ":" + key.UserId)

}

//
// shuts down all active contexts
//
// will save context info to file for use with Restore()
// if persist is true
//
func (n3cm *N3ContextManager) Close(persist bool) error {
	for _, c := range n3cm.contexts {
		c.Close()
	}
	if persist {
		return persistContexts(n3cm.contexts)
	}
	return nil
}

//
// saves details of active contexts so can be
// re-activated on each startup
//
func persistContexts(c map[N3ContextKey]*N3Context) error {

	fname := contextsFile
	f, err := os.Create(fname)
	if err != nil {
		return errors.Wrap(err, "Cannot persist context info: ")
	}
	defer f.Close()

	ctxList := make([]N3ContextKey, 0)
	for k, _ := range c { // we only need the keys
		ctxList = append(ctxList, k)
	}
	enc := json.NewEncoder(f)
	err = enc.Encode(ctxList)
	if err != nil {
		return errors.Wrap(err, "Cannot encode context list: ")
	}

	return nil
}

//
// check whether file is already there
//
func fileExists(fname string) bool {

	f, err := os.Open(fname)
	if err != nil {
		return false
	}
	defer f.Close()
	log.Println("found existing config file: ", fname)
	return true
}
