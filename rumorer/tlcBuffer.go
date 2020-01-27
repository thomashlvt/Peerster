package rumorer

import (
	"container/heap"
	"fmt"
	. "github.com/thomashlvt/Peerster/constants"
	. "github.com/thomashlvt/Peerster/utils"
	"sync"
)


/********************************************************************************/
/** Priority Queue implementation from PriorityQueue example of container/heap **/
/********************************************************************************/

// An Item is something we manage in a priority queue.
type Item struct {
	value *TLCMessage   // The value of the item; arbitrary.
	id int    // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the lowest id
	return pq[i].id < pq[j].id
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value *TLCMessage, id int) {
	item.value = value
	item.id = id
	heap.Fix(pq, item.index)
}

/********************************************************************************/


type tlcBuffer struct {
	*sync.RWMutex
	buffers map[string] PriorityQueue
}


func NewTLCBuffer() *tlcBuffer {
	return &tlcBuffer {
		RWMutex: &sync.RWMutex{},
		buffers: make(map[string]PriorityQueue) ,
	}
}


func (b *tlcBuffer) Add(msg *TLCMessage) {
	b.Lock()
	defer b.Unlock()

	pq, exists := b.buffers[msg.Origin]
	if !exists {
		b.buffers[msg.Origin] = make(PriorityQueue, 0)
	}

	if Debug {
		fmt.Printf("[DEBUG] Saving TLCMessage from %v for %v to buffer\n", msg.Origin, msg.TxBlock.Transaction.Name)
	}
	heap.Push(&pq, &Item{value: msg, id: int(msg.ID)})
	b.buffers[msg.Origin] = pq

}


func (b *tlcBuffer) Update(state *State) []*TLCMessage {
	b.Lock()
	defer b.Unlock()

	res := make([]*TLCMessage, 0)

	for origin, pq := range b.buffers {
		for {
			// Get the element with the lowest ID from the buffer
			if len(pq) == 0 {
				// Emptied queue
				break
			}
			next := heap.Pop(&pq).(*Item)

			tlc := next.value

			// Check if the vector clock is satisfied
			_, youHave := state.Compare(tlc.VectorClock)
			if youHave == nil {
				// Vector clock is satisfied: element is now removed
				// from buffer, and return it, so that it can be passed
				// to the ConfirmationRumorer
				res = append(res, tlc)
				if Debug {
					fmt.Printf("[DEBUG] Popping TLCMessage from %v for %v from buffer\n", tlc.Origin, tlc.TxBlock.Transaction.Name)
				}
			} else {
				// If this vector clock is not satisfied
				// the others also won't be (because higher ID)
				// So push it back on the heap and go to the next pq
				if Debug {
					fmt.Printf("[DEBUG] TLCMessage from %v for %v still has youHave: %v\n", tlc.Origin, tlc.TxBlock.Transaction.Name, *youHave)
				}
				heap.Push(&pq, next)
				break
			}
		}
		b.buffers[origin] = pq
	}

	return res
}