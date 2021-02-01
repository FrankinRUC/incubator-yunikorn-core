package defaults

import (
	"strconv"
	"testing"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
)

type FakeRequest struct {
	interfaces.Request
	allocationKey     string
	pendingAskRepeat  int32
	priority          int32
	createTime        time.Time
	allocatedResource *resources.Resource
}

func NewFakeRequest(allocationKey string, pendingAskRepeat int32, priority int32,
	allocatedResource *resources.Resource) *FakeRequest {
	return &FakeRequest{
		allocationKey:     allocationKey,
		pendingAskRepeat:  pendingAskRepeat,
		priority:          priority,
		createTime:        time.Now(),
		allocatedResource: allocatedResource,
	}
}

func (fr *FakeRequest) GetAllocationKey() string {
	return fr.allocationKey
}

func (fr *FakeRequest) GetPendingAskRepeat() int32 {
	return fr.pendingAskRepeat
}

func (fr *FakeRequest) GetPriority() int32 {
	return fr.priority
}

func (fr *FakeRequest) GetCreateTime() time.Time {
	return fr.createTime
}

func (fr *FakeRequest) GetAllocatedResource() *resources.Resource {
	return fr.allocatedResource
}

// Test adding/updating/removing requests with different priorities,
// to verify the correctness of all/pending priority groups and requests.
func TestRequests(t *testing.T, requests interfaces.Requests) {
	// ignore nil request
	requests.AddOrUpdateRequest(nil)
	assert.Equal(t, requests.Size(), 0)

	// add request-1 with priority 1
	reqKey1 := "req-1"
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0})
	req1 := NewFakeRequest(reqKey1, 1, 1, res)
	requests.AddOrUpdateRequest(req1)
	checkRequests(t, requests, []string{reqKey1}, []string{reqKey1})

	// update request-1 to non-pending state
	req1.pendingAskRepeat = 0
	requests.AddOrUpdateRequest(req1)
	checkRequests(t, requests, []string{reqKey1}, []string{})

	// update request-1 to pending state
	req1.pendingAskRepeat = 2
	requests.AddOrUpdateRequest(req1)
	checkRequests(t, requests, []string{reqKey1}, []string{reqKey1})

	// add request-2 with priority 1
	reqKey2 := "req-2"
	req2 := NewFakeRequest(reqKey2, 1, 1, res)
	requests.AddOrUpdateRequest(req2)
	checkRequests(t, requests, []string{reqKey1, reqKey2}, []string{reqKey1, reqKey2})

	// update request-1 to non-pending state
	req2.pendingAskRepeat = 0
	requests.AddOrUpdateRequest(req2)
	checkRequests(t, requests, []string{reqKey1, reqKey2}, []string{reqKey1})

	// add request-3 with priority 1
	reqKey3 := "req-3"
	req3 := NewFakeRequest(reqKey3, 1, 1, res)
	requests.AddOrUpdateRequest(req3)
	checkRequests(t, requests, []string{reqKey1, reqKey2, reqKey3}, []string{reqKey1, reqKey3})

	// add request-4 with priority 3
	reqKey4 := "req-4"
	req4 := NewFakeRequest(reqKey4, 1, 3, res)
	requests.AddOrUpdateRequest(req4)
	checkRequests(t, requests, []string{reqKey4, reqKey1, reqKey2, reqKey3}, []string{reqKey4, reqKey1, reqKey3})

	// update request-4 to non-pending state
	req4.pendingAskRepeat = 0
	requests.AddOrUpdateRequest(req4)
	checkRequests(t, requests, []string{reqKey4, reqKey1, reqKey2, reqKey3}, []string{reqKey1, reqKey3})

	// update request-4 to pending state
	req4.pendingAskRepeat = 2
	requests.AddOrUpdateRequest(req4)
	checkRequests(t, requests, []string{reqKey4, reqKey1, reqKey2, reqKey3}, []string{reqKey4, reqKey1, reqKey3})

	// add request-5 with priority 2
	reqKey5 := "req-5"
	req5 := NewFakeRequest(reqKey5, 1, 2, res)
	requests.AddOrUpdateRequest(req5)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3},
		[]string{reqKey4, reqKey5, reqKey1, reqKey3})

	// add request-0 with priority 1 and create time is less than request-1
	// it should be placed before request-1
	reqKey0 := "req-0"
	req0 := NewFakeRequest(reqKey0, 1, 1, res)
	req0.createTime = req1.createTime.Add(-1 * time.Millisecond)
	requests.AddOrUpdateRequest(req0)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey0, reqKey1, reqKey2, reqKey3},
		[]string{reqKey4, reqKey5, reqKey0, reqKey1, reqKey3})

	// remove request-0
	requests.RemoveRequest(reqKey0)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3},
		[]string{reqKey4, reqKey5, reqKey1, reqKey3})

	// update request-5 to non-pending state
	req5.pendingAskRepeat = 0
	requests.AddOrUpdateRequest(req5)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3},
		[]string{reqKey4, reqKey1, reqKey3})

	// update request-1 to non-pending state
	req1.pendingAskRepeat = 0
	requests.AddOrUpdateRequest(req1)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3}, []string{reqKey4, reqKey3})

	// update request-3 to non-pending state
	req3.pendingAskRepeat = 0
	requests.AddOrUpdateRequest(req3)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3}, []string{reqKey4})

	// update request-4 to non-pending state
	req4.pendingAskRepeat = 0
	requests.AddOrUpdateRequest(req4)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3}, []string{})

	// update request-5 to pending state
	req5.pendingAskRepeat = 1
	requests.AddOrUpdateRequest(req5)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3}, []string{reqKey5})

	// update request-4 to pending state
	req4.pendingAskRepeat = 1
	requests.AddOrUpdateRequest(req4)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3}, []string{reqKey4, reqKey5})

	// update request-3 to pending state
	req3.pendingAskRepeat = 1
	requests.AddOrUpdateRequest(req3)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3}, []string{reqKey4, reqKey5, reqKey3})

	// update request-2 to pending state
	req2.pendingAskRepeat = 1
	requests.AddOrUpdateRequest(req2)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3},
		[]string{reqKey4, reqKey5, reqKey2, reqKey3})

	// update request-1 to pending state
	req1.pendingAskRepeat = 1
	requests.AddOrUpdateRequest(req1)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3},
		[]string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3})

	// remove request-4
	requests.RemoveRequest(reqKey4)
	checkRequests(t, requests, []string{reqKey5, reqKey1, reqKey2, reqKey3}, []string{reqKey5, reqKey1, reqKey2, reqKey3})

	// remove request-3
	requests.RemoveRequest(reqKey3)
	checkRequests(t, requests, []string{reqKey5, reqKey1, reqKey2}, []string{reqKey5, reqKey1, reqKey2})

	// remove request-5
	requests.RemoveRequest(reqKey5)
	checkRequests(t, requests, []string{reqKey1, reqKey2}, []string{reqKey1, reqKey2})

	// remove request-2
	requests.RemoveRequest(reqKey2)
	checkRequests(t, requests, []string{reqKey1}, []string{reqKey1})

	// remove request-1
	requests.RemoveRequest(reqKey1)
	checkRequests(t, requests, []string{}, []string{})

	// add request-1/request-2/request-4/request-5
	requests.AddOrUpdateRequest(req1)
	requests.AddOrUpdateRequest(req2)
	requests.AddOrUpdateRequest(req4)
	requests.AddOrUpdateRequest(req5)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2}, []string{reqKey4, reqKey5, reqKey1, reqKey2})

	// reset
	requests.Reset()
	checkRequests(t, requests, []string{}, []string{})
}

// Check the correctness of all/pending requests
func checkRequests(t *testing.T, requests interfaces.Requests, expectedRequestKeys []string,
	expectedPendingRequestKeys []string) {
	// check length of all requests
	t.Logf("Check sorted requests: expectedRequestKeys=%v", expectedRequestKeys)
	assert.Equal(t, requests.Size(), len(expectedRequestKeys),
		"length of requests differs: expected=%v, actual=%v",
		len(expectedRequestKeys), requests.Size())
	// check length of pending requests
	reqIt := requests.SortForAllocation()
	assert.Equal(t, reqIt.Size(), len(expectedPendingRequestKeys),
		"length of pending requests differs: expected=%v, actual=%v",
		len(expectedPendingRequestKeys), reqIt.Size())

	reqIndex := 0
	for reqIt.HasNext() {
		assert.Equal(t, reqIt.Next().GetAllocationKey(), expectedPendingRequestKeys[reqIndex],
			"length of pending requests differs: expected=%v, actual=%v",
			expectedPendingRequestKeys[reqIndex])
		reqIndex++
	}
}

type FakePartition struct {
	interfaces.Partition
	Name   string
	Nodes  []interfaces.Node
	policy policies.SortingPolicy
}

func NewFakePartition(name string, policy policies.SortingPolicy) *FakePartition {
	return &FakePartition{
		Name:   name,
		Nodes:  make([]interfaces.Node, 0),
		policy: policy,
	}
}

func (fp *FakePartition) AddNode(node interfaces.Node) {
	fp.Nodes = append(fp.Nodes, node)
}

func (fp *FakePartition) GetName() string {
	return fp.Name
}

func (fp *FakePartition) GetNodes() []interfaces.Node {
	nodes := make([]interfaces.Node, len(fp.Nodes))
	for idx, n := range fp.Nodes {
		nodes[idx] = n
	}
	return nodes
}

func (fp *FakePartition) GetSchedulableNodes() []interfaces.Node {
	nodes := make([]interfaces.Node, 0)
	for _, n := range fp.Nodes {
		if n.IsSchedulable() {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

func (fp *FakePartition) GetNodeSortingPolicyType() policies.SortingPolicy {
	return fp.policy
}

type FakeNode struct {
	interfaces.Node
	nodeID    string
	capacity  *resources.Resource
	allocated *resources.Resource
	occupied  *resources.Resource
}

func (fn *FakeNode) GetNodeID() string {
	return fn.nodeID
}

func (fn *FakeNode) GetAvailableResource() *resources.Resource {
	return resources.SubEliminateNegative(fn.capacity, fn.allocated)
}

func (fn *FakeNode) GetAllocatedResource() *resources.Resource {
	return fn.allocated
}

func (fn *FakeNode) GetCapacity() *resources.Resource {
	return fn.capacity
}

func (fn *FakeNode) GetOccupiedResource() *resources.Resource {
	return fn.occupied
}

func (fn *FakeNode) GetReservations() []string {
	return nil
}

func (fn *FakeNode) IsSchedulable() bool {
	return true
}

func (fn *FakeNode) SetAvailableResource(available *resources.Resource) {
	fn.allocated = resources.SubEliminateNegative(fn.capacity, available)
}

func (fn *FakeNode) SetAllocatedResource(allocated *resources.Resource) {
	fn.allocated = allocated
}

func NewFakeNode(nodeID string, capacity *resources.Resource) *FakeNode {
	return &FakeNode{
		nodeID:    nodeID,
		capacity:  capacity,
		allocated: resources.Zero,
		occupied:  resources.Zero,
	}
}

// A list of nodes that can be iterated over.
func newSchedNodeList(number int) []interfaces.Node {
	list := make([]interfaces.Node, number)
	for i := 0; i < number; i++ {
		num := strconv.Itoa(i)
		node := NewFakeNode("node-"+num, resources.NewResource())
		list[i] = node
	}
	return list
}
