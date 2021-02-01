package defaults

import (
	"strconv"
	"testing"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
	"gotest.tools/assert"
)

func TestSortNodesBin(t *testing.T) {
	// nil or empty list cannot panic
	SortNodes(nil, policies.BinPackingPolicy)
	list := make([]interfaces.Node, 0)
	SortNodes(list, policies.BinPackingPolicy)
	list = append(list, NewFakeNode("node-nil", resources.NewResourceFromMap(nil)))
	SortNodes(list, policies.BinPackingPolicy)

	// stable sort is used so equal resources stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})

	// setup to sort ascending
	list = make([]interfaces.Node, 3)
	for i := 0; i < 3; i++ {
		num := strconv.Itoa(i)
		node := NewFakeNode("node-"+num, resources.Multiply(res, int64(3-i)))
		list[i] = node
	}
	// nodes should come back in order 2 (100), 1 (200), 0 (300)
	SortNodes(list, policies.BinPackingPolicy)
	assertNodeList(t, list, []int{2, 1, 0}, "bin base order")

	// change node-1 on place 1 in the slice to have no res
	list[1] = NewFakeNode("node-1", resources.Multiply(res, 0))
	// nodes should come back in order 1 (0), 2 (100), 0 (300)
	SortNodes(list, policies.BinPackingPolicy)
	assertNodeList(t, list, []int{2, 0, 1}, "bin no res node-1")

	// change node-1 on place 0 in the slice to have 300 res
	list[0] = NewFakeNode("node-1", resources.Multiply(res, 3))
	// nodes should come back in order 2 (100), 1 (300), 0 (300)
	SortNodes(list, policies.BinPackingPolicy)
	assertNodeList(t, list, []int{2, 1, 0}, "bin node-1 same as node-0")

	// change node-0 on place 2 in the slice to have -300 res
	list[2] = NewFakeNode("node-0", resources.Multiply(res, -3))
	// nodes should come back in order 0 (-300), 2 (100), 1 (300)
	SortNodes(list, policies.BinPackingPolicy)
	assertNodeList(t, list, []int{0, 2, 1}, "bin node-0 negative")
}

func TestSortNodesFair(t *testing.T) {
	// nil or empty list cannot panic
	SortNodes(nil, policies.FairnessPolicy)
	list := make([]interfaces.Node, 0)
	SortNodes(list, policies.FairnessPolicy)
	list = append(list, NewFakeNode("node-nil", resources.NewResourceFromMap(nil)))
	SortNodes(list, policies.FairnessPolicy)

	// stable sort is used so equal resources stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup to sort descending
	list = make([]interfaces.Node, 3)
	for i := 0; i < 3; i++ {
		num := strconv.Itoa(i)
		node := NewFakeNode("node-"+num, resources.Multiply(res, int64(1+i)))
		list[i] = node
	}
	// nodes should come back in order 2 (300), 1 (200), 0 (100)
	SortNodes(list, policies.FairnessPolicy)
	assertNodeList(t, list, []int{2, 1, 0}, "fair base order")

	// change node-1 on place 1 in the slice to have no res
	list[1] = NewFakeNode("node-1", resources.Multiply(res, 0))
	// nodes should come back in order 2 (300), 0 (100), 1 (0)
	SortNodes(list, policies.FairnessPolicy)
	assertNodeList(t, list, []int{1, 2, 0}, "fair no res node-1")

	// change node-1 on place 2 in the slice to have 300 res
	list[2] = NewFakeNode("node-1", resources.Multiply(res, 3))
	// nodes should come back in order 2 (300), 1 (300), 0 (100)
	SortNodes(list, policies.FairnessPolicy)
	assertNodeList(t, list, []int{2, 1, 0}, "fair node-1 same as node-2")

	// change node-2 on place 0 in the slice to have -300 res
	list[0] = NewFakeNode("node-2", resources.Multiply(res, -3))
	// nodes should come back in order 1 (300), 0 (100), 2 (-300)
	SortNodes(list, policies.FairnessPolicy)
	assertNodeList(t, list, []int{1, 0, 2}, "fair node-2 negative")
}

// list of nodes and the location of the named nodes inside that list
// place[0] defines the location of the node-0 in the list of nodes
func assertNodeList(t *testing.T, list []interfaces.Node, place []int, name string) {
	assert.Equal(t, "node-0", list[place[0]].GetNodeID(), "test name: %s", name)
	assert.Equal(t, "node-1", list[place[1]].GetNodeID(), "test name: %s", name)
	assert.Equal(t, "node-2", list[place[2]].GetNodeID(), "test name: %s", name)
}
