package objects

import (
	"strconv"
	"strings"
	"testing"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"

	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/plugins"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/plugins/internals"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
)

func setupInternalPlugin() {
	pluginsConfig := &configs.PluginsConfig{
		Plugins: []*configs.PluginConfig{
			{
				Name: internals.InternalAppsRequestsPluginName,
			},
			{
				Name: internals.InternalNodeManagerPluginName,
			},
		},
	}
	plugins.Init(plugins.NewRegistry(), pluginsConfig)
}

func teardownInternalPlugin() {
	plugins.Init(plugins.NewRegistry(), plugins.GetDefaultPluginsConfig())
}

func TestSortAppsNoPendingForInternal(t *testing.T) {
	setupInternalPlugin()
	TestSortAppsNoPending(t)
	teardownInternalPlugin()
}

func TestSortAppsFifoForInternal(t *testing.T) {
	setupInternalPlugin()
	TestSortAppsFifo(t)
	teardownInternalPlugin()
}

func TestSortAppsFairForInternal(t *testing.T) {
	setupInternalPlugin()
	TestSortAppsFair(t)
	teardownInternalPlugin()
}

//func TestSortAppsStateAwareForInternal(t *testing.T) {
//	setupInternalPlugin()
//	TestSortAppsStateAware(t)
//	teardownInternalPlugin()
//}

// list of application and the location of the named applications inside that list
// place[0] defines the location of the app-0 in the list of applications
func assertAppAsksList(t *testing.T, appIt interfaces.AppIterator, expectedAppIDs []int, expectedRequestPriorities []int, name string) {
	assert.Equal(t, len(expectedAppIDs), appIt.Size(), "length of apps is not correct")
	actualAppIDs := make([]int, 0)
	actualRequestPriorities := make([]int, 0)
	for appIt.HasNext() {
		app := appIt.Next()
		appID, err := strconv.Atoi(strings.Trim(app.GetApplicationID(), "app-"))
		assert.NilError(t, err, "failed to parse appID")
		actualAppIDs = append(actualAppIDs, appID)
		reqIt := app.GetRequestsWrapper().SortForAllocation()
		for reqIt.HasNext() {
			req := reqIt.Next()
			actualRequestPriorities = append(actualRequestPriorities, int(req.GetPriority()))
		}
	}
	assert.DeepEqual(t, actualAppIDs, expectedAppIDs)
	assert.DeepEqual(t, actualRequestPriorities, expectedRequestPriorities)
}

func TestSortAppsWithPriority(t *testing.T) {
	setupInternalPlugin()
	// init queue
	leafQueue := createTestQueue(t, queueName)
	// init apps
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newApplication(appID, "partition", queueName)
		leafQueue.AddApplication(app)
		app.SetQueue(leafQueue)
		// add 2 asks with priority P(i) and P(i+1) for app,
		// app-0 has P0 and P1 asks, app-1 has P1 and P2 asks,
		// app-2 has P2 and P3 asks, app-3 has P3 and P4 asks.
		ask1 := newAllocationAsk("ask-P"+strconv.Itoa(i), appID, res)
		ask1.priority = int32(i)
		app.AddAllocationAsk(ask1)
		ask2 := newAllocationAsk("ask-P"+strconv.Itoa(i+1), appID, res)
		ask2.priority = int32(i + 1)
		app.AddAllocationAsk(ask2)
		// add declining allocations for apps
		alloc := newAllocation(appID, "uuid-1", nodeID1, queueName, resources.Multiply(res, int64(4-i)))
		app.AddAllocation(alloc)
	}
	// for FifoSortPolicy,
	// apps/asks should come back in order: app-3/P4, app-2/P3, app-3/P3, app-1/P2, app-2/P2, app-0/P1, app-1/P1, app-0/P0
	leafQueue.sortType = policies.FifoSortPolicy
	appIt := leafQueue.GetApplications().SortForAllocation()
	assertAppAsksList(t, appIt, []int{3, 2, 3, 1, 2, 0, 1, 0}, []int{4, 3, 3, 2, 2, 1, 1, 0}, "priority + fifo simple")
	// for FairSortPolicy,
	// apps/asks should come back in order: app-3/P4, app-3/P3, app-2/P3, app-2/P2, app-1/P2, app-1/P1, app-0/P1, app-0/P0
	leafQueue.sortType = policies.FairSortPolicy
	appIt = leafQueue.GetApplications().SortForAllocation()
	assertAppAsksList(t, appIt, []int{3, 3, 2, 2, 1, 1, 0, 0}, []int{4, 3, 3, 2, 2, 1, 1, 0}, "priority + fifo simple")

	// add P5 ask for app-1
	app1 := leafQueue.getApplication(appID1)
	newAsk := newAllocationAsk("ask-P5", appID1, res)
	newAsk.priority = 5
	err := app1.AddAllocationAsk(newAsk)
	assert.NilError(t, err, "failed to add allocation ask")
	// for FifoSortPolicy,
	// apps/asks should come back in order: app-1/P5, app-3/P4, app-2/P3, app-3/P3, app-1/P2, app-2/P2, app-0/P1, app-1/P1, app-0/P0
	leafQueue.sortType = policies.FifoSortPolicy
	appIt = leafQueue.GetApplications().SortForAllocation()
	assertAppAsksList(t, appIt, []int{1, 3, 2, 3, 1, 2, 0, 1, 0}, []int{5, 4, 3, 3, 2, 2, 1, 1, 0}, "priority + fifo simple")
	// for FairSortPolicy,
	// apps/asks should come back in order: app-1/P5, app-3/P4, app-3/P3, app-2/P3, app-2/P2, app-1/P2, app-1/P1, app-0/P1, app-0/P0
	leafQueue.sortType = policies.FairSortPolicy
	appIt = leafQueue.GetApplications().SortForAllocation()
	assertAppAsksList(t, appIt, []int{1, 3, 3, 2, 2, 1, 1, 0, 0}, []int{5, 4, 3, 3, 2, 2, 1, 1, 0}, "priority + fifo simple")

	// add another P5 ask for app-1
	newAsk = newAllocationAsk("ask-P5-2", appID1, res)
	newAsk.priority = 5
	err = app1.AddAllocationAsk(newAsk)
	assert.NilError(t, err, "failed to add allocation ask")
	// for FifoSortPolicy,
	// apps/asks should come back in order: app-1/P5, app-1/P5-2, app-3/P4, app-2/P3, app-3/P3, app-1/P2, app-2/P2, app-0/P1, app-1/P1, app-0/P0
	leafQueue.sortType = policies.FifoSortPolicy
	appIt = leafQueue.GetApplications().SortForAllocation()
	assertAppAsksList(t, appIt, []int{1, 3, 2, 3, 1, 2, 0, 1, 0}, []int{5, 5, 4, 3, 3, 2, 2, 1, 1, 0}, "priority + fifo simple")
	// for FairSortPolicy,
	// apps/asks should come back in order: app-1/P5, app-1/P5-2, app-3/P4, app-3/P3, app-2/P3, app-2/P2, app-1/P2, app-1/P1, app-0/P1, app-0/P0
	leafQueue.sortType = policies.FairSortPolicy
	appIt = leafQueue.GetApplications().SortForAllocation()
	assertAppAsksList(t, appIt, []int{1, 3, 3, 2, 2, 1, 1, 0, 0}, []int{5, 5, 4, 3, 3, 2, 2, 1, 1, 0}, "priority + fifo simple")

	// remove P3 ask for app-2
	app2 := leafQueue.getApplication(appID2)
	app2.RemoveAllocationAsk("ask-P3")
	// for FifoSortPolicy,
	// apps/asks should come back in order: app-1/P5, app-1/P5-2, app-3/P4, app-3/P3, app-1/P2, app-2/P2, app-0/P1, app-1/P1, app-0/P0
	leafQueue.sortType = policies.FifoSortPolicy
	appIt = leafQueue.GetApplications().SortForAllocation()
	assertAppAsksList(t, appIt, []int{1, 3, 3, 1, 2, 0, 1, 0}, []int{5, 5, 4, 3, 2, 2, 1, 1, 0}, "priority + fifo simple")
	// for FairSortPolicy,
	// apps/asks should come back in order: app-1/P5, app-1/P5-2, app-3/P4, app-3/P3, app-2/P2, app-1/P2, app-1/P1, app-0/P1, app-0/P0
	leafQueue.sortType = policies.FairSortPolicy
	appIt = leafQueue.GetApplications().SortForAllocation()
	assertAppAsksList(t, appIt, []int{1, 3, 3, 2, 1, 1, 0, 0}, []int{5, 5, 4, 3, 2, 2, 1, 1, 0}, "priority + fifo simple")
}

func BenchmarkIteratePendingRequestsForInternal(b *testing.B) {
	setupInternalPlugin()
	BenchmarkIteratePendingRequests(b)
	teardownInternalPlugin()
}

func BenchmarkSortApplicationsForInternal(b *testing.B) {
	setupInternalPlugin()
	BenchmarkSortApplications(b)
	teardownInternalPlugin()
}
