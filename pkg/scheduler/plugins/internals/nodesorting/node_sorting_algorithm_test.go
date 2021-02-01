/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package nodesorting

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"

	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/plugins/defaults"

	"gopkg.in/yaml.v2"

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
)

//func createTestPartitionContext(t *testing.T, nodeEvaluator *NodeEvaluatorConfig,
//	nodeSortingAlgorithm *NodeSortingAlgorithmConfig) interfaces.Partition {
//	rootSched, err := createRootQueue(nil)
//	assert.NilError(t, err, "failed to create root queue")
//	info := &cache.PartitionInfo{
//		Name: "default",
//		Root: rootSched.QueueInfo,
//		RmID: "test",
//	}
//	if nodeEvaluator != nil {
//		info.SetNodeEvaluator(nodeEvaluator)
//	}
//	if nodeSortingAlgorithm != nil {
//		info.SetNodeSortingAlgorithm(nodeSortingAlgorithm)
//	}
//	partitionCtx := newPartitionSchedulingContext(info, rootSched)
//	return partitionCtx
//}

func initTestNodes(nodeManager interfaces.NodeManager, numNodes int,
	totalRes *resources.Resource, setAllocatedResFunc func(i int) *resources.Resource) {
	for i := 0; i < numNodes; i++ {
		nodeID := "node-" + strconv.Itoa(i+1)
		//partitionCtx.addSchedulingNode(cache.NewNodeForTest(nodeID, totalRes))
		//schedulingNode := partitionCtx.getSchedulingNode(nodeID)
		//schedulingNode.allocated = setAllocatedResFunc(i)
		//schedulingNode.nodeResourceUpdatedWithLock()
		// notify algorithm immediately
		//partitionCtx.subjectManager.NotifyEvent(&NodeSubjectResourceUpdateEvent{node: schedulingNode})
		node := defaults.NewFakeNode(nodeID, totalRes)
		node.SetAllocatedResource(setAllocatedResFunc(i))
		nodeManager.Add(node)
	}
}

func testNodeSortingAlgorithm(t *testing.T, nodeSortingAlgorithm InternalNodeSortingAlgorithmIf,
	numNodes int, totalRes *resources.Resource, setAllocatedResFunc func(i int) *resources.Resource,
	request interfaces.Request, expectedSortedNodeIDs []string) {
	// init scheduling nodes
	initTestNodes(nodeSortingAlgorithm, numNodes, totalRes, setAllocatedResFunc)
	// verify nodes are sorted as expected
	time.Sleep(time.Millisecond)
	nodeIt := nodeSortingAlgorithm.GetNodeIterator(request)
	sortedNodeIDs := make([]string, 0)
	for nodeIt.HasNext() {
		node := nodeIt.Next().(*defaults.FakeNode)
		sortedNodeIDs = append(sortedNodeIDs, node.GetNodeID())
	}
	assert.DeepEqual(t, sortedNodeIDs, expectedSortedNodeIDs)
}

func getSuccessiveTestNodeIDs(startIndex, endIndex int) []string {
	nodeIDs := make([]string, 0)
	if startIndex >= endIndex {
		for i := startIndex; i >= endIndex; i-- {
			nodeIDs = append(nodeIDs, "node-"+strconv.Itoa(i))
		}
	} else {
		for i := startIndex; i <= endIndex; i++ {
			nodeIDs = append(nodeIDs, "node-"+strconv.Itoa(i))
		}
	}
	return nodeIDs
}

type TestCase struct {
	description           string
	nodeManagerPluginArgs string
	sortingPolicy         policies.SortingPolicy
	numNodes              int
	totalRes              *resources.Resource
	setAllocatedResFunc   func(i int) *resources.Resource
	request               interfaces.Request
	expectedSortedNodeIDs []string
}

//func getTestAlgorithmConfigs(t *testing.T) []*NodeSortingAlgorithmConfig {
//	return []*NodeSortingAlgorithmConfig{
//		{
//			Name: IncrementalNodeSortingAlgorithmName,
//		},
//	}
//}

func getTestName(testCase *TestCase, nodeManagerConfig *NodeManagerConfig) string {
	var nodeScorerNames string
	if nodeManagerConfig != nil {
		for _, sc := range nodeManagerConfig.NodeEvaluator.ScorerConfigs {
			if nodeScorerNames != "" {
				nodeScorerNames += ","
			}
			nodeScorerNames += sc.Name
		}
	}
	return fmt.Sprintf("Algorithm:%s/Policy:%s/Scorers:%s/Desc:%s",
		nodeManagerConfig.NodeSortingAlgorithm.Name, testCase.sortingPolicy, nodeScorerNames, testCase.description)
}

func parseNodeManagerConfig(nodeManagerConfigStr string) *NodeManagerConfig {
	nodeManagerConfig := &NodeManagerConfig{}
	yaml.Unmarshal([]byte(nodeManagerConfigStr), &nodeManagerConfig)
	return nodeManagerConfig
}

func testNodeSortingAlgorithms(t *testing.T, testCases []*TestCase) {
	nsaRegistry := NewNSARegistry()
	for _, testCase := range testCases {
		partition := defaults.NewFakePartition("test", testCase.sortingPolicy)
		nodeManagerConfig := parseNodeManagerConfig(testCase.nodeManagerPluginArgs)
		nodeSortingAlgorithm, err := nsaRegistry.GenerateNodeSortingAlgorithm(nodeManagerConfig.NodeSortingAlgorithm)
		assert.NilError(t, err, "failed to generate node sorting algorithm")
		nodeEvaluator, err := NewDefaultNodeEvaluator(nodeManagerConfig.NodeEvaluator)
		assert.NilError(t, err, "failed to generate node evaluator")
		nodeSortingAlgorithm.Init(partition, nodeEvaluator)
		testName := getTestName(testCase, nodeManagerConfig)
		t.Run(testName, func(t *testing.T) {
			testNodeSortingAlgorithm(t, nodeSortingAlgorithm,
				testCase.numNodes, testCase.totalRes, testCase.setAllocatedResFunc,
				testCase.request, testCase.expectedSortedNodeIDs)
		})
	}
}

func TestAlgorithmWithDominantResourceUsageScorer(t *testing.T) {
	totalRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 1000, "vcores": 1000})
	nodeManagerConfigStr := `
nodesortingalgorithm:
  name: incremental
nodeevaluator:
  scorerconfigs:
    - name: dominant_resource_usage
      weight: 1
      args:
        resourcenames:
        - memory
        - vcores
`
	testCases := []*TestCase{
		/**
		 * Fair policy
		 */
		// allocated memory is increasing as nodeID grows,
		// nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		{
			description:           "allocated memory is increasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.FairnessPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
		// allocated memory is decreasing as nodeID grows,
		// nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		{
			description:           "allocated memory is decreasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.FairnessPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
		// allocated vcore is increasing as nodeID grows,
		// nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		{
			description:           "allocated vcore is increasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.FairnessPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"vcores": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
		// allocated vcore is decreasing as nodeID grows,
		// nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		{
			description:           "allocated vcore is decreasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.FairnessPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"vcores": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
		/**
		 * BinPacking policy
		 */
		// allocated memory is increasing as nodeID grows,
		// nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		{
			description:           "allocated memory is increasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.BinPackingPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
		// allocated memory is decreasing as nodeID grows,
		// nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		{
			description:           "allocated memory is decreasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.BinPackingPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
		// allocated vcore is increasing as nodeID grows,
		// nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		{
			description:           "allocated vcore is increasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.BinPackingPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"vcores": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
		// allocated vcore is decreasing as nodeID grows,
		// nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		{
			description:           "allocated vcore is decreasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.BinPackingPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"vcores": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
	}
	testNodeSortingAlgorithms(t, testCases)
}

func TestAlgorithmWithSingleResourceUsageScorer(t *testing.T) {
	totalRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 1000, "vcores": 1000})
	nodeManagerConfigStrForMemory := `
nodesortingalgorithm:
  name: incremental
nodeevaluator:
  scorerconfigs:
    - name: single_resource_usage
      weight: 1
      args:
        resourcename: memory
`
	nodeManagerConfigStrForVcores := `
nodesortingalgorithm:
  name: incremental
nodeevaluator:
  scorerconfigs:
    - name: single_resource_usage
      weight: 1
      args:
        resourcename: vcores
`
	//memoryResourceUsageScorerConfig := &NodeScorerConfig{
	//	ScorerName: ResourceUsageScorerName,
	//	Weight:     1,
	//	Conf:       map[string]interface{}{"resourceName": "memory"},
	//}
	//vcoreResourceUsageScorerConfig := &NodeScorerConfig{
	//	ScorerName: ResourceUsageScorerName,
	//	Weight:     1,
	//	Conf:       map[string]interface{}{"resourceName": "vcores"},
	//}
	testCases := []*TestCase{
		/**
		 * Fairness policy
		 */
		// allocated memory is increasing as nodeID grows,
		// nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		{
			description:           "allocated memory is increasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStrForMemory,
			sortingPolicy:         policies.FairnessPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
		// allocated memory is decreasing as nodeID grows,
		// nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		{
			description:           "allocated memory is decreasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStrForMemory,
			sortingPolicy:         policies.FairnessPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
		// allocated vcore is increasing as nodeID grows,
		// nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		{
			description:           "allocated vcores is increasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStrForVcores,
			sortingPolicy:         policies.FairnessPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"vcores": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
		// allocated vcore is decreasing as nodeID grows,
		// nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		{
			description:           "allocated vcore is decreasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStrForVcores,
			sortingPolicy:         policies.FairnessPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"vcores": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
		/**
		 * BinPacking policy
		 */
		// allocated memory is increasing as nodeID grows,
		// nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		{
			description:           "allocated memory is increasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStrForMemory,
			sortingPolicy:         policies.BinPackingPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
		// allocated memory is decreasing as nodeID grows,
		// nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		{
			description:           "allocated memory is decreasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStrForMemory,
			sortingPolicy:         policies.BinPackingPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
		// allocated vcore is increasing as nodeID grows,
		// nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		{
			description:           "allocated vcores is increasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStrForVcores,
			sortingPolicy:         policies.BinPackingPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"vcores": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
		// allocated vcore is decreasing as nodeID grows,
		// nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		{
			description:           "allocated vcore is decreasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStrForVcores,
			sortingPolicy:         policies.BinPackingPolicy,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"vcores": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
	}
	testNodeSortingAlgorithms(t, testCases)
}

func TestAlgorithmWithScarceResourceUsageScorer(t *testing.T) {
	totalRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 1000, "vcores": 1000, "gpu": 1000})
	gpuRequest := defaults.NewFakeRequest("alloc-1", 1, 1,
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1, "vcores": 1, "gpu": 1}))
	nodeManagerConfigStr := `
nodesortingalgorithm:
  name: incremental
nodeevaluator:
  scorerconfigs:
    - name: scarce_resource_usage
      weight: 1
      args:
        scarceresourcename: gpu
`
	testCases := []*TestCase{
		/**
		 * Fairness policy
		 */
		// allocated gpu is increasing as nodeID grows,
		// request gpu resource then nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		{
			description:           "allocated memory is increasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.FairnessPolicy,
			request:               gpuRequest,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"gpu": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
		// allocated gpu is decreasing as nodeID grows,
		// request gpu resource then nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		{
			description:           "allocated memory is decreasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.FairnessPolicy,
			request:               gpuRequest,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"gpu": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
		/**
		 * BinPacking policy
		 */
		// allocated gpu is increasing as nodeID grows,
		// request gpu resource then nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		{
			description:           "allocated memory is increasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.BinPackingPolicy,
			request:               gpuRequest,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"gpu": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
		// allocated gpu is decreasing as nodeID grows,
		// request gpu resource then nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		{
			description:           "allocated memory is decreasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.BinPackingPolicy,
			request:               gpuRequest,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"gpu": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
	}
	testNodeSortingAlgorithms(t, testCases)
}

func TestAlgorithmWithMixedResourceUsageScorers(t *testing.T) {
	totalRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 1000, "vcores": 1000, "gpu": 1000})
	gpuRequest := defaults.NewFakeRequest("alloc-1", 1, 1,
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1, "vcores": 1, "gpu": 1}))
	nodeManagerConfigStr := `
nodesortingalgorithm:
  name: incremental
nodeevaluator:
  scorerconfigs:
    - name: dominant_resource_usage
      weight: 1
      args:
        resourcenames:
        - memory
        - vcores
    - name: scarce_resource_usage
      weight: 3
      args:
        scarceresourcename: gpu
`
	testCases := []*TestCase{
		/*
		 * Fairness policy
		 */
		// allocated memory is increasing and allocated gpu is decreasing as nodeID grows,
		// request gpu resource then nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		// since weight of scarce_resource_scorer is greater than that of dominant_resource_scorer
		{
			description:           "allocated memory is increasing and allocated gpu is decreasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.FairnessPolicy,
			request:               gpuRequest,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(i), "gpu": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
		// allocated memory is decreasing and allocated gpu is increasing as nodeID grows,
		// request gpu resource then nodes should be sorted in increasing order: [node-1, node-2, ..., node-10]
		// since weight of scarce_resource_scorer is greater than that of dominant_resource_scorer
		{
			description:           "allocated memory is decreasing and allocated gpu is increasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.FairnessPolicy,
			request:               gpuRequest,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(1000 - i), "gpu": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
		// allocated memory is decreasing as nodeID grows,
		// the latter half of nodes have available gpu resource which is increasing as nodeID grows,
		// request gpu resource then nodes should be sorted like this:
		// [node-6, node-7, ..., node-10, node-5, node-4, ... node-1]
		// since weight of scarce_resource_scorer is greater than that of dominant_resource_scorer
		{
			description: "allocated memory is decreasing, the latter half of nodes have available gpu resource " +
				"which is increasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.FairnessPolicy,
			request:               gpuRequest,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				if i < 5 {
					return resources.NewResourceFromMap(
						map[string]resources.Quantity{"memory": resources.Quantity(1000 - i), "gpu": resources.Quantity(1000)})
				}
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(1000 - i), "gpu": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: append(getSuccessiveTestNodeIDs(6, 10),
				getSuccessiveTestNodeIDs(5, 1)...),
		},
		// allocated memory is increasing as nodeID grows,
		// the latter half of nodes have available gpu resource which is decreasing as nodeID grows,
		// request gpu resource then nodes should be sorted like this:
		// [node-10, node-9, ..., node-6, node-1, node-2, ... node-5]
		// since weight of scarce_resource_scorer is greater than that of dominant_resource_scorer
		{
			description: "allocated memory is increasing, the latter half of nodes have available gpu resource " +
				"which is decreasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.FairnessPolicy,
			request:               gpuRequest,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				if i < 5 {
					return resources.NewResourceFromMap(
						map[string]resources.Quantity{"memory": resources.Quantity(i), "gpu": resources.Quantity(1000)})
				}
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(i), "gpu": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: append(getSuccessiveTestNodeIDs(10, 6),
				getSuccessiveTestNodeIDs(1, 5)...),
		},

		/*
		 * BinPacking policy
		 */
		// allocated memory is increasing and allocated gpu is decreasing as nodeID grows,
		// request gpu resource then nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		// since weight of scarce_resource_scorer is greater than that of dominant_resource_scorer
		{
			description:           "allocated memory is increasing and allocated gpu is decreasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.BinPackingPolicy,
			request:               gpuRequest,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(i), "gpu": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
		// allocated memory is decreasing and allocated gpu is increasing as nodeID grows,
		// request gpu resource then nodes should be sorted in decreasing order: [node-10, node-9, ..., node-1]
		// since weight of scarce_resource_scorer is greater than that of dominant_resource_scorer
		{
			description:           "allocated memory is decreasing and allocated gpu is increasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.BinPackingPolicy,
			request:               gpuRequest,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(1000 - i), "gpu": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
		// allocated memory is decreasing as nodeID grows,
		// the latter half of nodes have available gpu resource which is increasing as nodeID grows,
		// request gpu resource then nodes should be sorted like this:
		// [node-6, node-7, ..., node-10, node-5, node-4, ... node-1]
		// since weight of scarce_resource_scorer is greater than that of dominant_resource_scorer
		{
			description: "allocated memory is decreasing, the latter half of nodes have allocated gpu resource " +
				"which is increasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.BinPackingPolicy,
			request:               gpuRequest,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				if i < 5 {
					return resources.NewResourceFromMap(
						map[string]resources.Quantity{"memory": resources.Quantity(1000 - i), "gpu": resources.Quantity(1000)})
				}
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(1000 - i), "gpu": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: append(getSuccessiveTestNodeIDs(1, 5),
				getSuccessiveTestNodeIDs(10, 6)...),
		},
		// allocated memory is increasing as nodeID grows,
		// the latter half of nodes have available gpu resource which is decreasing as nodeID grows,
		// request gpu resource then nodes should be sorted like this:
		// [node-10, node-9, ..., node-6, node-1, node-2, ... node-5]
		// since weight of scarce_resource_scorer is greater than that of dominant_resource_scorer
		{
			description: "allocated memory is increasing, the latter half of nodes have allocated gpu resource " +
				"which is decreasing as nodeID grows",
			nodeManagerPluginArgs: nodeManagerConfigStr,
			sortingPolicy:         policies.BinPackingPolicy,
			request:               gpuRequest,
			numNodes:              10,
			totalRes:              totalRes,
			setAllocatedResFunc: func(i int) *resources.Resource {
				if i < 5 {
					return resources.NewResourceFromMap(
						map[string]resources.Quantity{"memory": resources.Quantity(i), "gpu": resources.Quantity(1000)})
				}
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(i), "gpu": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: append(getSuccessiveTestNodeIDs(5, 1),
				getSuccessiveTestNodeIDs(6, 10)...),
		},
	}
	testNodeSortingAlgorithms(t, testCases)
}
