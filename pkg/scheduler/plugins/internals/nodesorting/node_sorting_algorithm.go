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
	"sync"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/plugins/defaults"
	"go.uber.org/zap"
)

const IncrementalNodeSortingAlgorithmName = "incremental"

// NodeSortingAlgorithmFactory is a function that builds a plugin.
type NSAFactory = func(args interface{}) (InternalNodeSortingAlgorithmIf, error)

// Registry is a collection of all available plugins. The framework uses a
// registry to enable and initialize configured plugins.
// All plugins must be in the registry before initializing the framework.
type NSARegistry map[string]NSAFactory

// NewRegistry builds the registry with all plugins.
func NewNSARegistry() NSARegistry {
	return NSARegistry{
		IncrementalNodeSortingAlgorithmName: NewIncrementalNodeSortingAlgorithm,
	}
}

// Register adds a new plugin to the registry. If a plugin with the same name
// exists, it returns an error.
func (r NSARegistry) Register(name string, factory NSAFactory) error {
	if _, ok := r[name]; ok {
		return fmt.Errorf("a plugin named %v already exists", name)
	}
	r[name] = factory
	return nil
}

// Generate plugin by plugin name and arguments
func (r NSARegistry) GenerateNodeSortingAlgorithm(config *NodeSortingAlgorithmConfig) (InternalNodeSortingAlgorithmIf, error) {
	if config == nil {
		return getDefaultNodeSortingAlgorithm(), nil
	}
	if pf, ok := r[config.Name]; ok {
		return pf(config.Args)
	}
	return nil, fmt.Errorf("unregistered plugin: %s", config.Name)
}

type InternalNodeSortingAlgorithmIf interface {
	interfaces.NodeManager
	Init(partition interfaces.Partition, nodeEvaluator NodeEvaluator)
}

type IncrementalNodeSortingAlgorithm struct {
	InternalNodeSortingAlgorithmIf
	partition     interfaces.Partition
	eventChan     chan *NodeEvent
	nodeEvaluator NodeEvaluator
	nodesManager  *IncrementalNodesManager
	//syncSubjectFn func()
	ticker *time.Ticker
	done   chan bool
	sync.RWMutex
}

func NewIncrementalNodeSortingAlgorithm(_ interface{}) (InternalNodeSortingAlgorithmIf, error) {
	return &IncrementalNodeSortingAlgorithm{
		eventChan:    make(chan *NodeEvent, 1024*1024),
		ticker:       time.NewTicker(1 * time.Second),
		done:         make(chan bool),
		nodesManager: NewIncrementalNodesManager(),
	}, nil
}

func (insa *IncrementalNodeSortingAlgorithm) Init(partition interfaces.Partition, nodeEvaluator NodeEvaluator) {
	insa.partition = partition
	insa.nodeEvaluator = nodeEvaluator
}

func (insa *IncrementalNodeSortingAlgorithm) Run() {
	go func() {
		log.Logger().Info("started daemon goroutine in incremental node sorting algorithm",
			zap.String("partition", insa.partition.GetName()))
		for {
			select {
			case <-insa.done:
				insa.ticker.Stop()
				log.Logger().Info("stopped daemon goroutine in incremental node sorting algorithm",
					zap.String("partition", insa.partition.GetName()))
				return
			case <-insa.ticker.C:
				if len(insa.eventChan) > 0 {
					log.Logger().Info("process node events periodically",
						zap.String("partition", insa.partition.GetName()),
						zap.Int("numEvents", len(insa.eventChan)))
					insa.processAllEvents()
				}
			}
		}
	}()
}

func (insa *IncrementalNodeSortingAlgorithm) Stop() {
	insa.done <- true
}

func (insa *IncrementalNodeSortingAlgorithm) processAllEvents() {
	for {
		select {
		case event := <-insa.eventChan:
			node := event.node
			switch event.eventType {
			case Add:
				score, subScores := insa.nodeEvaluator.CalculateStaticScore(node)
				insa.nodesManager.addNode(node, score, subScores)
				log.Logger().Info("added node for incremental NSA",
					zap.String("nodeID", node.GetNodeID()),
					zap.Any("availableResource", node.GetAvailableResource()),
					zap.Int64("score", score),
					zap.Any("subScores", subScores),
					zap.Int("numNodes", insa.nodesManager.numNodes()))
			case Remove:
				insa.nodesManager.removeNode(node.GetNodeID())
				log.Logger().Info("removed node for incremental NSA",
					zap.String("nodeID", node.GetNodeID()),
					zap.Int("numNodes", insa.nodesManager.numNodes()))
			case ResourceUpdate:
				//Profiling.AddCheckpoint("getNodeIterator-1-2")
				score, subScores := insa.nodeEvaluator.CalculateStaticScore(event.node)
				//Profiling.AddCheckpoint("getNodeIterator-1-3")
				insa.nodesManager.updateNode(event.node, score, subScores)
				//Profiling.AddCheckpoint("getNodeIterator-1-4")
				log.Logger().Info("updated node for incremental NSA",
					zap.String("nodeID", node.GetNodeID()),
					zap.Int64("score", score),
					zap.Any("subScores", subScores),
					zap.Int("numNodes", insa.nodesManager.numNodes()),
					zap.Any("allocated", node.GetAllocatedResource()),
					//zap.Any("allocations", v.node.GetAllAllocations()),
				)
			default:
				log.Logger().Error("Not an acceptable event", zap.Any("event", event))
			}
			//Profiling.AddCheckpoint("getNodeIterator-1-5")
		default:
			return
		}
	}
}

func (insa *IncrementalNodeSortingAlgorithm) GetNodeIterator(request interfaces.Request) interfaces.NodeIterator {
	// resync subject events
	//Profiling.AddCheckpoint("getNodeIterator-1")
	//Profiling.AddCheckpoint("getNodeIterator-1-1")
	insa.processAllEvents()
	//Profiling.AddCheckpoint("getNodeIterator-2")
	// resort nodes with non-zero dynamic score
	var nodesManager *IncrementalNodesManager
	validDynamicScorerConfigs := insa.nodeEvaluator.GetValidDynamicScorerConfigs(request)
	//Profiling.AddCheckpoint("getNodeIterator-3")
	if len(validDynamicScorerConfigs) > 0 {
		nodesManager = insa.nodesManager.clone()
		// prepare update nodes
		updateNodes := make([]*NodeSnapshot, 0)
		for _, nodeSnapshot := range nodesManager.nodes {
			score, subScores := insa.nodeEvaluator.CalculateDynamicScore(validDynamicScorerConfigs, nodeSnapshot.node)
			if score != 0 {
				mergedSubScores := make(map[string]int64)
				for subScorer, subScore := range nodeSnapshot.cachedSubScores {
					mergedSubScores[subScorer] = subScore
				}
				for subScorer, subScore := range subScores {
					mergedSubScores[subScorer] = subScore
				}
				updateNodes = append(updateNodes, &NodeSnapshot{
					node:            nodeSnapshot.node,
					cachedScore:     nodeSnapshot.cachedScore + score,
					cachedSubScores: mergedSubScores,
				})
			}
		}
		//TODO: If too many nodes have non-zero dynamic score,
		// maybe we should considering other sorting approaches.
		for _, updatedNodeSnapshot := range updateNodes {
			nodesManager.updateNode(updatedNodeSnapshot.node, updatedNodeSnapshot.cachedScore,
				updatedNodeSnapshot.cachedSubScores)
			log.Logger().Info("updated node for dynamic scorer",
				zap.String("nodeID", updatedNodeSnapshot.node.GetNodeID()),
				zap.Int64("score", updatedNodeSnapshot.cachedScore),
				zap.Any("subScores", updatedNodeSnapshot.cachedSubScores),
			)
		}
		//Profiling.AddCheckpoint("getNodeIterator-4")
	} else {
		nodesManager = insa.nodesManager
	}
	// return iterator of sorted nodes
	//Profiling.AddCheckpoint("getNodeIterator-5")
	sortedNodes := nodesManager.getSortedNodes(insa.partition.GetNodeSortingPolicyType())
	//Profiling.AddCheckpoint("getNodeIterator-6")
	return defaults.NewDefaultNodeIterator(sortedNodes)
}

/*
 * Manager node event
 */
type NodeEventType int

const (
	Add NodeEventType = iota
	Remove
	ResourceUpdate
)

type NodeEvent struct {
	eventType NodeEventType
	node      interfaces.Node
}

func (insa *IncrementalNodeSortingAlgorithm) Add(node interfaces.Node) {
	insa.eventChan <- &NodeEvent{
		eventType: Add,
		node:      node,
	}
}

func (insa *IncrementalNodeSortingAlgorithm) Remove(node interfaces.Node) {
	insa.eventChan <- &NodeEvent{
		eventType: Remove,
		node:      node,
	}
}

func (insa *IncrementalNodeSortingAlgorithm) StateUpdated(_ interfaces.Node) {
	//ignore state update event
}

func (insa *IncrementalNodeSortingAlgorithm) ResourceUpdated(node interfaces.Node) {
	insa.eventChan <- &NodeEvent{
		eventType: ResourceUpdate,
		node:      node,
	}
}

func getDefaultNodeSortingAlgorithm() InternalNodeSortingAlgorithmIf {
	nsa, _ := NewIncrementalNodeSortingAlgorithm(nil)
	return nsa
}
