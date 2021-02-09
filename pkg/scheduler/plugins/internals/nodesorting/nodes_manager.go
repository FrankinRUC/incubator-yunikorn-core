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
	"strings"
	"sync"

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
	"github.com/google/btree"
	"go.uber.org/zap"
)

// Snapshot of scheduling node used for sorting nodes based on btree,
// cachedScore is involved in the comparison between btree items,
// which should be unchangeable so that sorter can keep and manage a stable btree.
type NodeSnapshot struct {
	node            interfaces.Node
	cachedScore     int64
	cachedSubScores map[string]int64
}

// Comparing cachedAvailableResource and nodeId (to make sure all nodes are independent items in btree)
func (ns *NodeSnapshot) Less(than btree.Item) bool {
	if ns.cachedScore != than.(*NodeSnapshot).cachedScore {
		return ns.cachedScore < than.(*NodeSnapshot).cachedScore
	}
	return strings.Compare(ns.node.GetNodeID(), than.(*NodeSnapshot).node.GetNodeID()) < 0
}

type IncrementalNodesManager struct {
	nodes    map[string]*NodeSnapshot
	nodeTree *btree.BTree
	sync.RWMutex
}

func NewIncrementalNodesManager() *IncrementalNodesManager {
	return &IncrementalNodesManager{
		nodes:    make(map[string]*NodeSnapshot),
		nodeTree: btree.New(32),
	}
}

func (inm *IncrementalNodesManager) clone() *IncrementalNodesManager {
	inm.RLock()
	defer inm.RUnlock()
	newNodes := map[string]*NodeSnapshot{}
	for nodeID, ns := range inm.nodes {
		newNodes[nodeID] = ns
	}
	return &IncrementalNodesManager{
		nodes:    newNodes,
		nodeTree: inm.nodeTree.Clone(),
	}
}

func (inm *IncrementalNodesManager) addNode(node interfaces.Node, score int64, subScores map[string]int64) {
	inm.Lock()
	defer inm.Unlock()
	// remove existed node at first
	if nodeSnapshot, ok := inm.nodes[node.GetNodeID()]; ok {
		inm.nodeTree.Delete(nodeSnapshot)
	}
	// add node
	nodeSnapshot := &NodeSnapshot{
		node:            node,
		cachedScore:     score,
		cachedSubScores: subScores,
	}
	inm.nodeTree.ReplaceOrInsert(nodeSnapshot)
	inm.nodes[node.GetNodeID()] = nodeSnapshot
}

func (inm *IncrementalNodesManager) removeNode(nodeID string) *NodeSnapshot {
	inm.Lock()
	defer inm.Unlock()
	if nodeSnapshot, ok := inm.nodes[nodeID]; ok {
		inm.nodeTree.Delete(nodeSnapshot)
		delete(inm.nodes, nodeID)
		return nodeSnapshot
	}
	return nil
}

func (inm *IncrementalNodesManager) updateNode(node interfaces.Node, score int64, subScores map[string]int64) {
	inm.Lock()
	defer inm.Unlock()
	if nodeSnapshot, ok := inm.nodes[node.GetNodeID()]; ok {
		if score == nodeSnapshot.cachedScore {
			log.Logger().Info("skip updating node since the score is not changed",
				zap.String("nodeID", node.GetNodeID()), zap.Int64("score", score))
			return
		}
		// remove node first, then add it again
		inm.nodeTree.Delete(nodeSnapshot)
		nodeSnapshot := &NodeSnapshot{
			node:            node,
			cachedScore:     score,
			cachedSubScores: subScores,
		}
		inm.nodeTree.ReplaceOrInsert(nodeSnapshot)
		inm.nodes[node.GetNodeID()] = nodeSnapshot
	} else {
		log.Logger().Info("skip updating non-exist node for incremental nodes manager",
			zap.String("nodeID", node.GetNodeID()))
	}
}

func (inm *IncrementalNodesManager) getSortedNodes(policy policies.SortingPolicy) []interfaces.Node {
	inm.RLock()
	defer inm.RUnlock()
	sortedNodes := make([]interfaces.Node, inm.nodeTree.Len())
	var i = 0
	switch policy {
	case policies.FairnessPolicy:
		inm.nodeTree.Descend(func(item btree.Item) bool {
			node := item.(*NodeSnapshot).node
			if !node.IsSchedulable() {
				return true
			}
			sortedNodes[i] = node
			i++
			return true
		})
	case policies.BinPackingPolicy:
		inm.nodeTree.Ascend(func(item btree.Item) bool {
			node := item.(*NodeSnapshot).node
			if !node.IsSchedulable() {
				return true
			}
			sortedNodes[i] = node
			i++
			return true
		})
	}
	return sortedNodes[:i]
}

func (inm *IncrementalNodesManager) numNodes() int {
	inm.Lock()
	defer inm.Unlock()
	return inm.nodeTree.Len()
}
