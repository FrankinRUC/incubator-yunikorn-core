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

package defaults

import (
	"sort"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
)

const (
	DefaultNodeManagerPluginName = "DefaultNodeManagerPlugin"
)

func NewDefaultNodeManagerPlugin(_ interface{}) (interfaces.Plugin, error) {
	return &DefaultNodeManagerPlugin{}, nil
}

// This is the default implementation of Node
type DefaultNodeManagerPlugin struct {
}

func (dap *DefaultNodeManagerPlugin) Name() string {
	return DefaultNodeManagerPluginName
}

func (dap *DefaultNodeManagerPlugin) NewNodeManager(partition interfaces.Partition,
	_ interface{}) interfaces.NodeManager {
	return &DefaultNodeManager{
		partition: partition,
	}
}

func (dap *DefaultNodeManagerPlugin) Validate(_ interface{}) error {
	return nil
}

type DefaultNodeManager struct {
	partition interfaces.Partition
}

func (dnm *DefaultNodeManager) Run() {
}

func (dnm *DefaultNodeManager) Stop() {
}

func (dnm *DefaultNodeManager) GetNodeIterator(_ interfaces.Request) interfaces.NodeIterator {
	if nodes := dnm.partition.GetSchedulableNodes(); len(nodes) != 0 {
		sortingPolicyType := dnm.partition.GetNodeSortingPolicyType()
		if sortingPolicyType == policies.Unknown {
			return nil
		}
		// Sort Nodes based on the policy configured.
		SortNodes(nodes, sortingPolicyType)
		return NewDefaultNodeIterator(nodes)
	}
	return nil
}

func (dnm *DefaultNodeManager) Add(_ interfaces.Node) {
}

func (dnm *DefaultNodeManager) Remove(_ interfaces.Node) {
}

func (dnm *DefaultNodeManager) StateUpdated(_ interfaces.Node) {
}

func (dnm *DefaultNodeManager) ResourceUpdated(_ interfaces.Node) {
}

func SortNodes(nodes []interfaces.Node, sortType policies.SortingPolicy) {
	sortingStart := time.Now()
	switch sortType {
	case policies.FairnessPolicy:
		// Sort by available resource, descending order
		sort.SliceStable(nodes, func(i, j int) bool {
			l := nodes[i]
			r := nodes[j]
			return resources.CompUsageShares(l.GetAvailableResource(), r.GetAvailableResource()) > 0
		})
	case policies.BinPackingPolicy:
		// Sort by available resource, ascending order
		sort.SliceStable(nodes, func(i, j int) bool {
			l := nodes[i]
			r := nodes[j]
			return resources.CompUsageShares(r.GetAvailableResource(), l.GetAvailableResource()) > 0
		})
	}
	metrics.GetSchedulerMetrics().ObserveNodeSortingLatency(sortingStart)
}
