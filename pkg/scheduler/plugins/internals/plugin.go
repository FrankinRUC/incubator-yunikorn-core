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

package internals

import (
	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/plugins/internals/nodesorting"
	"github.com/mitchellh/mapstructure"
	"go.uber.org/zap"
)

const (
	InternalAppsRequestsPluginName = "InternalAppsRequestsPlugin"
	InternalNodeManagerPluginName  = "InternalNodeManagerPlugin"
)

func NewInternalAppsRequestsPlugin(_ interface{}) (interfaces.Plugin, error) {
	return &InternalAppsRequestsPlugin{}, nil
}

// This is the default implementation of RequestsPlugins
type InternalAppsRequestsPlugin struct {
}

func (drp *InternalAppsRequestsPlugin) Name() string {
	return InternalAppsRequestsPluginName
}

func (drp *InternalAppsRequestsPlugin) NewRequests() interfaces.Requests {
	return NewSortedRequests()
}

func (drp *InternalAppsRequestsPlugin) NewApplications(queue interfaces.Queue) interfaces.Applications {
	return NewInternalApplications(queue)
}

func NewInternalNodeManagerPlugin(_ interface{}) (interfaces.Plugin, error) {
	return &InternalNodeManagerPlugin{}, nil
}

// This is the default implementation of Node
type InternalNodeManagerPlugin struct {
}

func (dap *InternalNodeManagerPlugin) Name() string {
	return InternalNodeManagerPluginName
}

func (dap *InternalNodeManagerPlugin) NewNodeManager(partition interfaces.Partition,
	args interface{}) interfaces.NodeManager {
	nodeEvaluator, nodeSortingAlgorithm, err := parseArgs(args)
	if err != nil {
		log.Logger().Warn("failed to create new node manager, use default node manager", zap.Error(err))
		nodeSortingAlgorithm = getDefaultNodeManager()
	}
	nodeSortingAlgorithm.Init(partition, nodeEvaluator)
	log.Logger().Debug("created new node manager", zap.String("partition", partition.GetName()))
	return nodeSortingAlgorithm
}

func (dap *InternalNodeManagerPlugin) Validate(args interface{}) error {
	_, _, err := parseArgs(args)
	return err
}

func parseArgs(args interface{}) (nodesorting.NodeEvaluator, nodesorting.InternalNodeSortingAlgorithmIf, error) {
	var nodeManagerConfig *nodesorting.NodeManagerConfig
	if _, ok := args.(*nodesorting.NodeManagerConfig); ok {
		nodeManagerConfig = args.(*nodesorting.NodeManagerConfig)
	} else {
		nodeManagerConfig = &nodesorting.NodeManagerConfig{}
		err := mapstructure.Decode(args, &nodeManagerConfig)
		if err != nil {
			return nil, nil, err
		}
	}
	nodeEvaluator, err := nodesorting.NewDefaultNodeEvaluator(nodeManagerConfig.NodeEvaluator)
	if err != nil {
		return nil, nil, err
	}
	nsaRegistry := nodesorting.NewNSARegistry()
	nodeSortingAlgorithm, err := nsaRegistry.GenerateNodeSortingAlgorithm(nodeManagerConfig.NodeSortingAlgorithm)
	if err != nil {
		return nil, nil, err
	}
	return nodeEvaluator, nodeSortingAlgorithm, nil
}

func getDefaultNodeManager() nodesorting.InternalNodeSortingAlgorithmIf {
	algorithm, _ := nodesorting.NewIncrementalNodeSortingAlgorithm(nil)
	return algorithm
}

type NodeManagerPluginConfig struct {
	NodeEvaluatorConfig        nodesorting.NodeEvaluatorConfig
	NodeSortingAlgorithmConfig interface{}
}
