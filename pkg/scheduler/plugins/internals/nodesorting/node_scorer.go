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

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/mitchellh/mapstructure"
)

const (
	SingleResourceUsageScorerName   = "single_resource_usage"
	DominantResourceUsageScorerName = "dominant_resource_usage"
	ScarceResourceUsageScorerName   = "scarce_resource_usage"
)

func init() {
	RegisterNodeScorerMaker(SingleResourceUsageScorerName, NewSingleResourceUsageScorer)
	RegisterNodeScorerMaker(DominantResourceUsageScorerName, NewDominantResourceUsageScorer)
	RegisterNodeScorerMaker(ScarceResourceUsageScorerName, NewScarceResourceUsageScorerFactory)
}

// NodeScorer is a common scorer interface to give a sub-score for specified node
type NodeScorer interface {
	Score(node interfaces.Node) int64
}

// NodeScorerFactory is a scorer factory interface which can generate
// new node scorer according to specified request.
type NodeScorerFactory interface {
	NewNodeScorer(request interfaces.Request) NodeScorer
}

type SingleResourceUsageScorer struct {
	config *SingleResourceUsageScorerConfig
}

type SingleResourceUsageScorerConfig struct {
	ResourceName string
}

func NewSingleResourceUsageScorer(args interface{}) (interface{}, error) {
	config := &SingleResourceUsageScorerConfig{}
	err := mapstructure.Decode(args, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SingleResourceUsageScorerConfig: %s", err.Error())
	}
	if config.ResourceName == "" {
		return nil, fmt.Errorf("failed to parse SingleResourceUsageScorerConfig:" +
			" required ResourceName is not configured")
	}
	return &SingleResourceUsageScorer{config: config}, nil
}

func (rus *SingleResourceUsageScorer) Score(node interfaces.Node) int64 {
	avail := node.GetAvailableResource()
	total := node.GetCapacity()
	if availResQuantity, ok := avail.Resources[rus.config.ResourceName]; ok {
		totalResQuantity := total.Resources[rus.config.ResourceName]
		return MaxNodeScore * int64(availResQuantity) / int64(totalResQuantity)
	}
	return 0
}

type DominantResourceUsageScorer struct {
	config *DominantResourceUsageScorerConfig
}

type DominantResourceUsageScorerConfig struct {
	ResourceNames []string
}

func NewDominantResourceUsageScorer(args interface{}) (interface{}, error) {
	config := &DominantResourceUsageScorerConfig{}
	err := mapstructure.Decode(args, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ResourceUsageScorerConfig: %s", err.Error())
	}
	return &DominantResourceUsageScorer{config: config}, nil
}

func (drus *DominantResourceUsageScorer) Score(node interfaces.Node) int64 {
	avail := node.GetAvailableResource()
	total := node.GetCapacity()
	var curScore, minScore int64
	resourceNames := drus.config.ResourceNames
	// if resource names are not configured, use all resource names the node has.
	if resourceNames == nil {
		resourceNames = make([]string, 0)
		for resourceName := range avail.Resources {
			resourceNames = append(resourceNames, resourceName)
		}
	}

	for i, resourceName := range resourceNames {
		if availResQuantity, ok := avail.Resources[resourceName]; ok {
			totalResQuantity := total.Resources[resourceName]
			if totalResQuantity == 0 {
				curScore = 0
			} else {
				curScore = MaxNodeScore * int64(availResQuantity) / int64(totalResQuantity)
			}
		} else {
			curScore = 0
		}
		if i == 0 || curScore < minScore {
			minScore = curScore
		}
	}
	return minScore
}

type ScarceResourceUsageScorerFactory struct {
	config *ScarceResourceUsageScorerConfig
}

type ScarceResourceUsageScorer struct {
	config           *ScarceResourceUsageScorerConfig
	requiredQuantity resources.Quantity
}

type ScarceResourceUsageScorerConfig struct {
	ScarceResourceName string
}

func NewScarceResourceUsageScorerFactory(args interface{}) (interface{}, error) {
	config := &ScarceResourceUsageScorerConfig{}
	err := mapstructure.Decode(args, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ScarceResourceUsageScorerConfig: %s", err.Error())
	}
	if config.ScarceResourceName == "" {
		return nil, fmt.Errorf("failed to parse ScarceResourceUsageScorerConfig: " +
			"required ScarceResourceName is not configured")
	}
	return &ScarceResourceUsageScorerFactory{config: config}, nil
}

func (srsf *ScarceResourceUsageScorerFactory) NewNodeScorer(request interfaces.Request) NodeScorer {
	if request == nil {
		return nil
	}
	// return new scorer if this request contains the specified scarce resource
	if requiredQuantity, ok := request.GetAllocatedResource().Resources[srsf.config.ScarceResourceName]; ok {
		return &ScarceResourceUsageScorer{
			config:           srsf.config,
			requiredQuantity: requiredQuantity,
		}
	}
	// return nil if this request doesn't need the specified scarce resource
	return nil
}

func (srs *ScarceResourceUsageScorer) Score(node interfaces.Node) int64 {
	quantity := node.GetAvailableResource().Resources[srs.config.ScarceResourceName]
	if quantity >= srs.requiredQuantity {
		totalQuantity := node.GetCapacity().Resources[srs.config.ScarceResourceName]
		if totalQuantity == 0 {
			return 0
		}
		return MaxNodeScore * int64(quantity) / int64(totalQuantity)
	}
	return 0
}
