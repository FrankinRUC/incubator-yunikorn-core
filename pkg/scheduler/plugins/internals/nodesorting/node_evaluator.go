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

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

const (
	// MaxNodeScore is the maximum score a Scorer is expected to return.
	MaxNodeScore int64 = 1000
)

// interface of node evaluator which can give all-round scores
// including a static score (only depends on the node)
// and a dynamic score (depends on not only the node but also the request)
// for specified node.
type NodeEvaluator interface {
	GetValidDynamicScorerConfigs(request interfaces.Request) []*NodeScorerConfig
	CalculateStaticScore(node interfaces.Node) (int64, map[string]int64)
	CalculateDynamicScore(validDynamicScorers []*NodeScorerConfig, node interfaces.Node) (int64, map[string]int64)
	CalculateScore(validDynamicScorers []*NodeScorerConfig, node interfaces.Node) (int64, map[string]int64)
}

type DefaultNodeEvaluator struct {
	staticScorerConfigs  []*NodeScorerConfig
	dynamicScorerConfigs []*NodeScorerConfig
}

func NewDefaultNodeEvaluator(nodeEvaluatorConfig *NodeEvaluatorConfig) (NodeEvaluator, error) {
	var scorerConfigs []*NodeScorerConfig
	if nodeEvaluatorConfig == nil || len(nodeEvaluatorConfig.ScorerConfigs) == 0 {
		scorerConfigs = getDefaultNodeScorerConfigs()
		log.Logger().Info("using default scorer configs since node evaluator has not been configured")
	} else {
		scorerConfigs = nodeEvaluatorConfig.ScorerConfigs
	}
	staticScorerConfigs := make([]*NodeScorerConfig, 0)
	dynamicScorerConfigs := make([]*NodeScorerConfig, 0)
	for _, scorerConfig := range scorerConfigs {
		if scorerOrFactory, err := getScorerOrFactory(scorerConfig); err == nil {
			switch scorerOrFactory.(type) {
			case NodeScorer:
				staticScorerConfigs = append(staticScorerConfigs, scorerConfig)
			case NodeScorerFactory:
				dynamicScorerConfigs = append(dynamicScorerConfigs, scorerConfig)
			default:
			}
		} else {
			return nil, err
		}
	}
	log.Logger().Info("node evaluator initialized")
	return &DefaultNodeEvaluator{
		staticScorerConfigs:  staticScorerConfigs,
		dynamicScorerConfigs: dynamicScorerConfigs,
	}, nil
}

func getDefaultNodeScorerConfigs() []*NodeScorerConfig {
	singleScorerName := DominantResourceUsageScorerName
	scorer, _ := GetNodeScorerOrFactory(singleScorerName, nil)
	scorerConfigs := []*NodeScorerConfig{
		{
			Name:   singleScorerName,
			Weight: 1,
			Scorer: scorer,
		},
	}
	return scorerConfigs
}

func getScorer(nsc *NodeScorerConfig) NodeScorer {
	// skip error check since error has been verified when updating the config
	scorerOrFactory, _ := getScorerOrFactory(nsc)
	if scorerOrFactory != nil {
		if scorer, ok := scorerOrFactory.(NodeScorer); ok {
			return scorer
		}
	}
	return nil
}

func getScorerOrFactory(nsc *NodeScorerConfig) (interface{}, error) {
	if nsc.Scorer == nil {
		scorer, err := GetNodeScorerOrFactory(nsc.Name, nsc.Args)
		if err != nil {
			log.Logger().Error("failed to get scorer or scorer factory", zap.Error(err))
			return nil, err
		}
		nsc.Scorer = scorer
		log.Logger().Info("initialized scorer", zap.Any("name", nsc.Name), zap.Any("weight", nsc.Weight))
	}
	return nsc.Scorer, nil
}

func (dne *DefaultNodeEvaluator) GetValidDynamicScorerConfigs(request interfaces.Request) []*NodeScorerConfig {
	validScorerConfigs := make([]*NodeScorerConfig, 0)
	for _, dsc := range dne.dynamicScorerConfigs {
		// skip error check since error has been verified when updating the config
		if scorerOrFactory, _ := getScorerOrFactory(dsc); scorerOrFactory != nil {
			if nodeScorerFactory, ok := scorerOrFactory.(NodeScorerFactory); ok {
				dynamicNodeScorer := nodeScorerFactory.NewNodeScorer(request)
				if dynamicNodeScorer != nil {
					validScorerConfigs = append(validScorerConfigs, &NodeScorerConfig{
						Name:   dsc.Name,
						Weight: dsc.Weight,
						Scorer: dynamicNodeScorer,
					})
				}
			}
		}
	}
	return validScorerConfigs
}

func (dne *DefaultNodeEvaluator) CalculateStaticScore(node interfaces.Node) (int64, map[string]int64) {
	var score int64
	subScores := make(map[string]int64)
	for _, ssc := range dne.staticScorerConfigs {
		scorer := getScorer(ssc)
		if scorer != nil {
			subScore := scorer.Score(node)
			subScores[ssc.Name] = subScore
			score += subScore * ssc.Weight
		}
	}
	return score, subScores
}

func (dne *DefaultNodeEvaluator) CalculateDynamicScore(validDynamicScorerConfigs []*NodeScorerConfig, node interfaces.Node) (int64, map[string]int64) {
	var score int64
	subScores := make(map[string]int64)
	for _, vsc := range validDynamicScorerConfigs {
		scorer := getScorer(vsc)
		if scorer != nil {
			subScore := scorer.Score(node)
			subScores[vsc.Name] = subScore
			score += subScore * vsc.Weight
		}
	}
	return score, subScores
}

func (dne *DefaultNodeEvaluator) CalculateScore(validDynamicScorerConfigs []*NodeScorerConfig,
	node interfaces.Node) (int64, map[string]int64) {
	score, subScores := dne.CalculateStaticScore(node)
	for _, vsc := range validDynamicScorerConfigs {
		scorer := getScorer(vsc)
		if scorer != nil {
			subScore := scorer.Score(node)
			subScores[vsc.Name] = subScore
			score += subScore * vsc.Weight
		}
	}
	return score, subScores
}

type NewNodeScorerFunc func(conf interface{}) (interface{}, error)

//type NewNodeSortingAlgorithmFunc func(conf map[string]interface{}) (interface{}, error)

var (
	NodeScorerMakers = make(map[string]NewNodeScorerFunc)
	//NodeSortingAlgorithmMakers = make(map[string]NewNodeSortingAlgorithmFunc)
)

func RegisterNodeScorerMaker(name string, newNodeScorerFunc NewNodeScorerFunc) {
	NodeScorerMakers[name] = newNodeScorerFunc
}

func GetNodeScorerOrFactory(name string, args interface{}) (interface{}, error) {
	scorerMaker := NodeScorerMakers[name]
	if scorerMaker == nil {
		return nil, fmt.Errorf("node scorer '%s' not found", name)
	}
	scorer, err := scorerMaker(args)
	if err != nil {
		return nil, err
	}
	return scorer, nil
}

//func RegisterNodeSortingAlgorithmMaker(name string, newNodeSortingAlgorithmFunc NewNodeSortingAlgorithmFunc) {
//	NodeSortingAlgorithmMakers[name] = newNodeSortingAlgorithmFunc
//}

//func GetNodeSortingAlgorithm(name string, conf map[string]interface{}) (interface{}, error) {
//	nodeSortingAlgoMaker := NodeSortingAlgorithmMakers[name]
//	if nodeSortingAlgoMaker == nil {
//		return nil, fmt.Errorf("node sorting algorithm '%s' not found", name)
//	}
//	algorithm, err := nodeSortingAlgoMaker(conf)
//	if err != nil {
//		return nil, fmt.Errorf("failed to initialize node sorting algorithm %s: %s",
//			name, err.Error())
//	}
//	return algorithm, nil
//}
