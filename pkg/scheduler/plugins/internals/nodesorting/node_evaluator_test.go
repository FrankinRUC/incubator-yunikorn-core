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
	"strconv"
	"testing"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/plugins/defaults"
	"gotest.tools/assert"

	"gopkg.in/yaml.v2"
)

func newResource(quantities []resources.Quantity) *resources.Resource {
	resourceQuantities := make(map[string]resources.Quantity)
	for idx, quantity := range quantities {
		resourceQuantities["res-"+strconv.Itoa(idx)] = quantity
	}
	return resources.NewResourceFromMap(resourceQuantities)
}

func testScore(nodeEvaluator NodeEvaluator, totalRes []resources.Quantity, availRes []resources.Quantity,
	requestRes []resources.Quantity) int64 {
	node := defaults.NewFakeNode("test-node", newResource(totalRes))
	node.SetAvailableResource(newResource(availRes))
	request := defaults.NewFakeRequest("alloc-1", 1, 1, newResource(requestRes))
	score, _ := nodeEvaluator.CalculateScore(nodeEvaluator.GetValidDynamicScorerConfigs(request), node)
	return score
}

func parseNodeEvaluator(config string) (NodeEvaluator, error) {
	nodeEvaluatorConfig := &NodeEvaluatorConfig{}
	yaml.Unmarshal([]byte(config), &nodeEvaluatorConfig)
	return NewDefaultNodeEvaluator(nodeEvaluatorConfig)
}

func TestDominantResourceUsageScorer(t *testing.T) {
	config_str := `
scorerconfigs:
  - name: dominant_resource_usage
    weight: 1
`
	dominantNodeEvaluator, err := parseNodeEvaluator(config_str)
	assert.NilError(t, err, "failed to initialize node evaluator")

	config_str = `
scorerconfigs:
  - name: dominant_resource_usage
    weight: 1
    args:
      resourcenames:
        - res-0
        - res-1
`
	partDominantNodeEvaluator, err := parseNodeEvaluator(config_str)
	assert.NilError(t, err, "failed to initialize node evaluator")

	testcases := []struct {
		nodeEvaluator NodeEvaluator
		totalRes      []resources.Quantity
		availRes      []resources.Quantity
		requestRes    []resources.Quantity
		expectedScore int64
		message       string
	}{
		{
			nodeEvaluator: dominantNodeEvaluator,
			totalRes:      []resources.Quantity{100, 100, 0},
			availRes:      []resources.Quantity{30, 50, 0},
			requestRes:    []resources.Quantity{1, 1, 0},
			expectedScore: 0,
			message:       "default evaluator: ignore zero dimension in total resource",
		},
		{
			nodeEvaluator: dominantNodeEvaluator,
			totalRes:      []resources.Quantity{100, 100, 100},
			availRes:      []resources.Quantity{30, 50, 70},
			requestRes:    []resources.Quantity{1, 1, 0},
			expectedScore: 300,
			message:       "default evaluator: dominant",
		},
		{
			nodeEvaluator: dominantNodeEvaluator,
			totalRes:      []resources.Quantity{100, 100, 100},
			availRes:      []resources.Quantity{30, 50, 10},
			requestRes:    []resources.Quantity{1, 1, 0},
			expectedScore: 100,
			message:       "default evaluator: dominant",
		},
		{
			nodeEvaluator: partDominantNodeEvaluator,
			totalRes:      []resources.Quantity{100, 100, 100},
			availRes:      []resources.Quantity{30, 50, 10},
			requestRes:    []resources.Quantity{1, 1, 0},
			expectedScore: 300,
			message:       "default evaluator: part dominant - only consider res-1, res-2",
		},
	}
	for _, testcase := range testcases {
		actualScore := testScore(testcase.nodeEvaluator, testcase.totalRes, testcase.availRes, testcase.requestRes)
		assert.Equal(t, actualScore, testcase.expectedScore, testcase.message)
	}
}

func TestSingleResourceUsageScorer(t *testing.T) {
	// verify error when required resourcename is missing.
	config_str := `
scorerconfigs:
  - name: single_resource_usage
    weight: 1
`
	_, err := parseNodeEvaluator(config_str)
	assert.Error(t, err, "failed to parse SingleResourceUsageScorerConfig: "+
		"required ResourceName is not configured")

	// use single resource usage scorer for res-0
	config_str = `
scorerconfigs:
  - name: single_resource_usage
    weight: 1
    args:
      resourcename: res-0
`
	nodeEvaluatorForRes0, err := parseNodeEvaluator(config_str)
	assert.NilError(t, err, "failed to initialize node evaluator")
	assert.NilError(t, err, "failed to initialize node evaluator")

	// use single resource usage for res-1
	config_str = `
scorerconfigs:
  - name: single_resource_usage
    weight: 1
    args:
      resourcename: res-1
`
	nodeEvaluatorForRes1, err := parseNodeEvaluator(config_str)
	assert.NilError(t, err, "failed to initialize node evaluator")

	testcases := []struct {
		nodeEvaluator NodeEvaluator
		totalRes      []resources.Quantity
		availRes      []resources.Quantity
		requestRes    []resources.Quantity
		expectedScore int64
		message       string
	}{
		{
			nodeEvaluator: nodeEvaluatorForRes0,
			totalRes:      []resources.Quantity{100, 100, 0},
			availRes:      []resources.Quantity{30, 50, 0},
			requestRes:    []resources.Quantity{1, 1, 0},
			expectedScore: 300,
			message:       "single scorer: res-0",
		},
		{
			nodeEvaluator: nodeEvaluatorForRes1,
			totalRes:      []resources.Quantity{100, 100, 100},
			availRes:      []resources.Quantity{30, 50, 70},
			requestRes:    []resources.Quantity{1, 1, 0},
			expectedScore: 500,
			message:       "single scorer: res-1",
		},
	}
	for _, testcase := range testcases {
		actualScore := testScore(testcase.nodeEvaluator, testcase.totalRes, testcase.availRes, testcase.requestRes)
		assert.Equal(t, actualScore, testcase.expectedScore, testcase.message)
	}
}

func TestScarceResourceUsageScorer(t *testing.T) {
	// verify error when required ScarceResourceName is missing.
	config_str := `
scorerconfigs:
  - name: scarce_resource_usage
    weight: 1
`
	_, err := parseNodeEvaluator(config_str)
	assert.Error(t, err, "failed to parse ScarceResourceUsageScorerConfig:"+
		" required ScarceResourceName is not configured")

	// use scarce resource usage scorer for res-0
	config_str = `
scorerconfigs:
  - name: scarce_resource_usage
    weight: 1
    args:
      scarceresourcename: res-2
`
	nodeEvaluator, err := parseNodeEvaluator(config_str)
	assert.NilError(t, err, "failed to initialize node evaluator")

	testcases := []struct {
		nodeEvaluator NodeEvaluator
		totalRes      []resources.Quantity
		availRes      []resources.Quantity
		requestRes    []resources.Quantity
		expectedScore int64
		message       string
	}{
		{
			nodeEvaluator: nodeEvaluator,
			totalRes:      []resources.Quantity{100, 100, 0},
			availRes:      []resources.Quantity{30, 50, 0},
			requestRes:    []resources.Quantity{1, 1, 0},
			expectedScore: 0,
			message:       "scarce scorer: ignore zero dimension in total resource",
		},
		{
			nodeEvaluator: nodeEvaluator,
			totalRes:      []resources.Quantity{100, 100, 100},
			availRes:      []resources.Quantity{30, 50, 70},
			requestRes:    []resources.Quantity{1, 1, 1},
			expectedScore: 700,
			message:       "scarce scorer: dominant",
		},
	}
	for _, testcase := range testcases {
		actualScore := testScore(testcase.nodeEvaluator, testcase.totalRes, testcase.availRes, testcase.requestRes)
		assert.Equal(t, actualScore, testcase.expectedScore, testcase.message)
	}
}

func TestCombinedResourceUsageScorer(t *testing.T) {
	// use dominant and scarce resource usage scorer
	config_str := `
scorerconfigs:
  - name: dominant_resource_usage
    weight: 1
    args:
      resourcenames:
        - res-0
        - res-1
  - name: scarce_resource_usage
    weight: 1
    args:
      scarceresourcename: res-2
`
	balanceNodeEvaluator, err := parseNodeEvaluator(config_str)
	assert.NilError(t, err, "failed to initialize node evaluator")

	// use dominant and scarce resource usage scorer
	config_str = `
scorerconfigs:
  - name: dominant_resource_usage
    weight: 1
    args:
      resourcenames:
        - res-0
        - res-1
  - name: scarce_resource_usage
    weight: 2
    args:
      scarceresourcename: res-2
`
	scarceHeavyNodeEvaluator, err := parseNodeEvaluator(config_str)
	assert.NilError(t, err, "failed to initialize node evaluator")

	testcases := []struct {
		nodeEvaluator NodeEvaluator
		totalRes      []resources.Quantity
		availRes      []resources.Quantity
		requestRes    []resources.Quantity
		expectedScore int64
		message       string
	}{
		{
			nodeEvaluator: balanceNodeEvaluator,
			totalRes:      []resources.Quantity{100, 100, 0},
			availRes:      []resources.Quantity{30, 50, 0},
			requestRes:    []resources.Quantity{1, 1, 0},
			expectedScore: 300,
			message:       "scarce scorer: ignore zero dimension in total resource",
		},
		{
			nodeEvaluator: balanceNodeEvaluator,
			totalRes:      []resources.Quantity{100, 100, 100},
			availRes:      []resources.Quantity{30, 50, 70},
			requestRes:    []resources.Quantity{1, 1, 1},
			expectedScore: 1000,
			message:       "scarce scorer: dominant",
		},
		{
			nodeEvaluator: scarceHeavyNodeEvaluator,
			totalRes:      []resources.Quantity{100, 100, 100},
			availRes:      []resources.Quantity{30, 50, 70},
			requestRes:    []resources.Quantity{1, 1, 1},
			expectedScore: 1700,
			message:       "scarce scorer: dominant",
		},
	}
	for _, testcase := range testcases {
		actualScore := testScore(testcase.nodeEvaluator, testcase.totalRes, testcase.availRes, testcase.requestRes)
		assert.Equal(t, actualScore, testcase.expectedScore, testcase.message)
	}
}
