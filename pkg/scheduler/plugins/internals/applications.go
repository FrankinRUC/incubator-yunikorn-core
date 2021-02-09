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
	"sort"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/plugins/defaults"

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
)

// This is an implementation of Requests which keeps all requests in a map.
// It's not thread-safe, must be called while holding the lock of application.
type InternalApplications struct {
	mapper *defaults.CommonMapper
	queue  interfaces.Queue
}

type sortItem struct {
	app           interfaces.Application
	priorityGroup PriorityGroup
}

func NewInternalApplications(queue interfaces.Queue) interfaces.Applications {
	return &InternalApplications{
		mapper: defaults.NewCommonMapper(),
		queue:  queue.(interfaces.Queue),
	}
}

func (da *InternalApplications) AddApplication(app interfaces.Application) interfaces.Application {
	if app == nil {
		return nil
	}
	if existingApp := da.mapper.Add(app.GetApplicationID(), app); existingApp != nil {
		return existingApp.(interfaces.Application)
	}
	return nil
}

func (da *InternalApplications) RemoveApplication(appID string) interfaces.Application {
	if removedApp := da.mapper.Remove(appID); removedApp != nil {
		return removedApp.(interfaces.Application)
	}
	return nil
}

func (da *InternalApplications) GetApplication(appID string) interfaces.Application {
	if app := da.mapper.Get(appID); app != nil {
		return app.(interfaces.Application)
	}
	return nil
}

func (da *InternalApplications) GetApplications(
	filter func(request interfaces.Application) bool) []interfaces.Application {
	apps := make([]interfaces.Application, 0)
	for _, app := range da.mapper.GetItems() {
		if filter == nil || filter(app.(interfaces.Application)) {
			apps = append(apps, app.(interfaces.Application))
		}
	}
	return apps
}

func (da *InternalApplications) Size() int {
	return da.mapper.Size()
}

func (da *InternalApplications) Reset() {
	da.mapper.Reset()
}

func (da *InternalApplications) SortForAllocation() interfaces.AppIterator {
	// find pending apps
	var apps []interfaces.Application
	var comparators []func(l, r *sortItem, queue interfaces.Queue) (ok bool, less bool)
	switch da.queue.GetSortType() {
	case policies.FifoSortPolicy:
		apps = defaults.FilterOnPendingResources(da.queue.GetCopyOfApps())
		comparators = []func(l, r *sortItem, queue interfaces.Queue) (ok bool, less bool){
			comparePriority, compareAppSubmissionTime}
	case policies.FairSortPolicy:
		apps = defaults.FilterOnPendingResources(da.queue.GetCopyOfApps())
		comparators = []func(l, r *sortItem, queue interfaces.Queue) (ok bool, less bool){
			comparePriority, compareAppFairness}
	case policies.StateAwarePolicy:
		apps = defaults.StateAwareFilter(da.queue.GetCopyOfApps())
		comparators = []func(l, r *sortItem, queue interfaces.Queue) (ok bool, less bool){
			comparePriority, compareAppFairness}
	}
	// rearrange sort items: split app requests into different priority groups
	sortItems := make([]*sortItem, 0)
	for _, app := range apps {
		sortedRequests := app.GetRequestsWrapper().(*SortedRequests)
		pendingPGs := sortedRequests.GetPendingPriorityGroups()
		for _, pendingPG := range pendingPGs {
			sortItems = append(sortItems, &sortItem{app: app, priorityGroup: pendingPG})
		}
		sortedRequests.SetCachedPriorityGroups(pendingPGs)
	}
	// Sort applications based on the sort policy of queue
	Sort(da.queue, sortItems, false, comparators)
	// generate new apps which may contain duplicated apps
	newApps := make([]interfaces.Application, 0)
	for _, sortItem := range sortItems {
		newApps = append(newApps, sortItem.app)
	}
	// Return iterator of apps
	return defaults.NewDefaultAppIterator(newApps)
}

func (da *InternalApplications) SortForPreemption() interfaces.AppIterator {
	//TODO this should be implemented when refactoring the preemption process
	return nil
}

func comparePriority(l, r *sortItem, queue interfaces.Queue) (ok bool, less bool) {
	if l.priorityGroup.GetPriority() != r.priorityGroup.GetPriority() {
		return true, l.priorityGroup.GetPriority() > r.priorityGroup.GetPriority()
	}
	return false, true
}

func compareAppSubmissionTime(l, r *sortItem, queue interfaces.Queue) (ok bool, less bool) {
	if !l.app.GetSubmissionTime().Equal(r.app.GetSubmissionTime()) {
		return true, l.app.GetSubmissionTime().Before(r.app.GetSubmissionTime())
	}
	return false, true
}

func compareAppFairness(l, r *sortItem, queue interfaces.Queue) (ok bool, less bool) {
	compValue := resources.CompUsageRatio(l.app.GetAllocatedResource(), r.app.GetAllocatedResource(),
		queue.GetGuaranteedResource())
	if compValue != 0 {
		return true, compValue < 0
	}
	return false, true
}

func Sort(queue interfaces.Queue, sortItems []*sortItem, reverse bool,
	comparators []func(l, r *sortItem, queue interfaces.Queue) (ok bool, less bool)) {
	if len(sortItems) > 1 {
		sortingStart := time.Now()
		sort.SliceStable(sortItems, func(i, j int) bool {
			for _, comparator := range comparators {
				if ok, compV := comparator(sortItems[i], sortItems[j], queue); ok {
					if reverse {
						return !compV
					}
					return compV
				}
			}
			return true
		})
		metrics.GetSchedulerMetrics().ObserveAppSortingLatency(sortingStart)
	}
}
