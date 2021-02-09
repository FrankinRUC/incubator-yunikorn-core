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
	"sync"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/plugins/defaults"

	"github.com/apache/incubator-yunikorn-core/pkg/log"

	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/plugins/internals/maps"

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
)

// This is an implementation of Requests with two-level sorted structure,
// the first level has a map of all requests and priority groups in the descending order by priority.
// It's not thread-safe, must be called while holding the lock.
type SortedRequests struct {
	// a map of all requests
	requests map[string]interfaces.Request
	// sorted priority groups in the descending order by priority
	sortedPriorityGroups *maps.SortableLinkedMap

	// cached priority group keeps remaining priority group has not been iterated already
	cachedPriorityGroups []PriorityGroup

	sync.RWMutex
}

func NewSortedRequests() *SortedRequests {
	return &SortedRequests{
		requests: make(map[string]interfaces.Request),
		sortedPriorityGroups: maps.NewSortableLinkedMap(func(i, j interface{}) bool {
			return i.(PriorityGroup).GetPriority() > j.(PriorityGroup).GetPriority()
		}, func(value interface{}) bool {
			return value.(PriorityGroup).IsPending()
		}),
	}
}

func (sr *SortedRequests) AddOrUpdateRequest(request interfaces.Request) interfaces.Request {
	if request == nil {
		return nil
	}
	sr.Lock()
	defer sr.Unlock()
	existingRequest := sr.requests[request.GetAllocationKey()]
	// add or update in priority group
	priorityGroup := sr.sortedPriorityGroups.Get(request.GetPriority())
	if priorityGroup == nil {
		priorityGroup = NewSortedPriorityGroup(request.GetPriority())
	}
	priorityGroup.(PriorityGroup).AddRequest(request)
	// this must be called to update pending state
	sr.sortedPriorityGroups.Put(request.GetPriority(), priorityGroup)
	// add or update in map
	sr.requests[request.GetAllocationKey()] = request
	return existingRequest
}

func (sr *SortedRequests) RemoveRequest(allocationKey string) interfaces.Request {
	sr.Lock()
	defer sr.Unlock()
	existingRequest := sr.requests[allocationKey]
	if existingRequest != nil {
		// remove from map
		delete(sr.requests, allocationKey)
		// remove from priority group
		priorityGroup := sr.sortedPriorityGroups.Get(existingRequest.GetPriority())
		if priorityGroup != nil {
			priorityGroup.(*SortedPriorityGroup).RemoveRequest(allocationKey)
			if priorityGroup.(*SortedPriorityGroup).Size() == 0 {
				// remove this priority group if it has not any request
				sr.sortedPriorityGroups.Remove(existingRequest.GetPriority())
			} else {
				// this must be called to update pending state
				sr.sortedPriorityGroups.Put(priorityGroup.(*SortedPriorityGroup).GetPriority(), priorityGroup)
			}
		}
	}
	return existingRequest
}

func (sr *SortedRequests) GetRequest(allocationKey string) interfaces.Request {
	sr.RLock()
	defer sr.RUnlock()
	return sr.requests[allocationKey]
}

func (sr *SortedRequests) Size() int {
	sr.RLock()
	defer sr.RUnlock()
	return len(sr.requests)
}

func (sr *SortedRequests) Reset() {
	sr.Lock()
	defer sr.Unlock()
	sr.requests = make(map[string]interfaces.Request)
	sr.sortedPriorityGroups.Reset()
}

func (sr *SortedRequests) GetTopPendingPriorityGroup() PriorityGroup {
	sr.RLock()
	defer sr.RUnlock()
	_, v := sr.sortedPriorityGroups.GetFirstMatched()
	if v != nil {
		return v.(PriorityGroup)
	}
	return nil
}

func (sr *SortedRequests) GetPendingPriorityGroups() []PriorityGroup {
	sr.RLock()
	defer sr.RUnlock()
	priorityGroups := make([]PriorityGroup, 0)
	pendingPGIt := sr.sortedPriorityGroups.GetMatchedIterator()
	for pendingPGIt.HasNext() {
		if _, pg := pendingPGIt.Next(); pg != nil {
			priorityGroups = append(priorityGroups, pg.(PriorityGroup))
		}
	}
	return priorityGroups
}

// Requests are grouped and sorted by priority, each iterator only got requests belong to one priority,
// clear cache can make the priority iterator back to the highest priority.
func (sr *SortedRequests) ClearCache() {

}

func (sr *SortedRequests) GetRequests(filter func(request interfaces.Request) bool) []interfaces.Request {
	sr.RLock()
	defer sr.RUnlock()
	//should never be called
	log.Logger().Fatal("SortedRequests#GetRequests should never be called")
	return nil
}

func (dr *SortedRequests) SetCachedPriorityGroups(priorityGroups []PriorityGroup) {
	dr.Lock()
	defer dr.Unlock()
	dr.cachedPriorityGroups = priorityGroups
}

func (dr *SortedRequests) SortForAllocation() interfaces.RequestIterator {
	dr.RLock()
	defer dr.RUnlock()
	priorityGroups := make([]PriorityGroup, 0)
	if dr.cachedPriorityGroups == nil {
		// for testing
		priorityGroupIt := dr.sortedPriorityGroups.GetMatchedIterator()
		for priorityGroupIt.HasNext() {
			_, v := priorityGroupIt.Next()
			priorityGroups = append(priorityGroups, v.(PriorityGroup))
		}
	} else if len(dr.cachedPriorityGroups) == 0 {
		log.Logger().Info("cached priority groups are empty")
		return defaults.NewDefaultRequestIterator([]interfaces.Request{})
	} else {
		priorityGroups = append(priorityGroups, dr.cachedPriorityGroups[0])
		dr.cachedPriorityGroups = dr.cachedPriorityGroups[1:]
	}
	requests := make([]interfaces.Request, 0)
	for _, pg := range priorityGroups {
		requestIt := pg.GetPendingRequestIterator()
		for requestIt.HasNext() {
			requests = append(requests, requestIt.Next())
		}
	}
	return defaults.NewDefaultRequestIterator(requests)
}

func (dr *SortedRequests) SortForPreemption() interfaces.RequestIterator {
	//TODO this should be implemented when refactoring the preemption process
	return nil
}

// This is the second level of SortedRequests
type SortedPriorityGroup struct {
	// priority of the group
	priority int32
	// create time of the group
	createTime time.Time
	// sorted requests in ascending order by the creation time of request
	sortedRequests *maps.SortableLinkedMap
}

func NewSortedPriorityGroup(priority int32) *SortedPriorityGroup {
	return &SortedPriorityGroup{
		priority:   priority,
		createTime: time.Now(),
		sortedRequests: maps.NewSortableLinkedMap(func(i, j interface{}) bool {
			return i.(interfaces.Request).GetCreateTime().Before(j.(interfaces.Request).GetCreateTime())
		}, func(value interface{}) bool {
			return value.(interfaces.Request).GetPendingAskRepeat() > 0
		}),
	}
}

func (spg *SortedPriorityGroup) AddRequest(request interfaces.Request) {
	spg.sortedRequests.Put(request.GetAllocationKey(), request)
}

func (spg *SortedPriorityGroup) RemoveRequest(allocationKey string) {
	spg.sortedRequests.Remove(allocationKey)
}

func (spg *SortedPriorityGroup) GetPriority() int32 {
	return spg.priority
}

func (spg *SortedPriorityGroup) GetCreateTime() time.Time {
	return spg.createTime
}

func (spg *SortedPriorityGroup) Size() int {
	return spg.sortedRequests.Size()
}

func (spg *SortedPriorityGroup) IsPending() bool {
	return spg.sortedRequests.HasMatched()
}

func (spg *SortedPriorityGroup) GetPendingRequestIterator() interfaces.RequestIterator {
	return NewSortedRequestIterator(spg.sortedRequests.GetMatchedIterator())
}

type SortedRequestIterator struct {
	mapIt maps.MapIterator
	index int
}

func NewSortedRequestIterator(mapIt maps.MapIterator) *SortedRequestIterator {
	return &SortedRequestIterator{
		mapIt: mapIt,
		index: 0,
	}
}

func (sri *SortedRequestIterator) HasNext() (ok bool) {
	return sri.mapIt.HasNext()
}

func (sri *SortedRequestIterator) Next() interfaces.Request {
	_, v := sri.mapIt.Next()
	return v.(interfaces.Request)
}

func (sri *SortedRequestIterator) Size() int {
	return sri.mapIt.Size()
}

// This interface is intended for getting useful information from priority groups
type PriorityGroup interface {
	// add or update a request
	AddRequest(request interfaces.Request)
	// remove the request with the specified allocation key
	RemoveRequest(allocationKey string)
	// return the priority
	GetPriority() int32
	// return the create time
	GetCreateTime() time.Time
	// return the size of requests
	Size() int
	// return the pending state
	IsPending() bool
	// return requests iterator for pending requests
	GetPendingRequestIterator() interfaces.RequestIterator
	// return requests iterator for all requests
	//GetRequestIterator() interfaces.RequestIterator
}
