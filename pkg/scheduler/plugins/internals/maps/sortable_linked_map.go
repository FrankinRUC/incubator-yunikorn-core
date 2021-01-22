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

package maps

import (
	"sync"
)

// Find type for requests
type FindType int

const (
	FindAll FindType = iota
	FindMatched
	FindUnMatched
)

// This is an implementation of Map interface which leverages the hash and linked list algorithms
// to make data sorted by specified comparison and efficient to be found and updated.
// Note that the order of linked list won't change when updating an existing entry for now.
// It's not thread-safe, must be called while holding the lock.
type SortableLinkedMap struct {
	// a map of entries
	entries map[interface{}]*SortableLinkedMapEntry
	// the head of linked entries
	head *SortableLinkedMapEntry
	// the tail of linked entries
	tail *SortableLinkedMapEntry
	// keep the first entry in linked entries that matches the specific
	// findFirstMatchFunc function.
	firstMatchedEntry *SortableLinkedMapEntry
	// keep linked entries sorted by the provided compareFunc function,
	// if not provided, always put the new entry into the tail.
	// return true if i should be placed before j, otherwise return false.
	compareFunc func(i, j interface{}) bool
	// function helps to locate the entry in specified condition.
	matchFunc func(value interface{}) bool

	sync.RWMutex
}

// This struct defines a doubly-linked entry
type SortableLinkedMapEntry struct {
	// key of this mapping
	key interface{}
	// value of this mapping
	value interface{}
	// the pre entry
	pre *SortableLinkedMapEntry
	// the next entry
	next *SortableLinkedMapEntry
	// whether this entry is matched to the specified condition
	isMatched bool
}

func NewSortableLinkedMap(compareFunc func(i, j interface{}) bool,
	matchFunc func(value interface{}) bool) *SortableLinkedMap {
	return &SortableLinkedMap{
		entries:     make(map[interface{}]*SortableLinkedMapEntry),
		compareFunc: compareFunc,
		matchFunc:   matchFunc,
	}
}

func (slm *SortableLinkedMap) getEntry(entry *SortableLinkedMapEntry) (interface{}, interface{}) {
	if entry != nil {
		return entry.key, entry.value
	}
	return nil, nil
}

func (slm *SortableLinkedMap) GetHead() (interface{}, interface{}) {
	slm.RLock()
	defer slm.RUnlock()
	return slm.getEntry(slm.head)
}

func (slm *SortableLinkedMap) GetTail() (interface{}, interface{}) {
	slm.RLock()
	defer slm.RUnlock()
	return slm.getEntry(slm.tail)
}

func (slm *SortableLinkedMap) GetIterator() MapIterator {
	return slm.GetIteratorByConditions(FindAll, false)
}

func (slm *SortableLinkedMap) GetIteratorByConditions(findType FindType, reverse bool) MapIterator {
	entries := slm.GetEntriesByConditions(findType, reverse)
	return NewSimpleMapIterator(entries)
}

func (slm *SortableLinkedMap) GetEntriesByConditions(findType FindType, reverse bool) []*SortableLinkedMapEntry {
	slm.RLock()
	defer slm.RUnlock()
	var lookupEntry *SortableLinkedMapEntry
	if !reverse {
		switch findType {
		case FindAll, FindUnMatched:
			lookupEntry = slm.head
		case FindMatched:
			lookupEntry = slm.firstMatchedEntry
		}
	} else {
		lookupEntry = slm.tail
	}
	entries := make([]*SortableLinkedMapEntry, 0)
	for {
		if lookupEntry == nil {
			break
		}
		if findType == FindAll ||
			(findType == FindMatched && lookupEntry.isMatched) ||
			(findType == FindUnMatched && !lookupEntry.isMatched) {
			entries = append(entries, lookupEntry)
		}
		if !reverse {
			lookupEntry = lookupEntry.next
		} else {
			lookupEntry = lookupEntry.pre
		}
	}
	return entries
}

func (slm *SortableLinkedMap) GetMatchedIterator() MapIterator {
	return slm.GetIteratorByConditions(FindMatched, false)
}

func (slm *SortableLinkedMap) GetFirstMatched() (interface{}, interface{}) {
	slm.RLock()
	defer slm.RUnlock()
	if slm.firstMatchedEntry == nil {
		return nil, nil
	}
	return slm.firstMatchedEntry.key, slm.firstMatchedEntry.value
}

func (slm *SortableLinkedMap) HasMatched() bool {
	slm.RLock()
	defer slm.RUnlock()
	return slm.firstMatchedEntry != nil
}

// must be called while holding the lock of map.
func (slm *SortableLinkedMap) findPreEntry(fromEntry, newEntry *SortableLinkedMapEntry,
	isForward bool) *SortableLinkedMapEntry {
	// always put the new entry into the tail if compareFunc isn't defined
	if slm.compareFunc == nil {
		return slm.tail
	}
	// find the entry
	lookupEntry := fromEntry
	for {
		if lookupEntry == nil {
			break
		} else {
			if isForward {
				if slm.compareFunc(newEntry.value, lookupEntry.value) {
					// move forward if newEntry is pre
					lookupEntry = lookupEntry.pre
				} else {
					// lookupEntry should be the pre
					return lookupEntry
				}
			} else {
				if slm.compareFunc(lookupEntry.value, newEntry.value) {
					// move backward if lookupEntry is pre
					lookupEntry = lookupEntry.next
				} else {
					// lookupEntry should be next to newEntry
					return lookupEntry.pre
				}
			}
		}
	}
	if isForward {
		return nil
	}
	return slm.tail
}

// must be called while holding the lock of map.
func (slm *SortableLinkedMap) putIntoLinkedList(preEntry, newEntry *SortableLinkedMapEntry) {
	// insert new entry into the linked list
	if preEntry != nil {
		// put after the specified pre entry
		newEntry.pre = preEntry
		newEntry.next = preEntry.next
		if preEntry.next != nil {
			preEntry.next.pre = newEntry
		}
		preEntry.next = newEntry
	} else {
		// put as the head
		if slm.head == nil {
			slm.head = newEntry
		} else {
			slm.head.pre = newEntry
			newEntry.next = slm.head
			slm.head = newEntry
			slm.head.pre = nil
		}
	}
	// update the tail if it's nil or pre entry
	if slm.tail == preEntry {
		slm.tail = newEntry
	}
}

// must be called while holding the lock of map.
func (slm *SortableLinkedMap) removeFromLinkedList(entry *SortableLinkedMapEntry) {
	// update the head and tail if necessary
	if entry == slm.head {
		slm.head = entry.next
	}
	if entry == slm.tail {
		slm.tail = entry.pre
	}
	// take out this entry
	if entry.pre != nil {
		entry.pre.next = entry.next
	}
	if entry.next != nil {
		entry.next.pre = entry.pre
	}
	// if first match entry is removed, find next
	if entry == slm.firstMatchedEntry {
		slm.firstMatchedEntry = slm.findNextMatchedEntry()
	}
}

// Find next match entry after the first match entry
// must be called while holding the lock of map.
func (slm *SortableLinkedMap) findNextMatchedEntry() *SortableLinkedMapEntry {
	if slm.firstMatchedEntry == nil {
		return nil
	}
	lookupEntry := slm.firstMatchedEntry.next
	for {
		if lookupEntry == nil {
			return nil
		} else if lookupEntry.isMatched {
			return lookupEntry
		}
		lookupEntry = lookupEntry.next
	}
}

// Update isMatched state for entry and first matched entry for sortable linked map,
// this method must be called after the entry has already been updated in the linked map,
// required updates should be as below:
// 1. if updated entry is matched
//      1.1 if first matched entry is not present (no matter whether or not this map is sortable)
//          set firstMatchedEntry = updatedEntry
//      1.2 if this map is sortable and updated entry should be placed before first matched entry,
//          set firstMatchedEntry = updatedEntry
// 2. if update entry is not matched
//      2.1 if updated entry is first matched entry (no matter whether or not this map is sortable),
//          set firstMatchedEntry = nextMatchedEntry
func (slm *SortableLinkedMap) updateMatchedState(updatedEntry *SortableLinkedMapEntry) {
	updatedEntry.isMatched = slm.matchFunc(updatedEntry.value)
	if updatedEntry.isMatched {
		// if updated entry is matched
		if slm.firstMatchedEntry == nil {
			// set first matched entry if not present
			slm.firstMatchedEntry = updatedEntry
		} else if slm.compareFunc != nil && slm.compareFunc(updatedEntry.value, slm.firstMatchedEntry.value) {
			// if map is sortable and updated entry should be placed before first matched entry
			slm.firstMatchedEntry = updatedEntry
		}
	} else {
		// if updated entry is not matched
		if updatedEntry == slm.firstMatchedEntry {
			// if updated entry is first matched entry
			slm.firstMatchedEntry = slm.findNextMatchedEntry()
		}
	}
}

func (slm *SortableLinkedMap) Put(key interface{}, value interface{}) interface{} {
	slm.Lock()
	defer slm.Unlock()
	// update the value if entry exists
	if entry, ok := slm.entries[key]; ok {
		oldValue := entry.value
		entry.value = value
		// update matched state
		if slm.matchFunc != nil {
			slm.updateMatchedState(entry)
		}
		return oldValue
	}
	// create a new entry
	newEntry := &SortableLinkedMapEntry{
		key:   key,
		value: value,
	}
	// find the pre entry
	preEntry := slm.findPreEntry(slm.tail, newEntry, true)
	// insert into the linked list
	slm.putIntoLinkedList(preEntry, newEntry)
	// update matched state
	if slm.matchFunc != nil {
		slm.updateMatchedState(newEntry)
	}
	// update entry map
	slm.entries[key] = newEntry
	return nil
}

func (slm *SortableLinkedMap) Remove(key interface{}) interface{} {
	slm.Lock()
	defer slm.Unlock()
	if entry := slm.entries[key]; entry != nil {
		// remove from linked list
		slm.removeFromLinkedList(entry)
		// delete from map
		delete(slm.entries, key)
		return entry.value
	}
	return nil
}

func (slm *SortableLinkedMap) Get(key interface{}) interface{} {
	slm.RLock()
	defer slm.RUnlock()
	if entry := slm.entries[key]; entry != nil {
		return entry.value
	}
	return nil
}

func (slm *SortableLinkedMap) Size() int {
	slm.RLock()
	defer slm.RUnlock()
	return len(slm.entries)
}

func (slm *SortableLinkedMap) getEntryItems(getItemFunc func(*SortableLinkedMapEntry) interface{}) []interface{} {
	slm.RLock()
	defer slm.RUnlock()
	results := make([]interface{}, 0)
	lookupEntry := slm.head
	for {
		if lookupEntry != nil {
			results = append(results, getItemFunc(lookupEntry))
			lookupEntry = lookupEntry.next
		} else {
			break
		}
	}
	return results
}

func (slm *SortableLinkedMap) Values() []interface{} {
	return slm.getEntryItems(func(entry *SortableLinkedMapEntry) interface{} {
		return entry.value
	})
}

func (slm *SortableLinkedMap) Keys() []interface{} {
	return slm.getEntryItems(func(entry *SortableLinkedMapEntry) interface{} {
		return entry.key
	})
}

func (slm *SortableLinkedMap) Reset() {
	slm.Lock()
	defer slm.Unlock()
	slm.entries = make(map[interface{}]*SortableLinkedMapEntry)
	slm.head = nil
	slm.tail = nil
	slm.firstMatchedEntry = nil
}

type SimpleMapIterator struct {
	entries []*SortableLinkedMapEntry
	index   int
}

func NewSimpleMapIterator(entries []*SortableLinkedMapEntry) *SimpleMapIterator {
	return &SimpleMapIterator{
		entries: entries,
		index:   0,
	}
}

func (dri *SimpleMapIterator) HasNext() bool {
	return dri.index < len(dri.entries)
}

func (dri *SimpleMapIterator) Next() (key interface{}, value interface{}) {
	if dri.index >= len(dri.entries) {
		return nil, nil
	}
	obj := dri.entries[dri.index]
	dri.index++
	return obj.key, obj.value
}

func (dri *SimpleMapIterator) Size() int {
	return len(dri.entries)
}
