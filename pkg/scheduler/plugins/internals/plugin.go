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

import "github.com/apache/incubator-yunikorn-core/pkg/interfaces"

const (
	InternalAppsRequestsPluginName = "InternalAppsRequestsPlugin"
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
