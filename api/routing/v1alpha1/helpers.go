/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

// EPPGRPCPort is the ext-proc port the EPP serves and InferencePools
// reference in their endpointPickerRef.
const EPPGRPCPort int32 = 9002

// EPPServiceName is the deterministic name of a Router's EPP Service.
// ModelServers derive it to set the pool's endpointPickerRef.
func EPPServiceName(router string) string { return router + "-epp" }
