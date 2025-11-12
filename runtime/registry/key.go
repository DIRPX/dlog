/*
   Copyright 2025 The DIRPX Authors.

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

package registry

import "fmt"

// Key identifies a runtime plugin within a domain (Kind) under a Name.
// Examples:
//
//	{Kind: "sink",   Name: "stdout"}
//	{Kind: "stage",  Name: "filter.level"}
//	{Kind: "provider", Name: "static"}
type Key struct {
	Kind string
	Name string
}

// IsZero reports whether the key is incomplete.
func (k Key) IsZero() bool { return k.Kind == "" || k.Name == "" }

// String returns a human-readable representation "kind/name".
func (k Key) String() string {
	switch {
	case k.Kind == "" && k.Name == "":
		return "<empty>"
	case k.Kind == "":
		return fmt.Sprintf("<unknown>/%s", k.Name)
	case k.Name == "":
		return fmt.Sprintf("%s/<unknown>", k.Kind)
	default:
		return k.Kind + "/" + k.Name
	}
}
