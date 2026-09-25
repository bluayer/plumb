/*
Copyright The Plumb Authors.

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

package main

import "testing"

func TestCheckModelURL(t *testing.T) {
	for url, ok := range map[string]bool{
		"https://api.typesafe.ai":                    true,
		"http://jev.models.svc:8080":                 true,
		"http://jev.models.svc.cluster.local":        true,
		"http://127.0.0.1:8080":                      true,
		"http://localhost:8080":                      true,
		"http://api.typesafe.ai":                     false, // the API key would travel in clear text
		"http://models.example.com.svc.evil.example": false,
		"ftp://x":   false,
		"not a url": false,
	} {
		if err := checkModelURL(url); (err == nil) != ok {
			t.Errorf("checkModelURL(%q) = %v, want ok=%v", url, err, ok)
		}
	}
}
