// Copyright (c) 2014 The SurgeMQ Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sessions

import (
	"fmt"
	"sync"
)

func init() {
	Register("mem", NewMemProvider())
}

// MemProvider is a memory backed provider.
type MemProvider struct {
	st map[string]*Session
	mu sync.RWMutex
}

// NewMemProvider creates a MemProvider.
func NewMemProvider() *MemProvider {
	return &MemProvider{
		st: make(map[string]*Session),
	}
}

// New creates a new session.
func (mp *MemProvider) New(id string) (*Session, error) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	mp.st[id] = &Session{id: id}
	return mp.st[id], nil
}

// Get returns a specific session.
func (mp *MemProvider) Get(id string) (*Session, error) {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	sess, ok := mp.st[id]
	if !ok {
		return nil, fmt.Errorf("store/Get: No session found for key %s", id)
	}

	return sess, nil
}

// Del deletes a spcific session.
func (mp *MemProvider) Del(id string) {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	delete(mp.st, id)
}

// Save persists a session.
func (mp *MemProvider) Save(id string) error {
	return nil
}

// Count returns the number of sessions.
func (mp *MemProvider) Count() int {
	return len(mp.st)
}

// Close releases all resources.
func (mp *MemProvider) Close() error {
	mp.st = make(map[string]*Session)
	return nil
}
