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

package auth

import (
	"errors"
	"fmt"
)

var (
	// ErrAuthFailure indocates an authentication failure.
	ErrAuthFailure = errors.New("auth: Authentication failure")

	providers = make(map[string]Authenticator)
)

// Authenticator is the interface for an authentication provider.
type Authenticator interface {
	Authenticate(id string, cred interface{}) error
}

// Register registers an authenticator.
func Register(name string, provider Authenticator) {
	if provider == nil {
		panic("auth: Register provide is nil")
	}

	if _, dup := providers[name]; dup {
		panic("auth: Register called twice for provider " + name)
	}

	providers[name] = provider
}

// Unregister unregisters an authenticator.
func Unregister(name string) {
	delete(providers, name)
}

// Manager manages an authenticator.
type Manager struct {
	p Authenticator
}

// NewManager creates a new manager.
func NewManager(providerName string) (*Manager, error) {
	p, ok := providers[providerName]
	if !ok {
		return nil, fmt.Errorf("session: unknown provider %q", providerName)
	}

	return &Manager{p: p}, nil
}

// Authenticate authenticates a user with credentials.
func (m *Manager) Authenticate(id string, cred interface{}) error {
	return m.p.Authenticate(id, cred)
}
