package timeprovider

import (
	"time"
)

// Provider exposes the ability to retrieve the current time.
type Provider interface {
	Now() time.Time
}

// ProviderFunc adapts a function to satisfy the Provider interface.
type ProviderFunc func() time.Time

// Now returns the result of calling the underlying function.
func (f ProviderFunc) Now() time.Time {
	return f()
}

// RealProvider delegates to time.Now.
type RealProvider struct{}

// Now returns the current time using time.Now.
func (RealProvider) Now() time.Time {
	return time.Now()
}

// FixedProvider always returns the provided timestamp.
type FixedProvider struct {
	T time.Time
}

// Now returns the fixed timestamp.
func (f FixedProvider) Now() time.Time {
	return f.T
}
