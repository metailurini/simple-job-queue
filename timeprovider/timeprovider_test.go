package timeprovider

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFixedProviderNow(t *testing.T) {
	fixed := time.Date(2024, time.January, 15, 8, 30, 0, 123456000, time.UTC)
	provider := FixedProvider{T: fixed}

	got := provider.Now()
	assert.True(t, got.Equal(fixed), "Now() = %v, want %v", got, fixed)
}
