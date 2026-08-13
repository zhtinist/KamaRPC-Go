package limiter

import "testing"

func TestTokenBucketAllowsUpToRate(t *testing.T) {
	tb := NewTokenBucket(3)

	for i := 0; i < 3; i++ {
		if !tb.Allow() {
			t.Fatalf("request %d should be allowed", i)
		}
	}
	if tb.Allow() {
		t.Fatal("bucket should be empty")
	}
}
