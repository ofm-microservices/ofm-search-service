package elasticsearch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClaimEventTreatsConflictAsDuplicate(t *testing.T) {
	claims := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims++
		if claims == 1 {
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()

	cl, err := newClient(Config{URL: server.URL, Index: "gigs"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := cl.claimEvent(context.Background(), "event-1")
	if err != nil || !claimed {
		t.Fatalf("first claim = %v, %v", claimed, err)
	}
	claimed, err = cl.claimEvent(context.Background(), "event-1")
	if err != nil || claimed {
		t.Fatalf("duplicate claim = %v, %v", claimed, err)
	}
}
