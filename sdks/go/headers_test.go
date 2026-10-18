// B17: `TransportConfig.ExtraHeaders` reach the wire on unary calls and on
// server streams, a call's own header replaces one of the same name, and the
// bearer cannot be displaced by either.

package loams

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// headerServer answers every call with an empty message (a stream gets an empty
// body, which is an error the test does not care about) and records the request
// headers of each.
func headerServer(t *testing.T) (string, func() []http.Header) {
	t.Helper()
	var mu sync.Mutex
	var seen []http.Header
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		seen = append(seen, request.Header.Clone())
		mu.Unlock()
		writer.Header().Set("Content-Type", request.Header.Get("Content-Type"))
	}))
	t.Cleanup(server.Close)
	return server.URL, func() []http.Header {
		mu.Lock()
		defer mu.Unlock()
		return append([]http.Header(nil), seen...)
	}
}

func TestExtraHeadersReachTheWire(t *testing.T) {
	endpoint, seen := headerServer(t)
	client, err := New(Options{
		Endpoint:  endpoint,
		Auth:      APIKey("real-key"),
		NoRetries: true,
		Transport: TransportConfig{ExtraHeaders: http.Header{
			// Lower case on purpose: a literal map is not canonicalised.
			"loams-live-app": {"blog"},
			"X-Tenant":       {"acme"},
			"Authorization":  {"Bearer not-the-key"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if _, err := client.LiveAdmin().GetApp(ctx, &GetAppRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.LiveAdmin().GetApp(ctx, &GetAppRequest{}, WithLiveApp("shop")); err != nil {
		t.Fatal(err)
	}
	stream, err := client.Live().Watch(ctx, &WatchRequest{})
	if err == nil {
		for stream.Receive() {
		}
		_ = stream.Close()
	}

	got := seen()
	if len(got) != 3 {
		t.Fatalf("saw %d requests, want 3 (unary, unary with override, stream)", len(got))
	}
	for i, headers := range got {
		if v := headers.Get("X-Tenant"); v != "acme" {
			t.Errorf("request %d: X-Tenant = %q, want acme", i, v)
		}
		if v := headers.Get("Authorization"); v != "Bearer real-key" {
			t.Errorf("request %d: Authorization = %q, the client's header displaced the bearer", i, v)
		}
	}
	if v := got[0].Values(LiveAppHeader); len(v) != 1 || v[0] != "blog" {
		t.Errorf("unary: %s = %q, want [blog]", LiveAppHeader, v)
	}
	if v := got[1].Values(LiveAppHeader); len(v) != 1 || v[0] != "shop" {
		t.Errorf("unary with WithLiveApp: %s = %q, want [shop] (the call's header replaces the client's)", LiveAppHeader, v)
	}
	if v := got[2].Values(LiveAppHeader); len(v) != 1 || v[0] != "blog" {
		t.Errorf("stream: %s = %q, want [blog]", LiveAppHeader, v)
	}
}

// recordingTransport counts the requests that went through it.
type recordingTransport struct {
	mu    sync.Mutex
	count int
}

func (r *recordingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.count++
	r.mu.Unlock()
	return http.DefaultTransport.RoundTrip(request)
}

// TestTransportHTTPClientIsUsed: `TransportConfig.HTTPClient` was overwritten by
// a nil `Options.HTTPClient`, so a caller's client on the transport was ignored.
func TestTransportHTTPClientIsUsed(t *testing.T) {
	endpoint, _ := headerServer(t)
	transport := &recordingTransport{}
	client, err := New(Options{
		Endpoint:  endpoint,
		NoRetries: true,
		Transport: TransportConfig{HTTPClient: &http.Client{Transport: transport}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.LiveAdmin().GetApp(context.Background(), &GetAppRequest{}); err != nil {
		t.Fatal(err)
	}
	if transport.count != 1 {
		t.Errorf("the transport's HTTPClient carried %d requests, want 1", transport.count)
	}
}
