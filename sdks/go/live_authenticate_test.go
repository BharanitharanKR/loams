// `loams.live.authenticate` (LV1 Task 17): the typed call reaches
// `LiveService/Authenticate`, carries the client's credential as the bearer,
// and surfaces the identity version the server answers with, so a caller tells
// an extended session (same version) from a changed identity (a bumped one).

package loams

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"loams.dev/go/gen/facade"
)

type authSeen struct {
	path    string
	bearer  string
	session string
}

// authenticateServer answers Authenticate with the next identity in `answers`
// and records the path, the bearer and the session id.
func authenticateServer(t *testing.T, token string, answers []uint64) (*Client, func() []authSeen) {
	t.Helper()
	var mu sync.Mutex
	var seen []authSeen
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		contentType := request.Header.Get("Content-Type")
		var in AuthenticateRequest
		if contentType == "application/json" {
			_ = protojson.Unmarshal(body, &in)
		} else {
			_ = proto.Unmarshal(body, &in)
		}
		mu.Lock()
		seen = append(seen, authSeen{request.URL.Path, request.Header.Get("Authorization"), in.GetSessionId()})
		out := &AuthenticateResponse{Identity: answers[len(seen)-1]}
		mu.Unlock()
		writer.Header().Set("Content-Type", contentType)
		var payload []byte
		if contentType == "application/json" {
			payload, _ = protojson.Marshal(out)
		} else {
			payload, _ = proto.Marshal(out)
		}
		_, _ = writer.Write(payload)
	}))
	t.Cleanup(server.Close)
	client, err := New(Options{Endpoint: server.URL, Auth: StaticToken(token)})
	if err != nil {
		t.Fatal(err)
	}
	return client, func() []authSeen {
		mu.Lock()
		defer mu.Unlock()
		return append([]authSeen(nil), seen...)
	}
}

func TestLiveAuthenticate(t *testing.T) {
	t.Run("the catalogue has the call, unary and never retried", func(t *testing.T) {
		call, ok := facade.Binding("live", "Authenticate")
		if !ok {
			t.Fatal("live has no Authenticate")
		}
		if call.RPC != "loams.live.v1.LiveService/Authenticate" || call.Retry != facade.RetryManual ||
			call.Streaming != facade.Unary || call.TakesIdempotencyKey {
			t.Errorf("binding = %+v", call)
		}
	})

	t.Run("it sends the session and the client's bearer, and surfaces both outcomes", func(t *testing.T) {
		// 4 is the version the session holds: the same caller answers 4 again
		// (the expiry moved), another caller answers 5 (the identity changed).
		client, seen := authenticateServer(t, "tok-bob", []uint64{4, 5})
		ctx := context.Background()
		extended, err := client.Live().Authenticate(ctx, &AuthenticateRequest{SessionId: "s1"})
		if err != nil {
			t.Fatal(err)
		}
		changed, err := client.Live().Authenticate(ctx, &AuthenticateRequest{SessionId: "s1"})
		if err != nil {
			t.Fatal(err)
		}
		if extended.GetIdentity() != 4 || changed.GetIdentity() != 5 {
			t.Errorf("identities = %d, %d; want 4, 5", extended.GetIdentity(), changed.GetIdentity())
		}
		got := seen()
		if len(got) != 2 {
			t.Fatalf("saw %d requests", len(got))
		}
		for _, request := range got {
			if request.path != "/loams.live.v1.LiveService/Authenticate" || request.bearer != "Bearer tok-bob" || request.session != "s1" {
				t.Errorf("request = %+v", request)
			}
		}
	})

	t.Run("the rotation and revocation reasons are known", func(t *testing.T) {
		for _, reason := range []Reason{facade.ReasonSessionRotate, facade.ReasonTokenExpired, facade.ReasonTokenRevoked} {
			if !facade.IsKnownReason(reason) {
				t.Errorf("%s is not a known reason", reason)
			}
		}
	})
}
