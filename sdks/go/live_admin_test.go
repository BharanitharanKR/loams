// The app-directory calls of `loams.live.admin` (LV1 Task 13): the five calls
// reach `LiveAdminService`, `WithLiveApp` puts the app on the wire, and a refusal
// carries its `live_app_*` reason.

package loams

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"loams.dev/go/gen/facade"
	livev1 "loams.dev/go/gen/loams/live/v1"
)

type seenRequest struct {
	path string
	app  string
}

// adminServer answers every call with an empty message in the encoding it was
// sent in, and records the path and the app header.
func adminServer(t *testing.T) (*Client, func() []seenRequest) {
	t.Helper()
	var mu sync.Mutex
	var seen []seenRequest
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		seen = append(seen, seenRequest{request.URL.Path, request.Header.Get(LiveAppHeader)})
		mu.Unlock()
		contentType := request.Header.Get("Content-Type")
		writer.Header().Set("Content-Type", contentType)
		if contentType == "application/json" {
			_, _ = writer.Write([]byte("{}"))
		}
	}))
	t.Cleanup(server.Close)
	client, err := New(Options{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	return client, func() []seenRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]seenRequest(nil), seen...)
	}
}

func TestLiveAdminAppCalls(t *testing.T) {
	t.Run("the catalogue names the five calls with the right retry classes", func(t *testing.T) {
		module, ok := facade.Module("live.admin")
		if !ok {
			t.Fatal("no live.admin module")
		}
		if module.Service != "loams.live.v1.LiveAdminService" || !module.Unstable {
			t.Errorf("module = %+v", module)
		}
		want := map[string]facade.RetryClass{
			"CreateApp": facade.RetryManual, "DeleteApp": facade.RetryManual, "UpdateApp": facade.RetryManual,
			"GetApp": facade.RetrySafe, "ListApps": facade.RetrySafe,
		}
		if len(module.Calls) != len(want) {
			t.Fatalf("calls = %d, want %d", len(module.Calls), len(want))
		}
		for _, call := range module.Calls {
			if call.Retry != want[call.Name] {
				t.Errorf("%s retry = %s", call.Name, call.Retry)
			}
			if call.RPC != "loams.live.v1.LiveAdminService/"+call.Method {
				t.Errorf("%s rpc = %s", call.Name, call.RPC)
			}
			if call.TakesIdempotencyKey {
				t.Errorf("%s takes an idempotency key", call.Name)
			}
		}
		listApps, _ := facade.Binding("live.admin", "ListApps")
		if listApps.Pagination == nil || listApps.Pagination.ItemsField != "Apps" {
			t.Errorf("ListApps pagination = %+v", listApps.Pagination)
		}
	})

	t.Run("each call reaches the admin service and WithLiveApp sets the header", func(t *testing.T) {
		client, seen := adminServer(t)
		ctx := context.Background()
		admin := client.LiveAdmin()
		if _, ok := client.Module("live.admin"); !ok {
			t.Error("the client's module catalogue has no live.admin")
		}
		if _, err := admin.CreateApp(ctx, &CreateAppRequest{}); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.ListApps(ctx, &ListAppsRequest{}); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.GetApp(ctx, &GetAppRequest{}, WithLiveApp("blog")); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.UpdateApp(ctx, &UpdateAppRequest{}, WithLiveApp("blog")); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.DeleteApp(ctx, &DeleteAppRequest{}, WithLiveApp("blog")); err != nil {
			t.Fatal(err)
		}
		got := seen()
		names := []string{"CreateApp", "ListApps", "GetApp", "UpdateApp", "DeleteApp"}
		if len(got) != len(names) {
			t.Fatalf("saw %d requests: %+v", len(got), got)
		}
		for i, name := range names {
			if want := "/loams.live.v1.LiveAdminService/" + name; got[i].path != want {
				t.Errorf("request %d path = %s, want %s", i, got[i].path, want)
			}
		}
		if got[0].app != "" || got[1].app != "" {
			t.Errorf("no option, no header: %+v", got[:2])
		}
		for _, request := range got[2:] {
			if request.app != "blog" {
				t.Errorf("%s carries app %q", request.path, request.app)
			}
		}
	})

	t.Run("the live_app reasons are in the registry", func(t *testing.T) {
		for _, reason := range []Reason{facade.ReasonLiveAppDisabled, facade.ReasonLiveAppExists, facade.ReasonLiveAppMismatch} {
			if !facade.IsKnownReason(reason) {
				t.Errorf("%s is not a known reason", reason)
			}
		}
	})
}

// TestLiveAdminListAppsPaginates follows `next_page_token` through the real
// binding (R6), sending the app header on every page.
func TestLiveAdminListAppsPaginates(t *testing.T) {
	pages := map[string]*ListAppsResponse{
		"":   {Apps: []*livev1.App{{Name: "a"}, {Name: "b"}}, NextPageToken: "p2"},
		"p2": {Apps: []*livev1.App{{Name: "c"}}},
	}
	var mu sync.Mutex
	var apps []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		apps = append(apps, request.Header.Get(LiveAppHeader))
		mu.Unlock()
		body := make([]byte, 1<<16)
		n, _ := request.Body.Read(body)
		contentType := request.Header.Get("Content-Type")
		var in ListAppsRequest
		if contentType == "application/json" {
			_ = protojson.Unmarshal(body[:n], &in)
		} else {
			_ = proto.Unmarshal(body[:n], &in)
		}
		writer.Header().Set("Content-Type", contentType)
		var out []byte
		if contentType == "application/json" {
			out, _ = protojson.Marshal(pages[in.GetPageToken()])
		} else {
			out, _ = proto.Marshal(pages[in.GetPageToken()])
		}
		_, _ = writer.Write(out)
	}))
	t.Cleanup(server.Close)
	client, err := New(Options{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	items, err := PaginateCall[ListAppsRequest, ListAppsResponse, *livev1.App](
		client, "live.admin", "ListApps", client.LiveAdmin().ListApps,
		context.Background(), &ListAppsRequest{PageSize: 2}, WithLiveApp("blog"),
	)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for app := range items.Seq() {
		names = append(names, app.GetName())
	}
	if err := items.Err(); err != nil {
		t.Fatal(err)
	}
	if len(names) != 3 || names[0] != "a" || names[2] != "c" {
		t.Errorf("names = %v", names)
	}
	if len(apps) != 2 || apps[0] != "blog" || apps[1] != "blog" {
		t.Errorf("app headers = %v", apps)
	}
}
