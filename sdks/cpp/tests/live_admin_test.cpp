// `cpp_live_admin`: `loams.live.admin`'s app-directory calls (LV1 Task 13).
//
// It is **not** one of the six canonical tests `sdks/conformance/required.mjs`
// names, and it is registered under its own name for the reason
// `cpp_conformance_test_names` is: a seventh test under a six-test name would make
// this language's set differ from every other language's.
//
// What is pinned: the five calls reach `LiveAdminService`; an `app` argument puts
// the `loams-live-app` header on the request and no argument puts none; the
// mutations are never retried and the reads are; `ListApps` pages on `apps` and
// `next_page_token`; and the `live_app_*` reasons are in the registry.

#include "support.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

namespace {

using namespace loams;
using namespace loams_test;

/// The value of a request header, or "<none>" when it is absent.
std::string HeaderOf(const HttpRequest& request, const std::string& name) {
  for (const auto& header : request.headers) {
    if (header.first == name) {
      return header.second;
    }
  }
  return "<none>";
}

ScriptedTransport::Answer Ok(const std::string& json) {
  ScriptedTransport::Answer answer;
  answer.status = 200;
  answer.content_type = "application/json";
  answer.body = json;
  return answer;
}

Options JsonOptions(ScriptedTransport* transport, int max_retries = kDefaultMaxRetries) {
  Options options;
  options.endpoint = "http://127.0.0.1:1";
  // Non-owning, so the stack object outlives the client.
  options.transport = std::shared_ptr<HttpTransport>(transport, [](HttpTransport*) {});
  options.content_type = ContentType::kJson;
  options.max_retries = max_retries;
  return options;
}

}  // namespace

int main() {
  // --- the bindings: five calls, the right retry classes, ListApps paged --------------
  {
    const std::string admin = "loams.live.v1.LiveAdminService/";
    for (const std::string name : {"CreateApp", "UpdateApp", "DeleteApp"}) {
      LOAMS_CHECK(BindingFor(admin + name).retry_class == RetryClass::kManual,
                  name + " is a mutation with no key, so it is never retried");
      LOAMS_CHECK(BindingFor(admin + name).idempotency_field.empty(), name + " takes no idempotency key");
    }
    for (const std::string name : {"GetApp", "ListApps"}) {
      LOAMS_CHECK(BindingFor(admin + name).retry_class == RetryClass::kSafe, name + " is retry-safe");
    }
    LOAMS_CHECK_EQ(BindingFor(admin + "ListApps").items_field, std::string("apps"), "ListApps pages on apps");
    LOAMS_CHECK_EQ(BindingFor(admin + "ListApps").next_page_token_field, std::string("next_page_token"),
                   "and on next_page_token");
    LOAMS_CHECK_EQ(std::string(kLiveAppHeader), std::string("loams-live-app"), "the header's name");
  }

  // --- each call reaches the admin service; the app is a header ------------------------
  {
    ScriptedTransport transport;
    for (int at = 0; at < 5; ++at) {
      transport.AddAnswer(Ok("{}"));
    }
    std::shared_ptr<Loams> loams = MakeLoams(JsonOptions(&transport));
    LiveAdminModule* admin = loams->LiveAdmin();

    live::v1::CreateAppRequest create;
    live::v1::CreateAppResponse created;
    admin->CreateApp(&create, &created);
    live::v1::ListAppsRequest list;
    live::v1::ListAppsResponse listed;
    admin->ListApps(&list, &listed);
    live::v1::GetAppRequest get;
    live::v1::GetAppResponse got;
    admin->GetApp(&get, &got, "blog");
    live::v1::UpdateAppRequest update;
    live::v1::UpdateAppResponse updated;
    admin->UpdateApp(&update, &updated, "blog");
    live::v1::DeleteAppRequest remove;
    live::v1::DeleteAppResponse removed;
    admin->DeleteApp(&remove, &removed, "blog");

    const std::vector<HttpRequest> requests = transport.Requests();
    LOAMS_REQUIRE(requests.size() == 5, "five calls, five requests");
    const std::vector<std::string> names = {"CreateApp", "ListApps", "GetApp", "UpdateApp", "DeleteApp"};
    for (std::size_t at = 0; at < names.size(); ++at) {
      LOAMS_CHECK(requests[at].url.find("/loams.live.v1.LiveAdminService/" + names[at]) != std::string::npos,
                  names[at] + " should reach LiveAdminService, went to " + requests[at].url);
    }
    LOAMS_CHECK_EQ(HeaderOf(requests[0], kLiveAppHeader), std::string("<none>"), "no app, no header");
    LOAMS_CHECK_EQ(HeaderOf(requests[1], kLiveAppHeader), std::string("<none>"), "no app, no header");
    for (std::size_t at = 2; at < 5; ++at) {
      LOAMS_CHECK_EQ(HeaderOf(requests[at], kLiveAppHeader), std::string("blog"), names[at] + "'s app header");
    }
  }

  // --- a create is not retried; a get is ------------------------------------------------
  {
    ScriptedTransport transport;
    ScriptedTransport::Answer down;
    down.status = 503;
    down.content_type = "application/json";
    down.body = R"({"code":"unavailable","message":"the node is restarting"})";
    transport.AddAnswer(down);
    std::shared_ptr<Loams> loams = MakeLoams(JsonOptions(&transport));
    live::v1::CreateAppRequest request;
    live::v1::CreateAppResponse response;
    LOAMS_CHECK_THROWS(loams->LiveAdmin()->CreateApp(&request, &response), UnavailableError,
                       "an unavailable create");
    LOAMS_CHECK_EQ(transport.Requests().size(), std::size_t{1}, "a create is sent once");

    ScriptedTransport reads;
    reads.AddAnswer(down);
    reads.AddAnswer(Ok("{}"));
    std::shared_ptr<Loams> reader = MakeLoams(JsonOptions(&reads));
    live::v1::GetAppRequest get;
    live::v1::GetAppResponse got;
    reader->LiveAdmin()->GetApp(&get, &got);
    LOAMS_CHECK_EQ(reads.Requests().size(), std::size_t{2}, "a read is retried once after an unavailable");
  }

  // --- ListApps pages on `apps` and `next_page_token`, with the app on every page ------
  {
    ScriptedTransport transport;
    transport.AddAnswer(Ok(R"({"apps":[{"name":"a"},{"name":"b"}],"nextPageToken":"p2"})"));
    transport.AddAnswer(Ok(R"({"apps":[{"name":"c"}]})"));
    std::shared_ptr<Loams> loams = MakeLoams(JsonOptions(&transport));

    PageIterator iterator = PageIterator::For(
        BindingFor("loams.live.v1.LiveAdminService/ListApps"),
        [&loams](const google::protobuf::Message& request, google::protobuf::Message* response) {
          live::v1::ListAppsRequest typed;
          typed.CopyFrom(request);
          loams->LiveAdmin()->ListApps(&typed, static_cast<live::v1::ListAppsResponse*>(response), "blog");
        },
        std::make_unique<live::v1::ListAppsRequest>(), std::make_unique<live::v1::ListAppsResponse>());

    std::vector<std::string> names;
    live::v1::App app;
    while (iterator.Next(&app)) {
      names.push_back(app.name());
    }
    LOAMS_CHECK(!iterator.Error(), "the iteration should not have failed");
    LOAMS_CHECK_EQ(names.size(), std::size_t{3}, "three apps over two pages");
    LOAMS_CHECK_EQ(transport.Requests().size(), std::size_t{2}, "two page requests");
    for (const HttpRequest& request : transport.Requests()) {
      LOAMS_CHECK_EQ(HeaderOf(request, kLiveAppHeader), std::string("blog"), "the app on every page");
    }
  }

  // --- the reasons ------------------------------------------------------------------------
  {
    LOAMS_CHECK(ReasonFromString("live_app_disabled") == Reason::kLiveAppDisabled, "live_app_disabled");
    LOAMS_CHECK(ReasonFromString("live_app_exists") == Reason::kLiveAppExists, "live_app_exists");
    LOAMS_CHECK(ReasonFromString("live_app_mismatch") == Reason::kLiveAppMismatch, "live_app_mismatch");
  }

  return Finish("cpp_live_admin");
}
