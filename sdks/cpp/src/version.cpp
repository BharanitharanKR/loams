// What this SDK speaks, as `version.hpp` documents it.

#include "loams/version.hpp"

// CMakeLists.txt passes LOAMS_SDK_VERSION, the one place the version is written
// (sdks/tools/sdk-version.py sets and checks it). A build that does not go through
// that CMakeLists.txt still compiles, and says so in the version it reports.
#ifndef LOAMS_SDK_VERSION
#define LOAMS_SDK_VERSION "0.0.0-unknown"
#endif

namespace loams {

const std::string& ProtoRev() {
  static const std::string* const rev = new std::string(kProtoRev);
  return *rev;
}

const std::vector<std::string>& ProtoPackages() {
  // Every package the committed stubs cover, in registry order. This is the list
  // R9's report compares the server's `api_versions` against, and it is a
  // constant because it is a property of the SDK rather than of the server.
  static const std::vector<std::string>* const packages = new std::vector<std::string>{
      "loams.approvals.v1", "loams.devices.v1",  "loams.errors.v1",         "loams.instance.v1",
      "loams.live.v1",       "loams.notifications.v1", "loams.operations.v1", "loams.options.v1",
  };
  return *packages;
}

const std::string& UserAgent() {
  static const std::string* const agent = new std::string("loams-cpp/" LOAMS_SDK_VERSION " (Connect over HTTP)");
  return *agent;
}

const std::string& SdkVersion() {
  static const std::string* const version = new std::string(LOAMS_SDK_VERSION);
  return *version;
}

}  // namespace loams