// Links the installed SDK and prints the version it was built as; release.sh
// compares that against the release version.
#include <iostream>

#include "loams/loams.hpp"

int main() {
  std::cout << loams::SdkVersion() << "\n" << loams::UserAgent() << "\n";
  return 0;
}
