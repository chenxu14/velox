#include "velox/common/testutil/Latency.h"

namespace facebook::velox::common::testutil {

std::once_flag Latency::once;
std::shared_ptr<Latency> Latency::instance;

Latency& Latency::GetInstance() {
  std::call_once(once, &Latency::CreateSinglten);
  return *instance;
}

void Latency::CreateSinglten() {
  instance = std::shared_ptr<Latency> (new Latency());
}

} // namespace facebook::velox::common::testutil
