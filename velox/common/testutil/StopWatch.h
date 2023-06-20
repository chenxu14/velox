#pragma once

#include "velox/common/testutil/Latency.h"

namespace facebook::velox::common::testutil {

enum class LatencyType {
  READ, DECODE, DECOMPRESSION
};

class StopWatch {
public:
  StopWatch(LatencyType type) : type_(type) {
    startTime_ = common::testutil::Latency::GetInstance().getCurrentTime();
  }

  ~StopWatch() {
    auto& latency = common::testutil::Latency::GetInstance();
    int64_t procssTime = latency.getCurrentTime() - startTime_;
    switch (type_) {
      case LatencyType::READ:
        latency.incReadTime(procssTime);
        break;
      case LatencyType::DECODE:
        latency.incDecodeTime(procssTime);
        break;
      case LatencyType::DECOMPRESSION:
        latency.incDecompressionTime(procssTime);
        break;
      default:
        break;
    }
  }

private:
  LatencyType type_;
  int64_t startTime_;
};

} // facebook::velox::common::testutil
