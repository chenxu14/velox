#pragma once

#include <cpuid.h>
#include <atomic>
#include <string>
#include <sys/time.h>
#include <iostream>
#include <mutex>
#include <thread>

namespace facebook::velox::common::testutil {

class Latency {
public:
  static Latency& GetInstance();
  static void CreateSinglten();

  Latency() {}
  ~Latency() {}
  Latency(const Latency &) = delete;
  Latency &operator=(const Latency &) = delete;

  void incSeekCount() {
    seekCount++;
  }

  int getSeekCount() {
    return seekCount.load();
  }

  void incReadCount() {
    readCount++;
  }

  int getReadCount() {
    return readCount.load();
  }

  void incReadTime(int64_t value) {
    readTime += value;
  }

  int64_t getReadTime() {
    return readTime.load();
  }

  void incDecodeTime(int64_t value) {
    decodeTime += value;
  }

  int64_t getDecodeTime() {
    return decodeTime.load();
  }

  void incDecompressionTime(int64_t value) {
    decompressionTime += value;
  }

  int64_t getDecompressionTime() {
    return decompressionTime.load();
  }

  int64_t getCurrentTime() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
  }

private:
  static std::once_flag once;
  static std::shared_ptr<Latency> instance;
  std::atomic_int seekCount = 0;
  std::atomic_int readCount = 0;
  std::atomic_int readTime = 0;
  std::atomic_int decodeTime = 0;
  std::atomic_int decompressionTime = 0;
};

} // namespace facebook::velox::common::testutil
