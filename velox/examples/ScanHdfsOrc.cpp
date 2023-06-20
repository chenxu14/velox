/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/init/Init.h>
#include <algorithm>
#include <iostream>
#include <getopt.h>
#include <inttypes.h>
#include <unistd.h>
#include "hdfs/hdfs.h"

#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/Latency.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/BaseVector.h"
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsReadFile.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwrf;

struct {
  const char* test_file = NULL;
  const char* krb5 = NULL;
  const char* conf = NULL;
  const char* host = "default";
  int port = 0;
  int batch = 1024;
  int iter = 1;
  int verbose = false;
} options;

void print_usage() {
  printf("benchmark -d TESTDIR -k KRB5_FILE -c CONF_FILE\n"
      "  -f, --test_file      the ORC file that scan with\n"
      "  -k, --krb5_file      the krb5.conf file's path\n"
      "  -c, --conf_file      the hdfs-site.xml's path\n"
      "  -h, --host           the service host\n"
      "  -p, --port           the service port\n"
      "  -b, --batch          number of rows per next operation\n"
      "  -i, --iter           number of iterators\n"
      "  -v, --verbose        verbose output\n");
}

void parse_options(int argc, char *argv[]) {
  static struct option options_config[] = {
      {"test_file",      required_argument, 0,                'f'},
      {"krb5_file",      required_argument, 0,                'k'},
      {"conf_file",      required_argument, 0,                'c'},
      {"host",           optional_argument, 0,                'h'},
      {"port",           optional_argument, 0,                'p'},
      {"batch",          optional_argument, 0,                'b'},
      {"iter",           optional_argument, 0,                'i'},
      {"verbose",        no_argument,       &options.verbose, 'v'},
      {0, 0,                                0, 0}
  };

  int c = 0;
  while (c >= 0) {
    int option_index;
    c = getopt_long(argc, argv, "f:k:c:h:p:b:i:v", options_config, &option_index);
    switch (c) {
      case 'f':
        options.test_file = optarg;
        break;
      case 'k':
        options.krb5 = optarg;
        break;
      case 'c':
        options.conf = optarg;
        break;
      case 'h':
        options.host = optarg;
        break;
      case 'p':
        options.port = atoi(optarg);
        break;
      case 'b':
        options.batch = atoi(optarg);
        break;
      case 'i':
        options.iter = atoi(optarg);
        break;
      case 'v':
        options.verbose = true;
        break;
      default:
        break;
    }
  }

  if (options.test_file == NULL || options.krb5 == NULL || options.conf == NULL) {
    print_usage();
    exit(1);
  }
}

int main(int argc, char** argv) {
  parse_options(argc, argv);
  setenv("LIBHDFS3_CONF", options.conf, 1);
  setenv("KRB5_CONFIG", options.krb5, 1);
  if (options.verbose) {
    std::cout << "[krb5] " << options.krb5 << "\n";
    std::cout << "[conf] " << options.conf << "\n";
    std::cout << "[host] " << options.host << "\n";
  }

  dwrf::registerDwrfReaderFactory();
  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  ReaderOptions readerOpts{pool.get()};
  readerOpts.setFileFormat(FileFormat::ORC);
  RowReaderOptions rowReaderOptions;
  VectorPtr batch;

  hdfsFS fs = hdfsConnect(options.host, options.port);
  if (!fs) {
    std::cout << "cannot connect hdfs.\n";
    return -1;
  }

  auto& latency = common::testutil::Latency::GetInstance();
  int64_t start = latency.getCurrentTime();
  for (int i = 0; i < options.iter; i++) {
    auto reader = DwrfReader::create(
        std::make_unique<BufferedInput>(
            std::make_shared<HdfsReadFile>(fs, options.test_file),
            readerOpts.getMemoryPool()),
        readerOpts);
    auto rowReader = reader->createRowReader(rowReaderOptions);

    unsigned long rows = 0;
    unsigned long batches = 0;
    while (rowReader->next(options.batch, batch)) {
      batches += 1;
      rows += batch->size();
      if (options.verbose) {
        auto rowVector = batch->as<RowVector>();
        for (vector_size_t row = 0; row < rowVector->size(); ++row) {
          std::cout << rowVector->toString(row) << std::endl;
        }
      }
    }

    if (options.verbose) {
      std::cout << "Rows: " << rows << ", Batches: " << batches << std::endl;
    }
  }

#ifdef VELOX_ENABLE_TRACE
  int64_t procssTime = latency.getCurrentTime() - start;
  std::cout << "scan " << options.test_file << " " << options.iter
            << " times use " << procssTime << " ms, read hdfs use "
            << latency.getReadTime() << " ms, decoding data use "
            << latency.getDecodeTime() << " ms, decompression use "
            << latency.getDecompressionTime() << " ms.\n";
#endif

  if (fs && hdfsDisconnect(fs)) {
    std::cout << "disconnect from hdfs failed.\n";
  }
  return 0;
}
