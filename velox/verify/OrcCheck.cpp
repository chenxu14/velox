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
#include <string>
#include <memory>
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

#include "orc/ColumnPrinter.hh"
#include "orc/Exceptions.hh"
#include "OrcHdfsFile.hh"

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
      {"verbose",        no_argument,       &options.verbose, 'v'},
      {0, 0,                                0, 0}
  };

  int c = 0;
  while (c >= 0) {
    int option_index;
    c = getopt_long(argc, argv, "f:k:c:h:p:b:v", options_config, &option_index);
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
  orc::ReaderOptions orcReaderOpts;
  readerOpts.setFileFormat(FileFormat::ORC);

  hdfsFS fs = hdfsConnect(options.host, options.port);
  if (!fs) {
    std::cout << "cannot connect hdfs.\n";
    return -1;
  }

  auto reader = DwrfReader::create(
      std::make_unique<BufferedInput>(
          std::make_shared<HdfsReadFile>(fs, options.test_file),
          readerOpts.getMemoryPool()),
      readerOpts);

  auto stream = std::unique_ptr<orc::InputStream>(
      new orc::HdfsFileInputStream(fs, options.test_file));
  auto orcReader = orc::createReader(std::move(stream), orcReaderOpts);

  auto rowReader = reader->createRowReader();
  auto orcRowReader = orcReader->createRowReader();

  VectorPtr batch;
  auto orcBatch = orcRowReader->createRowBatch(options.batch);

  std::string line;
  auto printer = orc::createColumnPrinter(line, &orcRowReader->getSelectedType());

  unsigned long rows = 0;
  unsigned long orcRows = 0;

  while (true) {
    bool hasNext = rowReader->next(options.batch, batch);
    bool orcHasNext = orcRowReader->next(*orcBatch);
    if (hasNext != orcHasNext) {
      std::cout << "[ERROR] One of the Readers has reach end first." << std::endl;
      break;
    }

    if (!hasNext) {
      break;
    }

    printer->reset(*orcBatch);
    auto rowVector = batch->as<RowVector>();

    rows += rowVector->size();
    orcRows += orcBatch->numElements;
    if (rows != orcRows) {
      std::cout << "[ERROR] Readers do Next return different Elements, VELOX return "
                << batch->size() << ", Apache ORC return " << orcBatch->numElements
                << std::endl;
      break;
    }

    for (int32_t row = 0; row < rowVector->size(); ++row) {
      auto rowStr = rowVector->toString(row);
      line.clear();
      printer->printRow(row);
      if (rowStr != line) {
        int target = rows - rowVector->size() + row;
        std::cout << "[ERROR] Find mismatch on target row " << target << std::endl;
        std::cout << "Velox return: " << std::endl;
        std::cout << rowStr << std::endl;
        std::cout << "Apache ORC return: " << std::endl;
        std::cout << line << std::endl;
        goto exit;
      }
    }
  }

exit:
  if (options.verbose) {
    std::cout << "[INFO] Velox Reader return Rows: " << rows
              << ", Apache ORC Reader return Rows: " << orcRows << std::endl;
  }

  if (fs && hdfsDisconnect(fs)) {
    std::cout << "disconnect from hdfs failed.\n";
  }
  return 0;
}
