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

#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

#include <folly/init/Init.h>
#include <algorithm>

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwrf;
using exec::test::HiveConnectorTestBase;

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  if (argc < 3) {
    std::cout << "velox_example_orc_local_query {file} {sql}\n";
    return 1;
  }

  auto pool = memory::addDefaultLeafMemoryPool();
  std::string filePath{argv[1]};
  ReaderOptions readerOpts{pool.get()};
  readerOpts.setFileFormat(FileFormat::ORC);
  auto reader = DwrfReader::create(
      std::make_unique<BufferedInput>(
          std::make_shared<LocalReadFile>(filePath),
          readerOpts.getMemoryPool()),
          readerOpts);
  auto inputRowType = reader->rowType();

  auto& footer = reader->getFooter();
  std::string format = (footer.format() == DwrfFormat::kDwrf ? "DWRF" : "ORC");
  std::cout << "[file format] " << format << "\n";
  if (format == "ORC") { // orc_proto.pb.h
    auto orcFooter = reinterpret_cast<const proto::orc::Footer*>(footer.rawProtoPtr());
    if (orcFooter->has_softwareversion()) {
      std::cout << "[software version]" << orcFooter->softwareversion();
    }
    for (int i = 0; i < footer.typesSize(); i++) {
      TypeKind kind = footer.types(i).kind();
      std::cout << "[column_" << i << "] " << mapTypeKindToName(kind) << "\n";
    }
  }

  const std::string kHiveConnectorId = "test-hive";
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, nullptr);
  connector::registerConnector(hiveConnector);

  filesystems::registerLocalFileSystem();
  dwrf::registerOrcReaderFactory();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  core::PlanNodeId scanNodeId;
  auto readPlanFragment = exec::test::PlanBuilder()
      .tableScan(inputRowType)
      .capturePlanNodeId(scanNodeId)
      .planFragment();

  // Create the reader task.
  auto readTask = exec::Task::create(
      "my_read_task",
      readPlanFragment,
      /*destination=*/0,
      std::make_shared<core::QueryCtx>(executor.get()));

  auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
      kHiveConnectorId,
      "file:" + filePath,
      dwio::common::FileFormat::ORC);
  readTask->addSplit(scanNodeId, exec::Split{connectorSplit});
  readTask->noMoreSplits(scanNodeId);

  exec::test::DuckDbQueryRunner duckDb;
  // TODO chenxu14 replace with DwrfRowReader::next(500, batch)
  std::vector<RowVectorPtr> data{readTask->next()};
  duckDb.createTable("test", data);
  exec::test::DuckDBQueryResult queryRes = duckDb.execute(argv[2]);
  std::cout << queryRes->ToString();
  return 0;
}
