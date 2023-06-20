/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "orc/OrcFile.hh"

#include "Adaptor.hh"
#include "orc/Exceptions.hh"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <hdfs/hdfs.h>

namespace orc {

class HdfsFileInputStream : public InputStream {
 private:
  hdfsFS file_system;
  std::string filename;
  hdfsFile file;
  uint64_t totalLength;

 public:
  HdfsFileInputStream(hdfsFS fs, std::string _filename)
      : file_system(fs), filename(_filename) {
    file = hdfsOpenFile(file_system, filename.c_str(), O_RDONLY, 0, 0, 0);
    if (!file) {
      throw ParseError("Can't open " + filename + ". ");
    }
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(file_system, filename.c_str());
    if (!fileInfo) {
      throw ParseError("Can't stat " + filename + ". ");
    }
    totalLength = fileInfo->mSize;
    hdfsFreeFileInfo(fileInfo, 1);
  }

  ~HdfsFileInputStream() override;

  uint64_t getLength() const override {
    return totalLength;
  }

  uint64_t getNaturalReadSize() const override {
    return 128 * 1024;
  }

  void read(void* buf, uint64_t length, uint64_t offset) override {
    if (!buf) {
      throw ParseError("Buffer is null");
    }
    int rc = hdfsSeek(file_system, file, offset);
    if (rc < 0) {
      throw ParseError("do seek error.");
    }

    char* pos = (char*)buf;
    uint64_t totalBytesRead = 0;
    while (totalBytesRead < length) {
      auto bytesRead = hdfsRead(file_system, file, pos, length - totalBytesRead);
      VELOX_CHECK(bytesRead >= 0, "Read failure in HdfsFileInputStream::read.")
      totalBytesRead += bytesRead;
      pos += bytesRead;
    }
  }

  const std::string& getName() const override {
    return filename;
  }
};

HdfsFileInputStream::~HdfsFileInputStream() {
  if (file) {
    hdfsCloseFile(file_system, file);
  }
}

} // namespace orc
