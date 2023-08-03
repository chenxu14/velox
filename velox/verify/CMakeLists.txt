# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(
  apache_orc
  io/InputStream.cc
  io/OutputStream.cc
  wrap/orc-proto-wrapper.cc
  Adaptor.cc
  ByteRLE.cc
  ColumnPrinter.cc
  ColumnReader.cc
  ColumnWriter.cc
  Common.cc
  Compression.cc
  Exceptions.cc
  Int128.cc
  LzoDecompressor.cc
  MemoryPool.cc
  OrcFile.cc
  Reader.cc
  RLEv1.cc
  RLEv2.cc
  RLE.cc
  Statistics.cc
  StripeStream.cc
  Timezone.cc
  TypeImpl.cc
  Vector.cc
  Writer.cc)

add_dependencies(apache_orc velox_dwio_dwrf_proto)

target_link_libraries(
  apache_orc
  velox_common_base
  velox_dwio_common
  velox_common_compression
  velox_dwio_dwrf_proto
  velox_caching
  Snappy::snappy
  zstd::zstd)

add_executable(velox_verify_orc OrcCheck.cpp)
  target_link_libraries(
    velox_verify_orc
    velox_file
    velox_hdfs
    velox_core
    velox_type
    velox_vector
    velox_exec
    velox_hive_connector
    velox_memory
    apache_orc)
