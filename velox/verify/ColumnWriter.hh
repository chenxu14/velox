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

#ifndef ORC_COLUMN_WRITER_HH
#define ORC_COLUMN_WRITER_HH

#include "orc/Vector.hh"

#include "ByteRLE.hh"
#include "Compression.hh"
#include "orc/Exceptions.hh"
#include "Statistics.hh"

#include "wrap/orc-proto-wrapper.hh"

namespace orc {

  class StreamsFactory {
  public:
    virtual ~StreamsFactory();

    /**
     * Get the stream for the given column/kind in this stripe.
     * @param kind the kind of the stream
     * @return the buffer output stream
     */
    virtual std::unique_ptr<BufferedOutputStream>
                    createStream(facebook::velox::dwrf::proto::orc::Stream_Kind kind) const = 0;
  };

  std::unique_ptr<StreamsFactory> createStreamsFactory(
                                        const WriterOptions& options,
                                        OutputStream * outStream);

  /**
   * record stream positions for row index
   */
  class RowIndexPositionRecorder : public PositionRecorder {
  public:
    virtual ~RowIndexPositionRecorder() override;

    RowIndexPositionRecorder(facebook::velox::dwrf::proto::orc::RowIndexEntry& entry):
      rowIndexEntry(entry) {}

    virtual void add(uint64_t pos) override {
      rowIndexEntry.add_positions(pos);
    }

  private:
    facebook::velox::dwrf::proto::orc::RowIndexEntry& rowIndexEntry;
  };

  /**
   * The interface for writing ORC data types.
   */
  class ColumnWriter {
  protected:
    std::unique_ptr<ByteRleEncoder> notNullEncoder;
    uint64_t columnId;
    std::unique_ptr<MutableColumnStatistics> colIndexStatistics;
    std::unique_ptr<MutableColumnStatistics> colStripeStatistics;
    std::unique_ptr<MutableColumnStatistics> colFileStatistics;

    bool enableIndex;
    // row index for this column, contains all RowIndexEntries in 1 stripe
    std::unique_ptr<facebook::velox::dwrf::proto::orc::RowIndex> rowIndex;
    std::unique_ptr<facebook::velox::dwrf::proto::orc::RowIndexEntry> rowIndexEntry;
    std::unique_ptr<RowIndexPositionRecorder> rowIndexPosition;

  public:
    ColumnWriter(const Type& type, const StreamsFactory& factory,
                 const WriterOptions& options);

    virtual ~ColumnWriter();

    /**
     * Write the next group of values from this rowBatch.
     * @param rowBatch the row batch data to write
     * @param offset the starting point of row batch to write
     * @param numValues the number of values to write
     */
    virtual void add(ColumnVectorBatch& rowBatch,
                     uint64_t offset,
                     uint64_t numValues);
    /**
     * Flush column writer output steams
     * @param streams vector to store generated stream by flush()
     */
    virtual void flush(std::vector<facebook::velox::dwrf::proto::orc::Stream>& streams);

    /**
     * Get estimated sized of buffer used
     */
    virtual uint64_t getEstimatedSize() const;

    /**
     * Get the encoding used by the writer for this column.
     * ColumnEncoding info is pushed into the vector
     */
    virtual void getColumnEncoding(
      std::vector<facebook::velox::dwrf::proto::orc::ColumnEncoding>& encodings) const = 0;

    /**
     * Get the stripe statistics for this column
     */
    virtual void getStripeStatistics(
      std::vector<facebook::velox::dwrf::proto::orc::ColumnStatistics>& stats) const;

    /**
     * Get the file statistics for this column
     */
    virtual void getFileStatistics(
      std::vector<facebook::velox::dwrf::proto::orc::ColumnStatistics>& stats) const;

    /**
     * Merge index stats into stripe stats and reset index stats
     */
    virtual void mergeRowGroupStatsIntoStripeStats();

    /**
     * Merge stripe stats into file stats and reset stripe stats
     */
    virtual void mergeStripeStatsIntoFileStats();

    /**
     * Create a row index entry with the previous location and the current
     * index statistics. Also merges the index statistics into the stripe
     * statistics before they are cleared. Finally, it records the start of the
     * next index and ensures all of the children columns also create an entry.
     */
    virtual void createRowIndexEntry();

    /**
     * Write row index streams for this column
     * @param streams output list of ROW_INDEX streams
     */
    virtual void writeIndex(std::vector<facebook::velox::dwrf::proto::orc::Stream> &streams) const;

    /**
     * Record positions for index
     *
     * This function is called by createRowIndexEntry() and ColumnWrtier's
     * constructor. So base classes do not need to call inherited classes'
     * recordPosition() function.
     */
    virtual void recordPosition() const;

    /**
     * Reset positions for index
     */
    virtual void reset();

  protected:
    /**
     * Utility function to translate ColumnStatistics into protobuf form and
     * add it to output list
     * @param statsList output list for protobuf stats
     * @param stats ColumnStatistics to be transformed and added
     */
     void getProtoBufStatistics(
                                std::vector<facebook::velox::dwrf::proto::orc::ColumnStatistics>& statsList,
                                const MutableColumnStatistics* stats) const {
       facebook::velox::dwrf::proto::orc::ColumnStatistics pbStats;
       stats->toProtoBuf(pbStats);
       statsList.push_back(pbStats);
     }

  protected:
    MemoryPool& memPool;
    std::unique_ptr<BufferedOutputStream> indexStream;
  };

  /**
   * Create a writer for the given type.
   */
  std::unique_ptr<ColumnWriter> buildWriter(
                                            const Type& type,
                                            const StreamsFactory& factory,
                                            const WriterOptions& options);
}

#endif
