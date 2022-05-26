#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_segment.hpp"
#include "dictionary_segment.hpp"
#include "reference_segment.hpp"
#include "table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

ReferenceSegment::ReferenceSegment(
  const std::shared_ptr<const Table>& referenced_table, 
  const ColumnID referenced_column_id,
  const std::shared_ptr<const PosList>& pos) : 
    _referenced_table{referenced_table}, 
    _referenced_column_id{referenced_column_id}, 
    _pos{pos} {}

AllTypeVariant ReferenceSegment::operator[](const ChunkOffset chunk_offset) const {
  DebugAssert(chunk_offset < referenced_table()->row_count(), "invalid chunk offset");
  // find the right chunk and fetch the value from there
  auto n_chunks = referenced_table()->chunk_count();
  auto cur_row = ChunkOffset{0};
  for (auto cur_chunk_id = ChunkID{0}; cur_chunk_id < n_chunks; ++cur_chunk_id) {
    auto cur_chunk = referenced_table()->get_chunk(cur_chunk_id);
    if (cur_row + cur_chunk->size() < chunk_offset) {
      // the target row is in this chunk
      return (*(cur_chunk->get_segment(referenced_column_id())))[chunk_offset-cur_row];
    } else {
      cur_row = cur_row + cur_chunk->size();
    }
  }
  throw std::runtime_error("ran out of chunks to check");
}

void ReferenceSegment::append(const AllTypeVariant&) { throw std::logic_error("ReferenceSegment is immutable"); }

ChunkOffset ReferenceSegment::size() const {
  return pos_list()->size();
}

const std::shared_ptr<const PosList> ReferenceSegment::pos_list() const {
  return _pos;
}

const std::shared_ptr<const Table> ReferenceSegment::referenced_table() const {
  return _referenced_table;
}

ColumnID ReferenceSegment::referenced_column_id() const {
  return _referenced_column_id;
}

size_t ReferenceSegment::estimate_memory_usage() const {
  return sizeof(RowID) * pos_list()->capacity();
}

}  // namespace opossum
