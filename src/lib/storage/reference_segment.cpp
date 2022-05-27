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
  DebugAssert(
    chunk_offset < size(),
    "invalid chunk offset "+std::to_string(chunk_offset)+" for reference segment with "+std::to_string(size())+" rows"
  );
  auto row_id = _pos->at(chunk_offset);
  auto segment = _referenced_table->get_chunk(row_id.chunk_id)->get_segment(_referenced_column_id);
  return (*segment)[row_id.chunk_offset];
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
