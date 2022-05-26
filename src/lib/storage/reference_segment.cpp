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

ReferenceSegment::ReferenceSegment(const std::shared_ptr<const Table>& referenced_table, const ColumnID referenced_column_id,
                 const std::shared_ptr<const PosList>& pos) {
}

AllTypeVariant ReferenceSegment::operator[](const ChunkOffset chunk_offset) const {
  return AllTypeVariant{};
}

void ReferenceSegment::append(const AllTypeVariant&) { throw std::logic_error("ReferenceSegment is immutable"); }

ChunkOffset ReferenceSegment::size() const {
  throw std::logic_error("ReferenceSegment is not implemented yet.");
}

const std::shared_ptr<const PosList>& ReferenceSegment::pos_list() const {
  throw std::logic_error("ReferenceSegment is not implemented yet.");
}

const std::shared_ptr<const Table>& ReferenceSegment::referenced_table() const {
  throw std::logic_error("ReferenceSegment is not implemented yet.");
}

ColumnID ReferenceSegment::referenced_column_id() const {
  throw std::logic_error("ReferenceSegment is not implemented yet.");
}

size_t ReferenceSegment::estimate_memory_usage() const {
  throw std::logic_error("ReferenceSegment is not implemented yet.");
}

}  // namespace opossum
