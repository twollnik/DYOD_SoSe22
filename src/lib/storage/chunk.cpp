#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "abstract_segment.hpp"
#include "chunk.hpp"
#include "resolve_type.hpp"

#include "utils/assert.hpp"

#include <boost/range/combine.hpp>

namespace opossum {

Chunk::Chunk(ColumnID n_columns) : _segments(std::vector<std::shared_ptr<AbstractSegment>>(n_columns)) {}

void Chunk::add_segment(const std::shared_ptr<AbstractSegment> segment) { _segments.push_back(segment); }

void Chunk::insert_segment_at(const std::shared_ptr<AbstractSegment> segment, const ColumnID position) {
  DebugAssert(position < column_count(), "Can only substitute segments at existing indexes");
  _segments[position] = segment;
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == _segments.size(),
              "The number of segments in the chunk is different from the number of values to be added");
  for (auto const& [segment, value] : boost::combine(_segments, values)) {
    segment->append(value);
  }
}

std::shared_ptr<AbstractSegment> Chunk::get_segment(const ColumnID column_id) const { return _segments.at(column_id); }

ColumnCount Chunk::column_count() const { return static_cast<ColumnCount>(_segments.size()); }

ChunkOffset Chunk::size() const { return _segments.size() ? _segments[0]->size() : 0; }

void Chunk::create_and_add_segment(const std::string& type) {
  resolve_data_type(type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>();
    add_segment(value_segment);
  });
}

}  // namespace opossum
