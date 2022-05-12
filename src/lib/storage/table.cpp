#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "value_segment.hpp"
#include "dictionary_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const ChunkOffset target_chunk_size) : _target_chunk_size{target_chunk_size} {
  create_new_chunk();
}

void Table::add_column(const std::string& name, const std::string& type) {
  Assert(row_count()==0, "columns should only be added to empty tables. "+name+" is not empty");
  _column_names.push_back(name);
  _column_types.push_back(type);
  // add column to existing last chunk
  _chunks.back()->create_and_add_segment(type);
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  if (_chunks.back()->size() >= target_chunk_size()) {
    create_new_chunk();
  }
  _chunks.back()->append(values);
}

void Table::create_new_chunk() {
  auto new_chunk = std::make_shared<Chunk>();
  for (auto const& type : _column_types) {
    new_chunk->create_and_add_segment(type);
  }
  _chunks.push_back(new_chunk);
}

ColumnCount Table::column_count() const { return static_cast<ColumnCount>(_column_names.size()); }

ChunkOffset Table::row_count() const { 
  return std::accumulate(
    _chunks.begin(), _chunks.end(), ChunkOffset{0}, 
    [](const ChunkOffset& cur_sum, const auto cur_chunk) { return cur_sum + cur_chunk->size(); }
  );
}

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto iter = std::find(_column_names.begin(), _column_names.end(), column_name);
  DebugAssert(iter != _column_names.end(), "column "+column_name+" does not exist");
  return static_cast<ColumnID>(std::distance(_column_names.begin(), iter));
}

ChunkOffset Table::target_chunk_size() const { return _target_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(const ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(const ColumnID column_id) const { return _column_types.at(column_id); }

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) { return _chunks.at(chunk_id); }

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const { return _chunks.at(chunk_id); }

void Table::compress_chunk(const ChunkID chunk_id) {

  // create new chunk with dict encoded segments
  auto old_chunk = get_chunk(chunk_id);
  auto new_chunk = std::make_shared<Chunk>();
  auto n_segments = old_chunk->size();
  for (auto segment_indx = ColumnID{0}; segment_indx < n_segments; segment_indx++) {
    auto segment = static_cast<std::shared_ptr<AbstractSegment>>(old_chunk->get_segment(segment_indx));

    resolve_data_type(column_type(segment_indx), [&](auto type) {
      using Type = typename decltype(type)::type;
      segment = std::make_shared<DictionarySegment<Type>>(segment);
    }); 
    new_chunk->add_segment(segment);
  }

  // swap in new dict encoded chunk
  // TODO: consider concurrent accesses when exchanging the chunk
  _chunks[chunk_id] = new_chunk;
}

}  // namespace opossum
