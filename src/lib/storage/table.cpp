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

#include "dictionary_segment.hpp"
#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const ChunkOffset target_chunk_size) : _target_chunk_size{target_chunk_size} { create_new_chunk(); }

Table::Table(const std::vector<std::shared_ptr<Chunk>> chunks, const std::shared_ptr<const Table> table_config,
             const ChunkOffset target_chunk_size)
    : _target_chunk_size{target_chunk_size}, _chunks{chunks} {
  auto n_cols = table_config->column_count();
  for (auto col_id = ColumnID{0}; col_id < n_cols; ++col_id) {
    _column_names.push_back(table_config->column_name(col_id));
    _column_types.push_back(table_config->column_type(col_id));
  }
}

void Table::add_column(const std::string& name, const std::string& type) {
  Assert(row_count() == 0, "columns should only be added to empty tables. " + name + " is not empty");
  _column_names.push_back(name);
  _column_types.push_back(type);
  // add column to existing last chunk
  _chunks.back()->create_and_add_segment(type);
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  // Implementation goes here
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
  return std::accumulate(_chunks.begin(), _chunks.end(), ChunkOffset{0},
                         [](const ChunkOffset& cur_sum, const auto cur_chunk) { return cur_sum + cur_chunk->size(); });
}

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto iter = std::find(_column_names.begin(), _column_names.end(), column_name);
  DebugAssert(iter != _column_names.end(), "column " + column_name + " does not exist");
  return static_cast<ColumnID>(std::distance(_column_names.begin(), iter));
}

ChunkOffset Table::target_chunk_size() const { return _target_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(const ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(const ColumnID column_id) const { return _column_types.at(column_id); }

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) { return _chunks.at(chunk_id); }

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const { return _chunks.at(chunk_id); }

void Table::compress_chunk(const ChunkID chunk_id) {
  DebugAssert(chunk_id < chunk_count(), "invalid chunk id " + std::to_string(chunk_id) + ". table only has " +
                                            std::to_string(chunk_count()) + " chunks");

  auto old_chunk = get_chunk(chunk_id);
  auto n_segments = old_chunk->column_count();
  auto new_chunk = std::make_shared<Chunk>(ColumnID{n_segments});

  auto compression_worker_lambda = [this, &old_chunk, &new_chunk](const ColumnID column_id) {
    const auto& segment = old_chunk->get_segment(column_id);
    const auto& type = this->column_type(column_id);
    resolve_data_type(type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      const auto dictionary_segment = std::make_shared<DictionarySegment<ColumnDataType>>(segment);
      new_chunk->add_segment_at(dictionary_segment, column_id);
    });
  };

  auto threads = std::vector<std::thread>();
  for (auto column_index = ColumnID{0}; column_index < n_segments; ++column_index) {
    auto worker = std::thread(compression_worker_lambda, column_index);
    threads.emplace_back(std::move(worker));
  }
  for (auto& thread : threads) {
    thread.join();
  }

  // swap in new dict encoded chunk
  // TODO(all): consider concurrent accesses when exchanging the chunk?
  _chunks[chunk_id] = new_chunk;
}

}  // namespace opossum
