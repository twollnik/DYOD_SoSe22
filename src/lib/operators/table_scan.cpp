#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "table_scan.hpp"
#include "types.hpp"
#include "type_cast.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

TableScan::TableScan(
  const std::shared_ptr<const AbstractOperator>& in, 
  const ColumnID column_id, 
  const ScanType scan_type,
  const AllTypeVariant search_value) :
    _in{in}, _column_id{column_id}, _scan_type{scan_type}, _search_value{search_value} {}

ColumnID TableScan::column_id() const {
  return _column_id;
}

ScanType TableScan::scan_type() const {
  return _scan_type;
}

const AllTypeVariant& TableScan::search_value() const {
  return _search_value;
}

std::shared_ptr<const Table> TableScan::_on_execute() {

  auto in_table_ptr = _in->get_output();
  auto n_columns = in_table_ptr->column_count();
  auto n_chunks = in_table_ptr->chunk_count();
  auto data_type = in_table_ptr->column_type(_column_id);

  // accumulate the scan results here. 
  auto result_chunks_ptr = std::make_shared<std::vector<std::shared_ptr<Chunk>>>();

  // iterate over all chunks in the input table and process each chunk seperately
  for (auto chunk_id = ChunkID{0}; chunk_id < n_chunks; ++chunk_id) {
    auto chunk_ptr = in_table_ptr->get_chunk(chunk_id);
    auto include_rows = scan_chunk(chunk_ptr, chunk_id, data_type);
    if (!include_rows->empty()) {
      auto out_chunk = subset_chunk(in_table_ptr, chunk_ptr, chunk_id, include_rows);
      result_chunks_ptr->emplace_back(out_chunk);
    }
  }

  // build output table
  if (result_chunks_ptr->empty()) {
    // the result set is empty. Create an empty table and add the column definitions
    auto out_table_ptr = std::make_shared<Table>();
    for (auto col_id = ColumnID{0}; col_id < n_columns; ++col_id) {
      out_table_ptr->add_column(in_table_ptr->column_name(col_id), in_table_ptr->column_type(col_id));
    }
    return out_table_ptr;
  } else {
    // the scan has found results. Create a table with the results and copy the column names and types from the input table
    return std::make_shared<Table>(*result_chunks_ptr, in_table_ptr);
  }
}


const std::shared_ptr<std::vector<ChunkOffset>> TableScan::scan_chunk(
  const std::shared_ptr<const Chunk> chunk_ptr, 
  const ChunkID chunk_id,
  const std::string data_type
) const {

  // determine the set of rows that should be included in the scan output
  auto include_rows = std::make_shared<std::vector<ChunkOffset>>();
  auto segment_ptr = chunk_ptr->get_segment(_column_id);
  resolve_data_type(data_type, [&](auto type) {
    using Type = typename decltype(type)::type;
    // case 1: filter segment is value segment
    const auto typed_value_segment_ptr = std::dynamic_pointer_cast<ValueSegment<Type>>(segment_ptr);
    if (typed_value_segment_ptr) {
      scan_segment<Type>(typed_value_segment_ptr, include_rows);
      return include_rows;
    }
    // case 2: filter segment is dictionary segment
    const auto typed_dict_segment_ptr = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment_ptr);
    if (typed_dict_segment_ptr) {
      scan_segment<Type>(typed_dict_segment_ptr, include_rows);
      return include_rows;
    }
    // case 3: filter segment is reference segment
    const auto ref_segment_ptr = std::dynamic_pointer_cast<ReferenceSegment>(segment_ptr);
    if (ref_segment_ptr) {
      scan_segment<Type>(ref_segment_ptr, include_rows);
      return include_rows;
    }
    throw std::runtime_error("unrecognized segment class at chunk id "+std::to_string(chunk_id)+" and column id "+std::to_string(_column_id));
  });

  return include_rows;
}

const std::shared_ptr<Chunk> TableScan::subset_chunk(
  const std::shared_ptr<const Table> table_ptr,
  const std::shared_ptr<const Chunk> chunk_ptr, 
  const ChunkID chunk_id,
  const std::shared_ptr<std::vector<ChunkOffset>>& include_rows
) const {
  // create reference segment with just the values from include_rows
  // use include_rows to build up output segments and chunk
  auto out_chunk_ptr = std::make_shared<Chunk>();
  auto n_segments = chunk_ptr->column_count();
  for (auto col_id = ColumnID{0}; col_id < n_segments; ++col_id) {
    auto segment_ptr = chunk_ptr->get_segment(col_id);
    auto data_type = table_ptr->column_type(col_id);
    resolve_data_type(data_type, [&](auto type) {
      using Type = typename decltype(type)::type;
      // case 1: segment is value segment or dictionary segment
      // -> point directly to it
      const auto typed_value_segment_ptr = std::dynamic_pointer_cast<ValueSegment<Type>>(segment_ptr);
      const auto typed_dict_segment_ptr = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment_ptr);
      if (typed_value_segment_ptr || typed_dict_segment_ptr) {
        auto pos_list = std::make_shared<PosList>();
        for (const auto& pos : *include_rows) {
          pos_list->emplace_back(RowID{chunk_id, pos});
        }
        auto new_segment = std::make_shared<ReferenceSegment>(table_ptr, col_id, pos_list);
        out_chunk_ptr->add_segment(new_segment);
        return;
      }
      // case 2: segment is reference segment
      // -> we need to refer to the table that the reference segment refers
      //    to in order to keep the number indirections low
      const auto ref_segment_ptr = std::dynamic_pointer_cast<ReferenceSegment>(segment_ptr);
      if (ref_segment_ptr) {
        auto pos_list = ref_segment_ptr->pos_list();
        auto referenced_column_id = ref_segment_ptr->referenced_column_id();
        auto referenced_table = ref_segment_ptr->referenced_table();
        auto filtered_pos_list = std::make_shared<PosList>();
        for (const auto& pos : *include_rows) {
          filtered_pos_list->emplace_back((*pos_list)[pos]);
        }
        auto new_segment = std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, filtered_pos_list);
        out_chunk_ptr->add_segment(new_segment);
        return;
      }

      throw std::runtime_error("unrecognized segment class at chunk id "+std::to_string(chunk_id)+" and column id "+std::to_string(_column_id));
    });
  }
  return out_chunk_ptr;
}

template<typename T>
void TableScan::scan_segment(
  const std::shared_ptr<const ValueSegment<T>> segment_ptr,
  std::shared_ptr<std::vector<ChunkOffset>> include_rows
) const {
  auto values = segment_ptr->values();
  auto n_values = values.size();
  for (auto offset = ChunkOffset{0}; offset < n_values; ++offset) {
    auto value = values[offset];
    if (matches_search_value<T>(value)) {
      include_rows->emplace_back(offset);
    }
  }
}

template<typename T>
void TableScan::scan_segment(
  const std::shared_ptr<const DictionarySegment<T>> segment_ptr,
  std::shared_ptr<std::vector<ChunkOffset>> include_rows
) const {
  auto dictionary = segment_ptr->dictionary();
  auto attribute_vector_ptr = segment_ptr->attribute_vector();
  auto n_values = attribute_vector_ptr->size();
  for (auto offset = ChunkOffset{0}; offset < n_values; ++offset) {
    auto value = dictionary[attribute_vector_ptr->get(offset)];
    if (matches_search_value<T>(value)) {
      include_rows->emplace_back(offset);
    }
  }
}

template<typename T>
void TableScan::scan_segment(
  const std::shared_ptr<const ReferenceSegment> segment_ptr,
  std::shared_ptr<std::vector<ChunkOffset>> include_rows
) const {
  auto referenced_table_ptr = segment_ptr->referenced_table();
  auto referenced_column_id = segment_ptr->referenced_column_id();
  auto pos_list_ptr = segment_ptr->pos_list();
  auto n_values = pos_list_ptr->size();

  for (auto offset = ChunkOffset{0}; offset < n_values; ++offset) {
    auto row_id = (*pos_list_ptr)[offset];
    auto chunk_id = row_id.chunk_id;
    auto chunk_offset = row_id.chunk_offset;
    auto referenced_segment_ptr = referenced_table_ptr->get_chunk(chunk_id)->get_segment(referenced_column_id);

    // handle case where referenced segment is ValueSegment
    const auto typed_value_segment_ptr = std::dynamic_pointer_cast<ValueSegment<T>>(referenced_segment_ptr);
    if (typed_value_segment_ptr) {
      auto value = (typed_value_segment_ptr->values())[chunk_offset];
      if (matches_search_value<T>(value)) {
        include_rows->emplace_back(offset);
      }
      continue;
    }
    
    // handle case where referenced segment is DictionarySegment 
    const auto typed_dict_segment_ptr = std::dynamic_pointer_cast<DictionarySegment<T>>(referenced_segment_ptr);
    if (typed_dict_segment_ptr) {
      auto dictionary = typed_dict_segment_ptr->dictionary();
      auto attribute_vector_ptr = typed_dict_segment_ptr->attribute_vector();
      auto value = dictionary[attribute_vector_ptr->get(chunk_offset)];
      if (matches_search_value<T>(value)) {
        include_rows->emplace_back(offset);
      }
      continue;
    }

    // reference segments can only refer to value segments or dict segments
    throw std::runtime_error("reference segment refers to invalid segment type");
  }
}

template<typename T>
bool TableScan::matches_search_value(T value) const {
  auto search_value = type_cast<T>(_search_value);
  switch (_scan_type) {
    case ScanType::OpEquals:
      return search_value == value;
    case ScanType::OpNotEquals:
      return search_value != value;
    case ScanType::OpLessThan:
      return value < search_value;
    case ScanType::OpLessThanEquals:
      return value <= search_value;
    case ScanType::OpGreaterThan:
      return value > search_value;
    case ScanType::OpGreaterThanEquals:
      return value >= search_value;
    default:
      throw std::runtime_error("unknown search type");
  }
}

}  // namespace opossum
