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

  auto pos_list_ptr = std::make_shared<PosList>();
  auto table_ptr = _in->get_output();
  auto n_columns = table_ptr->column_count();
  auto data_type = table_ptr->column_type(_column_id);

  // iterate over all segments for the filter column
  auto n_chunks = table_ptr->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < n_chunks; ++chunk_id) {
    auto segment_ptr = table_ptr->get_chunk(chunk_id)->get_segment(_column_id);
    resolve_data_type(data_type, [&](auto type) {
     using Type = typename decltype(type)::type;

     // try casting to value segment
     const auto typed_value_segment_ptr = std::dynamic_pointer_cast<ValueSegment<Type>>(segment_ptr);
     if (typed_value_segment_ptr) {
       scan_segment<Type>(typed_value_segment_ptr, pos_list_ptr, chunk_id);
       return;
     }

     // try casting to dictionary segment
     const auto typed_dict_segment_ptr = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment_ptr);
     if (typed_dict_segment_ptr) {
       scan_segment<Type>(typed_dict_segment_ptr, pos_list_ptr, chunk_id);
       return;
     }
     throw std::runtime_error("unrecognized segment class at chunk id "+std::to_string(chunk_id)+" and column id "+std::to_string(_column_id));
    });
  }

  // build output table
  auto out_table_ptr = std::make_shared<Table>();
  // first: add column definitions
  for (auto col_id = ColumnID{0}; col_id < n_columns; ++col_id) {
    out_table_ptr->add_column(table_ptr->column_name(col_id), table_ptr->column_type(col_id));
  }
  // second: create reference segments and add them to the output table
  auto out_chunk_ptr = out_table_ptr->get_chunk(ChunkID{0});
  for (auto col_id = ColumnID{0}; col_id < n_columns; ++col_id) {
    auto ref_seg_ptr = std::make_shared<ReferenceSegment>(table_ptr, col_id, pos_list_ptr);
    out_chunk_ptr->add_segment_at(ref_seg_ptr, col_id);
  }
  return out_table_ptr;
}

template<typename T>
void TableScan::scan_segment(
  const std::shared_ptr<const ValueSegment<T>>& segment_ptr,
  const std::shared_ptr<PosList> pos_list_ptr,
  const ChunkID chunk_id
) {
  auto values = segment_ptr->values();
  auto n_values = values.size();
  for (auto offset = ChunkOffset{0}; offset < n_values; ++offset) {
    auto value = values[offset];
    if (matches_search_value<T>(value)) {
      pos_list_ptr->emplace_back(RowID{chunk_id, offset});
    }
  }
}

template<typename T>
void TableScan::scan_segment(
  const std::shared_ptr<const DictionarySegment<T>>& segment_ptr,
  const std::shared_ptr<PosList> pos_list_ptr,
  const ChunkID chunk_id
) {
  // TODO
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
