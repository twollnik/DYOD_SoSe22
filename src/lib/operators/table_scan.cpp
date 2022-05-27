#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "table_scan.hpp"
#include "types.hpp"
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

  auto pos_list = std::make_shared<PosList>();
  auto table_ptr = _in->get_output();
  auto n_columns = table_ptr->column_count();
  auto data_type = table_ptr->column_type(_column_id);

  // iterate over all segments for the filter column
  auto n_chunks = table_ptr->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < n_chunks; ++chunk_id) {
    auto segment_ptr = table_ptr->get_chunk(chunk_id)->get_segment(_column_id);
    resolve_data_type(data_type, [&](auto type) {
     using Type = typename decltype(type)::type;

     // try casting to ValueSegment
     const auto typed_value_segment_ptr = std::dynamic_pointer_cast<ValueSegment<Type>>(segment_ptr);
     if (typed_value_segment_ptr) {
       auto values = typed_value_segment_ptr->values();
       auto n_rows = typed_value_segment_ptr->size();
       for (auto offset = ChunkOffset{0}; offset < n_rows; ++offset) {
         // TODO: perform comparison and add to pos_list
       }
       return;
     }

     // try casting to dictionary segment
     const auto typed_dict_segment_ptr = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment_ptr);
     if (typed_dict_segment_ptr) {
       // TODO: filter dictionary segment
       return;
     }
     throw std::runtime_error("unrecognized segment class at chunk id "+std::to_string(chunk_id)+" and column id "+std::to_string(_column_id));
    });
  }

  // build output table
  auto out_table_ptr = std::make_shared<Table>();
  auto out_chunk_ptr = out_table_ptr->get_chunk(ChunkID{0});
  for (auto col_id = ColumnID{0}; col_id < n_columns; ++col_id) {
    // create reference segment and add to output table
    auto ref_seg_ptr = std::make_shared<ReferenceSegment>(table_ptr, col_id, pos_list);
    out_chunk_ptr -> add_segment(ref_seg_ptr);
    // add column definition to output table
    out_table_ptr->add_column(table_ptr->column_name(col_id), table_ptr->column_type(col_id));
  }
  return out_table_ptr;
}

}  // namespace opossum
