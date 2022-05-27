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

  // accumulate the scan results here. We will have one chunk
  // for each chunk in the input table (that has at least one 
  // row matching the filter).
  auto result_chunks_ptr = std::make_shared<std::vector<std::shared_ptr<Chunk>>>();

  // iterate over all chunks in the input table and process each chunk seperately.
  // We first determine which rows should be included in the output table,
  // i.e. which rows match the filter condition. Then, we construct new
  // chunks that consist of reference segments and make up the output table.
  for (auto chunk_id = ChunkID{0}; chunk_id < n_chunks; ++chunk_id) {
    auto chunk_ptr = in_table_ptr->get_chunk(chunk_id);
    auto include_rows_ptr = scan_chunk(chunk_ptr, chunk_id, data_type);
    if (!include_rows_ptr->empty()) {
      auto out_chunk = subset_chunk(in_table_ptr, chunk_ptr, chunk_id, include_rows_ptr);
      result_chunks_ptr->emplace_back(out_chunk);
    }
  }

  // build the output table. There are two cases: 
  // (1) if the result set is empty (i.e. no rows match
  // the filter condition) we create an empty table. The empty table
  // has the same column definitions (i.e. column names and types) as the input table.
  // (2) if there are rows in the result set we create a new table with the
  // chunks that we have already constructed. We copy the column configuration 
  // from the input table by using Table's specialized constructor.
  if (result_chunks_ptr->empty()) {
    auto out_table_ptr = std::make_shared<Table>();
    for (auto col_id = ColumnID{0}; col_id < n_columns; ++col_id) {
      out_table_ptr->add_column(in_table_ptr->column_name(col_id), in_table_ptr->column_type(col_id));
    }
    return out_table_ptr;
  } else {
    return std::make_shared<Table>(*result_chunks_ptr, in_table_ptr);
  }
}

const std::shared_ptr<std::vector<ChunkOffset>> TableScan::scan_chunk(
  const std::shared_ptr<const Chunk> chunk_ptr, 
  const ChunkID chunk_id,
  const std::string data_type
) const {
  // determine the set of rows that should be included in the scan output,
  // i.e. find the row indexes that match the filter condition.

  // accumulate the indexes of the rows that should be included in the output here
  auto include_rows_ptr = std::make_shared<std::vector<ChunkOffset>>();

  // get segment that we want to filter on and cast it to the right type. Then,
  // perform a scan on the segment based on the segment type (value/dict/reference).
  auto segment_ptr = chunk_ptr->get_segment(_column_id);
  resolve_data_type(data_type, [&](auto type) {
    using Type = typename decltype(type)::type;
    // case 1: segment is value segment
    const auto typed_value_segment_ptr = std::dynamic_pointer_cast<ValueSegment<Type>>(segment_ptr);
    if (typed_value_segment_ptr) {
      include_rows_ptr = scan_segment<Type>(typed_value_segment_ptr);
      return;
    }
    // case 2: segment is dictionary segment
    const auto typed_dict_segment_ptr = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment_ptr);
    if (typed_dict_segment_ptr) {
      include_rows_ptr = scan_segment<Type>(typed_dict_segment_ptr);
      return;
    }
    // case 3: segment is reference segment
    const auto ref_segment_ptr = std::dynamic_pointer_cast<ReferenceSegment>(segment_ptr);
    if (ref_segment_ptr) {
      include_rows_ptr = scan_segment<Type>(ref_segment_ptr);
      return;
    }

    // we do not support any segment types beyond value, dict, and reference segments
    throw std::runtime_error("unrecognized segment class at chunk id "+std::to_string(chunk_id)+" and column id "+std::to_string(_column_id));
  });

  return include_rows_ptr;
}

const std::shared_ptr<Chunk> TableScan::subset_chunk(
  const std::shared_ptr<const Table> table_ptr,
  const std::shared_ptr<const Chunk> chunk_ptr, 
  const ChunkID chunk_id,
  const std::shared_ptr<std::vector<ChunkOffset>>& include_rows_ptr
) const {
  // create a chunk that consists of reference segments that point 
  // only to the values that we want to include in the output table.
  // The values that we want to include are given in include_rows_ptr.

  // accumulate the new reference segments in this chunk
  auto out_chunk_ptr = std::make_shared<Chunk>();

  // we convert each segment to a reference segment independently
  auto n_segments = chunk_ptr->column_count();
  for (auto col_id = ColumnID{0}; col_id < n_segments; ++col_id) {
    auto segment_ptr = chunk_ptr->get_segment(col_id);
    auto data_type = table_ptr->column_type(col_id);
    resolve_data_type(data_type, [&](auto type) {
      using Type = typename decltype(type)::type;

      // case 1: segment is value segment or dictionary segment
      // the new reference segment can point directly to the existing segment. We
      // just need to create a new reference segments with the indexes of the 
      // rows that we want to keep (i.e. the values in include_rows_ptr).
      const auto typed_value_segment_ptr = std::dynamic_pointer_cast<ValueSegment<Type>>(segment_ptr);
      const auto typed_dict_segment_ptr = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment_ptr);
      if (typed_value_segment_ptr || typed_dict_segment_ptr) {
        auto pos_list = std::make_shared<PosList>();
        for (const auto& pos : *include_rows_ptr) {
          pos_list->emplace_back(RowID{chunk_id, pos});
        }
        auto new_segment = std::make_shared<ReferenceSegment>(table_ptr, col_id, pos_list);
        out_chunk_ptr->add_segment(new_segment);
        return;
      }

      // case 2: segment is reference segment
      // In this case we need to create a new reference segment that points to the table
      // that the existing reference segment points to. This is needed to keep the number
      // of indirections low. 
      const auto ref_segment_ptr = std::dynamic_pointer_cast<ReferenceSegment>(segment_ptr);
      if (ref_segment_ptr) {
        auto referenced_table = ref_segment_ptr->referenced_table();
        auto referenced_column_id = ref_segment_ptr->referenced_column_id();
        auto pos_list = ref_segment_ptr->pos_list();
        auto filtered_pos_list = std::make_shared<PosList>();
        for (const auto& pos : *include_rows_ptr) {
          filtered_pos_list->emplace_back((*pos_list)[pos]);
        }
        auto new_segment = std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, filtered_pos_list);
        out_chunk_ptr->add_segment(new_segment);
        return;
      }

      // we do not support any segment types beyond value, dict, and reference segments
      throw std::runtime_error("unrecognized segment class at chunk id "+std::to_string(chunk_id)+" and column id "+std::to_string(_column_id));
    });
  }
  return out_chunk_ptr;
}

template<typename T>
std::shared_ptr<std::vector<ChunkOffset>> TableScan::scan_segment(
  const std::shared_ptr<const ValueSegment<T>> segment_ptr
) const {
  // determine which values match the filter condition in a 
  // value segment.
  auto include_rows_ptr = std::make_shared<std::vector<ChunkOffset>>();
  auto values = segment_ptr->values();
  auto n_values = values.size();
  for (auto offset = ChunkOffset{0}; offset < n_values; ++offset) {
    auto value = values[offset];
    if (matches_search_value<T>(value)) {
      include_rows_ptr->emplace_back(offset);
    }
  }
  return include_rows_ptr;
}

template<typename T>
std::shared_ptr<std::vector<ChunkOffset>> TableScan::scan_segment(
  const std::shared_ptr<const DictionarySegment<T>> segment_ptr
) const {
  // determine which values match the filter condition in a 
  // dictionary segment. 
  // The current implementation uses an unoptimized approach where
  // we unpack each value in the dictionary segment and check the
  // condition. We could improve performance by making use of the 
  // ordered dictionary directly.
  auto include_rows_ptr = std::make_shared<std::vector<ChunkOffset>>();
  auto dictionary = segment_ptr->dictionary();
  auto attribute_vector_ptr = segment_ptr->attribute_vector();
  auto n_values = attribute_vector_ptr->size();
  for (auto offset = ChunkOffset{0}; offset < n_values; ++offset) {
    auto value = dictionary[attribute_vector_ptr->get(offset)];
    if (matches_search_value<T>(value)) {
      include_rows_ptr->emplace_back(offset);
    }
  }
  return include_rows_ptr;
}

template<typename T>
std::shared_ptr<std::vector<ChunkOffset>> TableScan::scan_segment(
  const std::shared_ptr<const ReferenceSegment> segment_ptr
) const {
  // determine which values match the filter condition in a 
  // reference segment.
  // We cannot determine if a row matches the filter condition
  // based just on the information in the reference segment. We 
  // need to go to the table that the reference segment points to
  // in order to retrieve the actual values and perform the filtering.
  // We need to treat the reference segment differently based on whether
  // it points to a value segment or a dictionary segment.

  auto include_rows_ptr = std::make_shared<std::vector<ChunkOffset>>();
  auto referenced_table_ptr = segment_ptr->referenced_table();
  auto referenced_column_id = segment_ptr->referenced_column_id();
  auto pos_list_ptr = segment_ptr->pos_list();
  auto n_values = pos_list_ptr->size();

  // iterate over all rows in the reference segment. Retrieve the
  // segment that the reference segment points to and get the actual
  // value from that segment.
  for (auto offset = ChunkOffset{0}; offset < n_values; ++offset) {
    auto row_id = (*pos_list_ptr)[offset];
    auto chunk_id = row_id.chunk_id;
    auto chunk_offset = row_id.chunk_offset;
    auto referenced_segment_ptr = referenced_table_ptr->get_chunk(chunk_id)->get_segment(referenced_column_id);

    // referenced segment is ValueSegment
    const auto typed_value_segment_ptr = std::dynamic_pointer_cast<ValueSegment<T>>(referenced_segment_ptr);
    if (typed_value_segment_ptr) {
      auto value = (typed_value_segment_ptr->values())[chunk_offset];
      if (matches_search_value<T>(value)) {
        include_rows_ptr->emplace_back(offset);
      }
      continue;
    }
    
    // referenced segment is DictionarySegment 
    const auto typed_dict_segment_ptr = std::dynamic_pointer_cast<DictionarySegment<T>>(referenced_segment_ptr);
    if (typed_dict_segment_ptr) {
      auto dictionary = typed_dict_segment_ptr->dictionary();
      auto attribute_vector_ptr = typed_dict_segment_ptr->attribute_vector();
      auto value = dictionary[attribute_vector_ptr->get(chunk_offset)];
      if (matches_search_value<T>(value)) {
        include_rows_ptr->emplace_back(offset);
      }
      continue;
    }

    // reference segments can only refer to value segments or dict segments
    throw std::runtime_error("reference segment refers to invalid segment type");
  }

  return include_rows_ptr;
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
