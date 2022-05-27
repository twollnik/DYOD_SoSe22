#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

class BaseTableScanImpl;
class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  template<typename T>
  void scan_segment(
    const std::shared_ptr<const ValueSegment<T>>& segment_ptr,
    const std::shared_ptr<PosList> pos_list_ptr,
    const ChunkID chunk_id);

  template<typename T>
  void scan_segment(
    const std::shared_ptr<const DictionarySegment<T>>& segment_ptr,
    const std::shared_ptr<PosList> pos_list_ptr,
    const ChunkID chunk_id);

  template<typename T>
  void scan_segment(
    const std::shared_ptr<const ReferenceSegment>& segment_ptr,
    const std::shared_ptr<PosList> pos_list_ptr,
    const ChunkID chunk_id);

  template<typename T>
  bool matches_search_value(T value) const;

  std::shared_ptr<const AbstractOperator> _in;
  ColumnID _column_id;
  ScanType _scan_type;
  AllTypeVariant _search_value;
};

}  // namespace opossum
