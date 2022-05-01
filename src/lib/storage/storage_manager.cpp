#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

#include <boost/range/combine.hpp>

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager instance;
  return instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  _table_names.push_back(name);
  _table_ptrs.push_back(table);
}

void StorageManager::drop_table(const std::string& name) {
  auto table_index = get_table_index(name);
  _table_names.erase(_table_names.begin() + table_index);
  _table_ptrs.erase(_table_ptrs.begin() + table_index);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  return _table_ptrs.at(get_table_index(name));
}

bool StorageManager::has_table(const std::string& name) const {
  return std::find(_table_names.begin(), _table_names.end(), name) != _table_names.end();
}

std::vector<std::string> StorageManager::table_names() const { return _table_names; }

void StorageManager::print(std::ostream& out) const {
  for (auto const& [table_name, table_ptr] : boost::combine(_table_names, _table_ptrs)) {
    out << "=== " << table_name << " ===" << std::endl;
    out << "n columns: " << table_ptr->column_count() << std::endl;
    out << "n rows: " << table_ptr->row_count() << std::endl;
    out << "n chunks: " << table_ptr->chunk_count() << std::endl;
    out << "columns:" << std::endl;
    for (ColumnID col_id = ColumnID{0}; col_id < table_ptr->column_count(); col_id++) {
      out << "  " << table_ptr->column_name(col_id) << " (" << table_ptr->column_type(col_id) << ")" << std::endl;
    }
  }
}

void StorageManager::reset() { get() = StorageManager{}; }

uint16_t StorageManager::get_table_index(const std::string& name) const {
  auto offset = std::find(_table_names.begin(), _table_names.end(), name);
  if (offset == _table_names.end()) {
    throw std::invalid_argument("table name is unknown");
  }
  return std::distance(_table_names.begin(), offset);
}

}  // namespace opossum
