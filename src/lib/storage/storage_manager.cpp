#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

#include <boost/range/combine.hpp>

namespace opossum {

StorageManager& StorageManager::get() {
  static auto instance = StorageManager{};
  return instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  Assert(!_tables.contains(name), "Table " + name + " already exists. Please drop the existing table first");
  _tables[name] = table;
}

void StorageManager::drop_table(const std::string& name) {
  Assert(_tables.contains(name), "Table " + name + " does not exist");
  _tables.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  DebugAssert(_tables.contains(name), "Table " + name + " does not exist");
  return _tables.at(name);
}

bool StorageManager::has_table(const std::string& name) const { return _tables.contains(name); }

std::vector<std::string> StorageManager::table_names() const {
  auto table_names = std::vector<std::string>{};
  table_names.reserve(_tables.size());
  for (auto const& [table_name, _] : _tables) {
    table_names.push_back(table_name);
  }
  return table_names;
}

void StorageManager::print(std::ostream& out) const {
  for (auto const& [table_name, table_ptr] : _tables) {
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

}  // namespace opossum
