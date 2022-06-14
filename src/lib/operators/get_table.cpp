#include <memory>
#include <string>
#include <vector>

#include "../storage/storage_manager.hpp"
#include "get_table.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

GetTable::GetTable(const std::string& name) : _table_name{name} {}

const std::string& GetTable::table_name() { return _table_name; }

std::shared_ptr<const Table> GetTable::_on_execute() {
  DebugAssert(StorageManager::get().has_table(_table_name), "Table " + _table_name + " does not exist");
  return StorageManager::get().get_table(_table_name);
}

}  // namespace opossum
