#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_operator.hpp"

namespace opossum {

// Operator to retrieve a table from the StorageManager by specifying its name.
class GetTable : public AbstractOperator {
 public:
  explicit GetTable(const std::string& name);
  const std::string& table_name();

 protected:
  std::shared_ptr<const Table> _on_execute();
  std::string _table_name;
};

}  // namespace opossum
