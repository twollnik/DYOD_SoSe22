#include <memory>
#include <sstream>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/storage_manager.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageStorageManagerTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& storage_manager = StorageManager::get();
    storage_manager.reset();
    auto table_a = std::make_shared<Table>();
    auto table_b = std::make_shared<Table>(4);

    storage_manager.add_table("first_table", table_a);
    storage_manager.add_table("second_table", table_b);
  }
};

TEST_F(StorageStorageManagerTest, GetTable) {
  auto& storage_manager = StorageManager::get();
  auto table_c = storage_manager.get_table("first_table");
  auto table_d = storage_manager.get_table("second_table");
  EXPECT_THROW(storage_manager.get_table("third_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, DropTable) {
  auto& storage_manager = StorageManager::get();
  storage_manager.drop_table("first_table");
  EXPECT_THROW(storage_manager.get_table("first_table"), std::exception);
  EXPECT_THROW(storage_manager.drop_table("first_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, ResetTable) {
  StorageManager::get().reset();
  auto& storage_manager = StorageManager::get();
  EXPECT_THROW(storage_manager.get_table("first_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, DoesNotHaveTable) {
  auto& storage_manager = StorageManager::get();
  EXPECT_EQ(storage_manager.has_table("third_table"), false);
}

TEST_F(StorageStorageManagerTest, HasTable) {
  auto& storage_manager = StorageManager::get();
  EXPECT_EQ(storage_manager.has_table("first_table"), true);
}

TEST_F(StorageStorageManagerTest, TableNames) {
  auto& storage_manager = StorageManager::get();
  EXPECT_EQ(storage_manager.table_names(), (std::vector<std::string>{"first_table","second_table"}));
}

TEST_F(StorageStorageManagerTest, PrintSimpleTables) {
  auto& storage_manager = StorageManager::get();
  std::ostringstream oss;
  storage_manager.print(oss);
  EXPECT_EQ(
    oss.str(), 
    "=== first_table ===\nn columns: 0\nn rows: 0\nn chunks: 1\ncolumns:\n"
    "=== second_table ===\nn columns: 0\nn rows: 0\nn chunks: 1\ncolumns:\n"
  );
}

TEST_F(StorageStorageManagerTest, PrintMoreComplexTables) {
  auto& storage_manager = StorageManager::get();
  std::ostringstream oss;
  auto first_table = storage_manager.get_table("first_table");
  first_table->add_column("first_col", "int");
  first_table->add_column("second_col", "double");
  first_table->append(std::vector<AllTypeVariant>{AllTypeVariant{1}, AllTypeVariant{2.0}});
  first_table->append(std::vector<AllTypeVariant>{AllTypeVariant{2}, AllTypeVariant{3.0}});
  auto second_table = storage_manager.get_table("second_table");
  second_table->add_column("second_table_first_col", "string");
  storage_manager.print(oss);
  EXPECT_EQ(
    oss.str(), 
    "=== first_table ===\nn columns: 2\nn rows: 2\nn chunks: 1\ncolumns:\n  "
    "first_col (int)\n  second_col (double)\n"
    "=== second_table ===\nn columns: 1\nn rows: 0\nn chunks: 1\ncolumns:\n  "
    "second_table_first_col (string)\n"
  );
}

}  // namespace opossum
