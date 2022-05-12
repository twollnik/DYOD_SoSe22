#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/resolve_type.hpp"
#include "../lib/storage/table.hpp"
#include "../lib/storage/dictionary_segment.hpp"

namespace opossum {

class StorageTableTest : public BaseTest {
 protected:
  void SetUp() override {
    table.add_column("col_1", "int");
    table.add_column("col_2", "string");
  }

  Table table{2};
};

TEST_F(StorageTableTest, ChunkCount) {
  EXPECT_EQ(table.chunk_count(), 1u);
  table.append({4, "Hello,"});
  table.append({6, "world"});
  table.append({3, "!"});
  EXPECT_EQ(table.chunk_count(), 2u);
}

TEST_F(StorageTableTest, GetChunk) {
  table.get_chunk(ChunkID{0});
  table.append({4, "Hello,"});
  table.append({6, "world"});
  table.append({3, "!"});
  table.get_chunk(ChunkID{1});
  const auto chunk = table.get_chunk(ChunkID{0});
  EXPECT_EQ(chunk->size(), 2);
  EXPECT_THROW(table.get_chunk(ChunkID{7}), std::exception);
}

TEST_F(StorageTableTest, ColumnCount) { EXPECT_EQ(table.column_count(), 2u); }

TEST_F(StorageTableTest, RowCount) {
  EXPECT_EQ(table.row_count(), 0u);
  table.append({4, "Hello,"});
  table.append({6, "world"});
  table.append({3, "!"});
  EXPECT_EQ(table.row_count(), 3u);
  
  // should still work with chunks of unequal size
  table.create_new_chunk();
  table.create_new_chunk();
  table.append({6, "lala"});
  EXPECT_EQ(table.row_count(), 4u);
}

TEST_F(StorageTableTest, GetColumnName) {
  EXPECT_EQ(table.column_name(ColumnID{0}), "col_1");
  EXPECT_EQ(table.column_name(ColumnID{1}), "col_2");
  EXPECT_EQ(table.column_names(), std::vector<std::string>({"col_1", "col_2"}));
  EXPECT_THROW(table.column_name(ColumnID{7}), std::exception);
}

TEST_F(StorageTableTest, GetColumnType) {
  EXPECT_EQ(table.column_type(ColumnID{0}), "int");
  EXPECT_EQ(table.column_type(ColumnID{1}), "string");
  EXPECT_THROW(table.column_type(ColumnID{7}), std::exception);
}

TEST_F(StorageTableTest, GetColumnIdByName) {
  EXPECT_EQ(table.column_id_by_name("col_2"), 1u);
  EXPECT_THROW(table.column_id_by_name("no_column_name"), std::exception);
}

TEST_F(StorageTableTest, GetChunkSize) { EXPECT_EQ(table.target_chunk_size(), 2u); }

TEST_F(StorageTableTest, CompressChunk) { 

  table.append({4, "Hello"});
  table.append({6, "Hello"});
  table.append({3, "!"});

  table.compress_chunk(ChunkID{0});

  auto encoded_chunk = table.get_chunk(ChunkID{0});
  auto dict_seg_0 = static_cast<DictionarySegment<int>>(encoded_chunk->get_segment(ColumnID{0}));
  auto dict_seg_1 = static_cast<DictionarySegment<std::string>>(encoded_chunk->get_segment(ColumnID{1}));
  auto att_vec_0 = dict_seg_0.attribute_vector();
  auto att_vec_1 = dict_seg_1.attribute_vector();

  EXPECT_EQ(encoded_chunk->size(), 2u);
  EXPECT_EQ(encoded_chunk->column_count(), 2u);
  EXPECT_EQ(dict_seg_0.dictionary(), (std::vector<int>{4,6}));
  EXPECT_EQ(dict_seg_1.dictionary(), (std::vector<std::string>{"Hello"}));
  EXPECT_EQ(att_vec_0->get(0), 0);
  EXPECT_EQ(att_vec_0->get(1), 1);
  EXPECT_EQ(att_vec_1->get(0), 0);
  EXPECT_EQ(att_vec_1->get(1), 0);
  EXPECT_ANY_THROW(att_vec_0->get(2));
  EXPECT_ANY_THROW(att_vec_1->get(2));

  EXPECT_ANY_THROW(table.compress_chunk(ChunkID{3}));

  // regular table functions should still work as expected
  EXPECT_EQ(table.row_count(), 3u);
  EXPECT_EQ(table.chunk_count(), 2u);
}

}  // namespace opossum
