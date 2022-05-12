#include <memory>
#include <string>

#include "../base_test.hpp"

#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/dictionary_segment.hpp"

namespace opossum {

class StorageDictionarySegmentTest : public BaseTest {
 protected:
  std::shared_ptr<ValueSegment<int32_t>> value_segment_int = std::make_shared<ValueSegment<int32_t>>();
  std::shared_ptr<ValueSegment<std::string>> value_segment_str = std::make_shared<ValueSegment<std::string>>();
};

TEST_F(StorageDictionarySegmentTest, CompressSegmentString) {
  value_segment_str->append("Bill");
  value_segment_str->append("Steve");
  value_segment_str->append("Alexander");
  value_segment_str->append("Steve");
  value_segment_str->append("Hasso");
  value_segment_str->append("Bill");

  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("string", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_str);
  });

  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<std::string>>(segment);

  // Test attribute_vector size
  EXPECT_EQ(dict_segment->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_segment->unique_values_count(), 4u);

  // Test sorting
  const auto dict = dict_segment->dictionary();
  EXPECT_EQ(dict[0], "Alexander");
  EXPECT_EQ(dict[1], "Bill");
  EXPECT_EQ(dict[2], "Hasso");
  EXPECT_EQ(dict[3], "Steve");
}

TEST_F(StorageDictionarySegmentTest, LowerUpperBound) {
  for (auto value = int16_t{0}; value <= 10; value += 2) {
    value_segment_int->append(value);
  }

  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);

  EXPECT_EQ(dict_segment->lower_bound(4), ValueID{2});
  EXPECT_EQ(dict_segment->upper_bound(4), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant{4}), ValueID{2});
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant{4}), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(5), ValueID{3});
  EXPECT_EQ(dict_segment->upper_bound(5), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(15), INVALID_VALUE_ID);
  EXPECT_EQ(dict_segment->upper_bound(15), INVALID_VALUE_ID);
}

TEST_F(StorageDictionarySegmentTest, AccessingElements) {
  value_segment_int->append(1);
  value_segment_int->append(2);

  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);

  // test [] operator
  EXPECT_EQ((*dict_segment)[ChunkOffset{0}], AllTypeVariant{1});
  EXPECT_EQ((*dict_segment)[ChunkOffset{1}], AllTypeVariant{2});
  EXPECT_NO_THROW((*dict_segment)[100]);

  // test get function
  EXPECT_EQ(dict_segment->get(ChunkOffset{0}), 1);
  EXPECT_EQ(dict_segment->get(ChunkOffset{1}), 2);
  EXPECT_ANY_THROW(dict_segment->get(ChunkOffset{100}));
}

TEST_F(StorageDictionarySegmentTest, AppendingElements) {
  value_segment_int->append(1);
  value_segment_int->append(2);

  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);

  EXPECT_ANY_THROW(dict_segment->append(AllTypeVariant{5}));
}

TEST_F(StorageDictionarySegmentTest, AccessingUnderlyingDataStructures) {
  value_segment_int->append(1);
  value_segment_int->append(2);
  value_segment_int->append(2);

  std::shared_ptr<AbstractSegment> segment_int;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment_int = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment_int = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment_int);

  std::shared_ptr<AbstractSegment> segment_str;
  resolve_data_type("string", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment_str = std::make_shared<DictionarySegment<Type>>(value_segment_str);
  });
  auto dict_segment_str = std::dynamic_pointer_cast<DictionarySegment<std::string>>(segment_str);

  // test accessing the underlying dictionary
  EXPECT_EQ(dict_segment_int->dictionary(), (std::vector<int>{1, 2}));
  EXPECT_EQ(dict_segment_str->dictionary(), (std::vector<std::string>{}));

  // test accessing the underlying attribute vector
  EXPECT_EQ(*(dict_segment_int->attribute_vector()), (std::vector<uint32_t>{0,1,1}));
  EXPECT_EQ(*(dict_segment_str->attribute_vector()), (std::vector<uint32_t>{}));
}

TEST_F(StorageDictionarySegmentTest, ValueOfValueId) {
  value_segment_int->append(1);
  value_segment_int->append(2);
  value_segment_int->append(2);

  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);

  EXPECT_EQ(dict_segment->value_of_value_id(ValueID{0}), 1);
  EXPECT_EQ(dict_segment->value_of_value_id(ValueID{1}), 2);
  EXPECT_ANY_THROW(dict_segment->value_of_value_id(ValueID{2}));
}

}  // namespace opossum