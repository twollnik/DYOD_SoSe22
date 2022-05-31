#include <set>
#include <unordered_map>
#include <limits>

#include "dictionary_segment.hpp"
#include "fixed_width_integer_vector.hpp"
#include "utils/assert.hpp"
#include "types.hpp"
#include "type_cast.hpp"

namespace opossum {

template <typename T>
DictionarySegment<T>::DictionarySegment(const std::shared_ptr<AbstractSegment>& abstract_segment) {

  // determine unique values and store in sorted set
  auto dict_values = std::set<T>{};
  auto segment_size = abstract_segment->size();
  for (auto value_index = ChunkOffset{0}; value_index < segment_size; ++value_index) {
    dict_values.emplace(type_cast<T>((*abstract_segment)[value_index]));
  }

  // select the right attribute vector integer type
  auto n_unique_values = dict_values.size();
  Assert(n_unique_values <= std::numeric_limits<uint32_t>::max(), "Too many unique values");
  if (n_unique_values <= std::numeric_limits<uint8_t>::max()) {
    _attribute_vector = std::make_shared<FixedWidthIntegerVector<uint8_t>>(segment_size);
  } else if (n_unique_values <= std::numeric_limits<uint16_t>::max()) {
    _attribute_vector = std::make_shared<FixedWidthIntegerVector<uint16_t>>(segment_size);
  } else {
    _attribute_vector = std::make_shared<FixedWidthIntegerVector<uint32_t>>(segment_size);
  }

  // build dictionary and store dictionary indexes in hash map for quick lookup during encoding
  auto dict_indexes = std::unordered_map<T, ValueID>{};
  dict_indexes.reserve(n_unique_values);
  _dictionary.reserve(n_unique_values);
  auto cur_dict_index = ValueID{0};
  for (const auto& value : dict_values) {
    _dictionary.push_back(value);
    dict_indexes[value] = cur_dict_index;
    cur_dict_index++;
  }

  // apply dictionary encoding to input segment
  for (ChunkOffset value_index = 0; value_index < segment_size; ++value_index) {
    auto cur_value = type_cast<T>((*abstract_segment)[value_index]);
    _attribute_vector->set(value_index, dict_indexes[cur_value]);
  }
}

template <typename T>
AllTypeVariant DictionarySegment<T>::operator[](const ChunkOffset chunk_offset) const {
  return AllTypeVariant{_dictionary[_attribute_vector->get(chunk_offset)]};
}

template <typename T>
T DictionarySegment<T>::get(const ChunkOffset chunk_offset) const {
  return _dictionary[_attribute_vector->get(chunk_offset)];
}

template <typename T>
void DictionarySegment<T>::append(const AllTypeVariant& value) {
  Fail("Dictionary segments are immutable, i.e., values cannot be appended.");
}

template <typename T>
const std::vector<T>& DictionarySegment<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
std::shared_ptr<AbstractAttributeVector> DictionarySegment<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
const T DictionarySegment<T>::value_of_value_id(const ValueID value_id) const {
  return _dictionary.at(value_id);
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const T value) const {
  auto iter = std::lower_bound(_dictionary.begin(), _dictionary.end(), value);
  if (iter == _dictionary.end()) {
    return INVALID_VALUE_ID;
  }
  return static_cast<ValueID>(std::distance(_dictionary.begin(), iter));
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const AllTypeVariant& value) const {
  return lower_bound(type_cast<T>(value));
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const T value) const {
  auto iter = std::upper_bound(_dictionary.begin(), _dictionary.end(), value);
  if (iter == _dictionary.end()) {
    return INVALID_VALUE_ID;
  }
  return static_cast<ValueID>(std::distance(_dictionary.begin(), iter));
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  return upper_bound(type_cast<T>(value));
}

template <typename T>
ChunkOffset DictionarySegment<T>::unique_values_count() const {
  return static_cast<ChunkOffset>(_dictionary.size());
}

template <typename T>
ChunkOffset DictionarySegment<T>::size() const {
  return static_cast<ChunkOffset>(_attribute_vector->size());
}

template <typename T>
size_t DictionarySegment<T>::estimate_memory_usage() const {
  auto dict_size = sizeof(T) * dictionary().size();
  auto att_vec_size = attribute_vector()->width() * attribute_vector()->size();
  return dict_size + att_vec_size;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DictionarySegment);

}  // namespace opossum
