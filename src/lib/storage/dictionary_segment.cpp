#include <unordered_map>

#include "dictionary_segment.hpp"

#include "utils/assert.hpp"
#include "types.hpp"
#include "type_cast.hpp"

namespace opossum {

template <typename T>
DictionarySegment<T>::DictionarySegment(const std::shared_ptr<AbstractSegment>& abstract_segment) 
  : _attribute_vector{std::make_shared<std::vector<uint32_t>>()} {

  // TODO:
  // - dict should be sorted
  // - use different uintX_t types

  // keep dictionary indexes in map for quick lookup during construction
  auto dict_indexes = std::unordered_map<T, uint32_t>();
  
  // apply dictionary encoding to input segment
  auto segment_size = abstract_segment->size();
  for (ChunkOffset value_index = 0; value_index < segment_size; value_index++) {
    auto cur_value = type_cast<T>((*abstract_segment)[value_index]);
    if (!dict_indexes.contains(cur_value)) {
      _dictionary.push_back(cur_value);
      dict_indexes[cur_value] = _dictionary.size()-1;
    }
    _attribute_vector->push_back(dict_indexes.at(cur_value));
  }
}

template <typename T>
AllTypeVariant DictionarySegment<T>::operator[](const ChunkOffset chunk_offset) const {
  return AllTypeVariant{get(chunk_offset)};
}

template <typename T>
T DictionarySegment<T>::get(const ChunkOffset chunk_offset) const {
  return _dictionary[_attribute_vector->at(chunk_offset)];
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
std::shared_ptr<const AbstractAttributeVector> DictionarySegment<T>::attribute_vector() const {
  // Implementation goes here
  return nullptr;
}

template <typename T>
const T DictionarySegment<T>::value_of_value_id(const ValueID value_id) const {
  // Implementation goes here
  Fail("Implementation is missing.");
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const T value) const {
  // Implementation goes here
  return ValueID{};
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const AllTypeVariant& value) const {
  // Implementation goes here
  return ValueID{};
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const T value) const {
  // Implementation goes here
  return ValueID{};
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  // Implementation goes here
  return ValueID{};
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
  return size_t{};
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DictionarySegment);

}  // namespace opossum
