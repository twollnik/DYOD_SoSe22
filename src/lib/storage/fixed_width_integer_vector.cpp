#include "fixed_width_integer_vector.hpp"

#include "utils/assert.hpp"
#include "types.hpp"

namespace opossum {

template <typename uintX_t>
ValueID FixedWidthIntegerVector<uintX_t>::get(const size_t index) const {
  return static_cast<ValueID>(_vector.at(index));
}

template <typename uintX_t>
void FixedWidthIntegerVector<uintX_t>::set(const size_t index, const ValueID value_id) {
  _vector[index] = static_cast<uintX_t>(value_id);
}

template <typename uintX_t>
size_t FixedWidthIntegerVector<uintX_t>::size() const {
  return _vector.size();
}

template <typename uintX_t>
AttributeVectorWidth FixedWidthIntegerVector<uintX_t>::width() const {
  return static_cast<AttributeVectorWidth>(sizeof(uintX_t));
}

template <typename uintX_t>
void FixedWidthIntegerVector<uintX_t>::reserve(const size_t capacity) {
  return _vector.reserve(capacity);
}

template <typename uintX_t>
void FixedWidthIntegerVector<uintX_t>::add_value(const ValueID value_id) {
  _vector.push_back(static_cast<uintX_t>(value_id));
}

}  // namespace opossum