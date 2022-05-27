#pragma once

#include "abstract_attribute_vector.hpp"
#include "types.hpp"

namespace opossum {

// TODO: comments

template <typename uintX_t>
class FixedWidthIntegerVector : public AbstractAttributeVector {
 public:
  FixedWidthIntegerVector(const size_t capacity);
  ValueID get(const size_t index) const override;
  void set(const size_t index, const ValueID value_id) override;
  size_t size() const override;
  AttributeVectorWidth width() const override;

 protected:
  std::vector<uintX_t> _vector{};
};

}  // namespace opossum
