#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>

#include "../internal/constants.hpp"
#include "../internal/decorator.hpp"
#include "../internal/types.hpp"
#include "../types.hpp"

using namespace dory;
using namespace paxos;

// Value spanning over multiple cache lines.
struct Value {
  uint8_t data[internal::CacheLineSize * 8];
  bool operator==(Value const &o) const {
    return std::equal(std::cbegin(data), std::cend(data), std::cbegin(o.data));
  }
};

using Decorator = internal::RealProposalValueDecorator<Value>;

TEST(Decorator, Simple) {
  Instance instance = 3;
  Value original = {};
  for (size_t i = 0; i < sizeof(Value::data); i++) {
    original.data[i] = static_cast<uint8_t>(i);
  }
  auto decorated = Decorator::decorate(original, instance);
  EXPECT_FALSE(
      Decorator::validate(decorated, static_cast<Instance>(instance + 1)));
  EXPECT_FALSE(
      Decorator::validate(decorated, static_cast<Instance>(instance - 1)));
  EXPECT_TRUE(Decorator::validate(decorated, instance));
  EXPECT_EQ(Decorator::undecorate(decorated), original);
}

TEST(Decorator, Concurrent) {
  Instance instance = 3;
  Value original = {};
  for (size_t i = 0; i < sizeof(Value::data); i++) {
    original.data[i] = static_cast<uint8_t>(i);
  }
  auto decorated = Decorator::decorate(original, instance);
  // We simulate a concurrent write over the value by rewriting a cache line.
  Value const second = {};
  Instance const second_instance = 2;
  auto const second_decorated = Decorator::decorate(second, second_instance);
  std::memcpy(&decorated, &second_decorated, internal::CacheLineSize);

  EXPECT_FALSE(Decorator::validate(decorated, instance));
}
