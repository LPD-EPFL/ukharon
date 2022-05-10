#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>

namespace dory::membership::internal {
template <typename ProcIdType>
struct FullMember {
  enum MemberType : uint8_t {
    None = 0,
    Consensus,
    Cache,
    App,
  };

  FullMember(ProcIdType pid = 0, MemberType type = None)  // NOLINT
      : pid{pid}, type{type} {}                           // NOLINT

  ProcIdType pid;   // NOLINT
  MemberType type;  // NOLINT

  bool isApp() const { return type == App; }

  bool isCache() const { return type == Cache; }

  bool isNone() const { return type == None; }

  bool isConsensus() const { return type == Consensus; }

  constexpr bool operator==(FullMember const &o) const {
    return o.type == type && o.pid == pid;
  }
};
}  //  namespace dory::membership::internal

namespace std {
template <typename ProcIdType>
struct hash<dory::membership::internal::FullMember<ProcIdType>> {
  using argument_type = dory::membership::internal::FullMember<ProcIdType>;
  using result_type = size_t;

  size_t operator()(const argument_type &x) const noexcept {
    return static_cast<size_t>(x.pid);
  }
};
}  // namespace std

namespace dory::membership::internal {
template <typename ProcIdType, typename ViewIdType, size_t N>
struct FullMembershipImpl {
  using MemberType = FullMember<ProcIdType>;

  ViewIdType id = 0;  // Sequence number of the membership

  uint16_t cnt = 0;  // Number of members that follow
  MemberType members[N];

  static size_t constexpr MaxCount = N;

  FullMembershipImpl() = default;

  FullMembershipImpl(FullMembershipImpl const &m) noexcept {
    id = m.id;
    cnt = m.cnt;

    std::memcpy(members, m.members, cnt * sizeof(MemberType));
  }

  FullMembershipImpl &operator=(FullMembershipImpl const &m) {
    if (this == &m) {
      return *this;
    }

    id = m.id;
    cnt = m.cnt;

    std::memcpy(members, m.members, cnt * sizeof(MemberType));
    return *this;
  }

  bool addApp(ProcIdType pid) { return add(pid, MemberType::MemberType::App); }

  bool addConsensus(ProcIdType pid) {
    return add(pid, MemberType::MemberType::Consensus);
  }

  bool addCache(ProcIdType pid) {
    return add(pid, MemberType::MemberType::Cache);
  }

  bool add(ProcIdType pid, typename MemberType::MemberType type) {
    if (static_cast<size_t>(cnt) >= MaxCount) {
      return false;
    }

    auto [exists, index] = includes(pid);

    if (exists) {
      return false;
    }

    members[cnt] = MemberType(pid, type);
    cnt++;

    return true;
  }

  bool remove(ProcIdType pid) {
    for (int32_t i = 0; i < cnt; i++) {
      if (members[i].pid == pid) {
        cnt--;
        members[i] = members[cnt];
        return true;
      }
    }
    return false;
  }

  std::pair<bool, size_t> includes(ProcIdType pid) const {
    auto member_first = members;
    auto member_last = members + cnt;

    // Binary search (assumes sorted)
    // auto member_it = std::lower_bound(member_first, member_last,
    // MemberType(pid),
    //   [] (MemberType const& m1, MemberType const& m2) -> bool {
    //     return m1.pid < m2.pid;
    //   });
    // bool exists = !(member_it == member_last) && !(pid < member_it->pid);

    // Linear search
    auto member_it = std::find_if(
        member_first, member_last,
        [pid](MemberType const &m) -> bool { return m.pid == pid; });

    bool exists = member_it != member_last;

    return std::make_pair(exists, std::distance(member_first, member_it));
  }

  std::optional<ProcIdType> cyclicallyNext(size_t index) const {
    if (index >= cnt) {
      return std::nullopt;
    }

    auto next_idx = (index + 1) % cnt;
    if (next_idx == index) {
      return std::nullopt;
    }

    return std::optional<ProcIdType>(members[next_idx].pid);
  }

  std::string toString() const {
    std::stringstream ss;
    ss << "FullMembershipImpl{id:" << id << ", "
       << "cnt:" << cnt << ", "
       << "[";

    if (cnt > 0) {
      for (int i = 0; i + 1 < cnt; i++) {
        ss << members[i].pid << ", ";
      }
      ss << members[cnt - 1].pid;
    }

    ss << "]}";

    return ss.str();
  }

  bool isCompatible(FullMembershipImpl const &other) const {
    std::unordered_set<MemberType> exclusive;
    for (size_t i = 0; i < cnt; i++) {
      exclusive.insert(members[i]);
    }
    for (size_t i = 0; i < other.cnt; i++) {
      if (exclusive.erase(other.members[i]) == 0) {
        exclusive.insert(other.members[i]);
      }
    }
    for (auto const &m : exclusive) {
      if (m.isApp()) {
        return false;
      }
    }
    return true;
  }

  size_t sizeOf() const { return size_of_membership(this); }
};

template <typename ProcIdType, typename ViewIdType, size_t N>
static inline size_t size_of_membership(
    FullMembershipImpl<ProcIdType, ViewIdType, N> const *m) {
  using T = FullMembershipImpl<ProcIdType, ViewIdType, N>;
  return offsetof(T, members) +
         sizeof(FullMember<ProcIdType>) * static_cast<size_t>(m->cnt);
}
}  // namespace dory::membership::internal
