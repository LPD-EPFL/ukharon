#pragma once

// Pin the HERD client to an unused core on the same socket as the RDMA NIC
static constexpr int HerdClientCore = 10;

// Pin the HERD server to an unused core on the same socket as the RDMA NIC.
// Beware not to pick a core that was already picked when compiling Mu.
static constexpr int HerdServerCore = 14;
