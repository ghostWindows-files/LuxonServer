#pragma once

#include "global.hpp"

namespace server {
namespace IPCEventCodes {
enum Enum : uint8_t { GameUpdate, GameDelete, PersistentPeerStore, PersistentPeerConsume };
}

// IPC-only dictionary keys are intentionally outside the Photon parameter-code
// ranges and are shared by every process in the server cluster.
namespace IPCDictKeyCodes {
enum Enum : uint8_t {
    Revision = 190,
    CreationGeneration = 189,
    IsCreating = 188,
    IsVisible = 187,
    ExpectedUsers = 186,
    ExpectedUserGenerations = 185
};
}
} // namespace server
