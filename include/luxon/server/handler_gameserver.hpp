// Copyright (c) 2026, the Luxon Server contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "global.hpp"
#include "handler_base.hpp"
#include "peer_persistence.hpp"
#include "coro_support.hpp"

#include <optional>
#include <atomic>
#include <mutex>
#include <commoncpp/timer.hpp>
#include <luxon/ser_types.hpp>

namespace server {
struct GamePeer;
struct Game;

class GameServerHandler : public HandlerBase {
public:   
    using HandlerBase::HandlerBase;

    Awaitable<> HandleDisconnect() override;
    Awaitable<> HandleOperationRequest(ser::OperationRequestMessage&& req, bool is_encrypted, const enet::EnetCommandHeader& cmd_header) override;

    void mark_disconnected() { disconnected_.store(true, std::memory_order_release); }
    const std::shared_ptr<Game>& get_current_game() const { return current_game_; }

    // Public access for pending join processing
    std::shared_ptr<Peer>& get_peer() { return peer_; }

protected:
    std::shared_ptr<Game> current_game_;
    GamePeer *game_peer_{};
    bool has_left_{};
    std::atomic_bool disconnected_{};
    // Serializes command/disconnect coroutines that access handler-owned game state.
    mutable std::recursive_mutex operation_mutex_;
    uint64_t creation_generation_{};
    std::weak_ptr<Game> creation_game_;

    struct PendingJoin {
        std::shared_ptr<Game> game;
        uint64_t creation_generation{};
        ser::OperationRequestMessage request;
        bool is_encrypted{};
        enet::EnetCommandHeader command_header;
        common::Timer wait_started;
    };
    std::optional<PendingJoin> pending_join_;

    struct PendingAuthentication {
        ser::OperationRequestMessage request;
        bool is_encrypted{};
        enet::EnetCommandHeader command_header;
        common::Timer wait_started;
    };
    std::optional<PendingAuthentication> pending_authentication_;

    void HandleSlowUpdate() override;
};
} // namespace server
