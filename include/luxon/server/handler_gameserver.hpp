// Copyright (c) 2026, the Luxon Server contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "global.hpp"
#include "handler_base.hpp"
#include "peer_persistence.hpp"
#include "coro_support.hpp"

#include <luxon/ser_types.hpp>

namespace server {
struct GamePeer;
struct Game;

class GameServerHandler : public HandlerBase {
public:   
    using HandlerBase::HandlerBase;

    Awaitable<> HandleDisconnect() override;
    Awaitable<> HandleOperationRequest(ser::OperationRequestMessage&& req, bool is_encrypted, const enet::EnetCommandHeader& cmd_header) override;

    const std::shared_ptr<Game>& get_current_game() const { return current_game_; }

    // Public access for pending join processing
    std::shared_ptr<Peer>& get_peer() { return peer_; }

protected:
    std::shared_ptr<Game> current_game_;
    GamePeer *game_peer_{};
    bool has_left_{};
};
} // namespace server
