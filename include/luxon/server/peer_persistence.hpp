// Copyright (c) 2026, the Luxon Server contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "game.hpp"
#include "apps.hpp"
#include "pfr_pack.hpp"

#include <string>
#include <string_view>
#include <memory>

namespace server {
class ServerManager;
struct App;
class IPC;

class PeerPersistent {
    GameInfo invitation{};
    std::string invitation_buf;
    std::shared_ptr<Game> owned_game;

public:
    std::shared_ptr<App> app;
    std::string user_id, token;
    int32_t reconnect_ttl_ms = 30000;
    uint64_t store_generation{};

    void reset_invitation() {
        invitation = {};
        invitation_buf.clear();
    }
    void reset_owned_game() { owned_game.reset(); }
    void reset_game() {
        reset_invitation();
        reset_owned_game();
    }

    const GameInfo& get_invitation() const { return invitation; }
    bool has_invitation() const { return invitation.has_game_info(); }
    bool owns(Game& game) const {
        return owned_game.get() == &game;
    }
    void reset_owned_game_if_created() {
        if (owned_game &&
            owned_game->is_created.load(std::memory_order_acquire) &&
            !owned_game->is_creating.load(std::memory_order_acquire))
            owned_game.reset();
    }

    bool invite(std::shared_ptr<Game> game, bool is_creating) {
        if (!invite(game))
            return false;
        if (is_creating)
            owned_game = std::move(game);

        return true;
    }

    bool invite(const std::shared_ptr<Game>& game) {
        invitation = game->get_game_info();
        pack_and_relink::string_views(invitation, invitation_buf);
        return true;
    }

    bool invite(const GameInfo& game_info) {
        invitation = game_info;
        pack_and_relink::string_views(invitation, invitation_buf);
        return true;
    }
};

void store_persistent_peer(ServerManager& server_manager, std::unique_ptr<PeerPersistent>&& pp);
std::unique_ptr<PeerPersistent> load_persistent_peer(ServerManager& server_manager, std::string_view token, bool refresh_token = true);
std::unique_ptr<PeerPersistent> create_persistent_peer();
void reset_persistent_peer_game_ownership(ServerManager& server_manager, Game& game);
void sync_persistent_peer(ServerManager& server_manager, const PeerPersistent& pp);
} // namespace server
