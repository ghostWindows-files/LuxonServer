// Copyright (c) 2026, the Luxon Server contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "peer_persistence.hpp"
#include "server_manager.hpp"
#include "string_hash.hpp"
#include "ipc_codes.hpp"
#include "apps.hpp"
#include "game.hpp"

#include <vector>
#include <random>
#include <algorithm>
#include <atomic>
#include <luxon/common_codes.hpp>
#include <tracy/Tracy.hpp>

namespace server {
namespace {
std::atomic_uint64_t next_store_generation{0};

std::string create_token(size_t length = 32) {
    static std::random_device gen;
    const std::string_view charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::uniform_int_distribution<size_t> dist(0, charset.size() - 1);

    std::string s(length, '\0');
    std::ranges::generate(s, [&] { return charset[dist(gen)]; });
    return s;
}
} // namespace

void store_persistent_peer(ServerManager& server_manager, std::unique_ptr<PeerPersistent>&& pp) {
    ZoneScoped;

    if (!pp->app)
        return;

    // Make sure to not store peer holding an expired game
    pp->reset_owned_game_if_created();

    const std::string token = pp->token;
    std::erase_if(server_manager.peer_persistent_data, [&token](const auto& value) { return value->token == token; });
    const uint64_t store_generation = pp->store_generation =
        next_store_generation.fetch_add(1, std::memory_order_relaxed) + 1;
    if (pp->reconnect_ttl_ms >= 0) {
        const unsigned ttl_ms = static_cast<unsigned>(std::max(30000, pp->reconnect_ttl_ms));
        server_manager.add_scheduled_task(ttl_ms, [&server_manager, token, store_generation]() {
            std::erase_if(server_manager.peer_persistent_data, [&token, store_generation](const auto& value) {
                return value->token == token && value->store_generation == store_generation;
            });
        });
    }
    server_manager.peer_persistent_data.emplace_back(std::move(pp));
}

std::unique_ptr<PeerPersistent> load_persistent_peer(ServerManager& server_manager, std::string_view token, bool refresh_token) {
    ZoneScoped;

    for (auto it = server_manager.peer_persistent_data.begin(); it != server_manager.peer_persistent_data.end(); ++it) {
        if (it->get()->token != token)
            continue;

        auto fres = std::move(*it);
        server_manager.peer_persistent_data.erase(it);
#ifdef LUXON_SERVER_ENABLE_MULTIPROCESSING
        luxon::ser::EventMessage msg;
        msg.event_code = IPCEventCodes::PersistentPeerConsume;
        msg.parameters[DictKeyCodes::LoadBalancing::Token] = fres->token;
        msg.parameters[IPCDictKeyCodes::Revision] = static_cast<int64_t>(fres->store_generation);
        server_manager.ipc_broadcast(msg);
#endif
        if (refresh_token)
            fres->token = create_token();
        return fres;
    }

    return nullptr;
}

std::unique_ptr<PeerPersistent> create_persistent_peer() {
    ZoneScoped;

    auto fres = std::make_unique<PeerPersistent>();
    fres->token = create_token();
    return fres;
}

void reset_persistent_peer_game_ownership(ServerManager& server_manager, Game& game) {
    for (const auto& peer : server_manager.peer_persistent_data) {
        if (peer->owns(game)) {
            peer->reset_owned_game();
            return; // No more than one peer should ever own a game
        }
    }
}

#ifdef LUXON_SERVER_ENABLE_MULTIPROCESSING
void sync_persistent_peer(ServerManager& server_manager, const PeerPersistent& pp) {
    ZoneScoped;

    luxon::ser::EventMessage msg;
    msg.event_code = IPCEventCodes::PersistentPeerStore;
    msg.parameters[DictKeyCodes::LoadBalancing::Token] = pp.token;
    msg.parameters[DictKeyCodes::LoadBalancing::UserId] = pp.user_id;
    msg.parameters[DictKeyCodes::GameSettings::PlayerTTL] = pp.reconnect_ttl_ms;
    msg.parameters[IPCDictKeyCodes::Revision] = static_cast<int64_t>(pp.store_generation);
    if (pp.has_invitation())
        pp.get_invitation().encode_game_info(msg.parameters);
    else if (pp.app)
        pp.app->add_app_info(msg.parameters);
    server_manager.ipc_broadcast(msg);
}
#else
void sync_persistent_peer(ServerManager&, const PeerPersistent&) {}
#endif
} // namespace server
