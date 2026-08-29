// Copyright (c) 2026, the Luxon Server contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "global.hpp"
#include "handler_base.hpp"
#include "lobby.hpp"
#include "coro_support.hpp"

#include <unordered_set>
#include <functional>
#include <list>
#include <optional>
#include <vector>
#include <variant>
#include <commoncpp/timer.hpp>
#include <luxon/ser_types.hpp>

namespace server {
struct Lobby;

class MasterServerHandler : public HandlerBase {
public:
    using HandlerBase::HandlerBase;

    void HandleSlowUpdate() override;
    Awaitable<> HandleOperationRequest(ser::OperationRequestMessage&& req, bool is_encrypted, const enet::EnetCommandHeader& cmd_header) override;

    std::shared_ptr<Lobby> get_joined_lobby() {
        if (!joined_lobby_.has_value())
            return {};

        return joined_lobby_->lobby;
    }
    std::string_view get_joined_lobby_name() const { return joined_lobby_.has_value() ? joined_lobby_->lobby->name : std::string_view{}; }

protected:
    struct GameListUpdate {
        enum Type : size_t { Update, Delete };
        std::variant<std::weak_ptr<Game>, std::string> game;
    };

    struct JoinedLobby {
        std::shared_ptr<Lobby> lobby;
        std::list<GameListUpdateHandler>::iterator game_list_update_handler;

        JoinedLobby(std::shared_ptr<Lobby>, GameListUpdateHandler&&);
        ~JoinedLobby();

        bool operator==(const std::shared_ptr<Lobby>& o) const { return lobby == o; }

        JoinedLobby(const JoinedLobby&) = delete;
        JoinedLobby(JoinedLobby&&) = delete;
        JoinedLobby& operator=(const JoinedLobby&) = delete;
        JoinedLobby& operator=(JoinedLobby&&) = delete;
    };

    std::optional<JoinedLobby> joined_lobby_;
    common::Timer last_batched_update_;
    struct PendingJoin {
        std::shared_ptr<Game> game;
        uint64_t creation_generation{};
        ser::OperationRequestMessage request;
        bool is_encrypted{};
        enet::EnetCommandHeader command_header;
        common::Timer wait_started;
    };
    std::optional<PendingJoin> pending_join_;

    // Reservations created by the in-flight request that have not been
    // handed off to a routed response yet. Cleaned up on disconnect so a
    // crashed matchmaking request cannot block capacity for the full TTL.
    std::vector<std::pair<std::weak_ptr<Game>, std::pair<std::string, uint64_t>>> owned_reservations_;

    std::vector<GameListUpdate> pending_game_list_updates_;

    std::expected<std::shared_ptr<Lobby>, ser::OperationResponseMessage> get_requested_lobby(const ser::OperationRequestMessage& req);

    void join_lobby(std::shared_ptr<Lobby> lobby);
    void leave_lobby() { joined_lobby_.reset(); }

    void send_app_stats();
    ser::Dictionary get_lobby_stats(std::function<bool(const Lobby&)> lobby_filter = nullptr);
    void send_lobby_stats();

    Awaitable<> HandleDisconnect() override;

    ser::HashtablePtr get_game_list(Lobby& lobby, const Game& game);
    ser::HashtablePtr get_game_list(Lobby& lobby, std::function<bool(const Game&)> game_filter = nullptr);
    void send_game_list();
};
} // namespace server
