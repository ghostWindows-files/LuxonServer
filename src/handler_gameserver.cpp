// Copyright (c) 2026, the Luxon Server contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "handler_gameserver.hpp"
#include "game.hpp"
#ifdef LUXON_SERVER_ENABLE_PLUGINS
#include "game_plugin_registry.hpp"
#include "game_plugin_base.hpp"
#endif
#include "global.hpp"
#include "data_model.hpp"
#include "authentication.hpp"
#include "server_manager.hpp"

#include <algorithm>
#include <mutex>
#include <ranges>
#include <luxon/ser_interface.hpp>
#include <luxon/common_codes.hpp>
#include <tracy/Tracy.hpp>

namespace server {
namespace models {
using namespace DictKeyCodes;

using RaiseEvent = Model<Parameter<std::vector<int32_t>, GameAndActor::ActorList, true>,
                         Parameter<ReceiverGroup::Enum, RoutingAndEvents::ReceiverGroup, false, DefaultConst<ReceiverGroup::Others>>,
                         Parameter<uint8_t, RoutingAndEvents::InterestGroup, false, DefaultConst<0>>,
                         Parameter<CacheOperation::Enum, RoutingAndEvents::Cache, false, DefaultConst<CacheOperation::DoNotCache>>,
                         Parameter<uint8_t, RoutingAndEvents::Code, false, DefaultConst<200>>>;

using JoinOrCreateGame = Model<Parameter<std::string, GameAndActor::GameId>, Parameter<bool, RoutingAndEvents::Broadcast, false, DefaultConst<true>>,
                               Parameter<uint8_t, AuthAndLobby::CreateIfNotExists, false, DefaultConst<false>>,
                               Parameter<std::vector<std::string>, GameProps::ExpectedUsers, false, DefaultInit>,
                               Parameter<std::vector<std::string>, RpcAndPlugins::Plugins, false, DefaultInit>,
                               Parameter<ser::HashtablePtr, Properties::GameProperties, false, DefaultInit>,
                               Parameter<ser::HashtablePtr, Properties::ActorProperties, false, DefaultInit>, Parameter<int32_t, GameSettings::PlayerTTL, true>,
                               Parameter<int32_t, GameSettings::EmptyRoomTTL, true>, Parameter<int32_t, GameSettings::GameFlags, true>,
                               Parameter<bool, GameSettings::CheckUserOnJoin, true>, Parameter<bool, RoutingAndEvents::SuppressRoomEvents, true>,
                               Parameter<bool, RoutingAndEvents::PublishUserId, true>>;

using SetProperties =
    Model<Parameter<ser::HashtablePtr, Properties::Properties, false, DefaultInit>,
          Parameter<ser::HashtablePtr, Properties::ExpectedValues, false, DefaultInit>, Parameter<bool, RoutingAndEvents::Broadcast, false, DefaultConst<true>>,
          Parameter<int32_t, GameAndActor::ActorNo, false, DefaultConst<0>>>;

using ChangeInterestGroups = Model<Parameter<ser::ByteArray, RoutingAndEvents::Add, true>, Parameter<ser::ByteArray, RoutingAndEvents::Remove, true>>;
} // namespace models

void GameServerHandler::HandleSlowUpdate() {
    std::unique_lock operation_lock(operation_mutex_);
    if (pending_authentication_) {
        if (const auto token = pending_authentication_->request.parameters[DictKeyCodes::LoadBalancing::Token].get_ptr<std::string>();
            token && server_manager_.has_persistent_peer(*token)) {
            auto pending_authentication = std::move(*pending_authentication_);
            pending_authentication_.reset();
            operation_lock.unlock();
            HandleOperationRequest(std::move(pending_authentication.request), pending_authentication.is_encrypted,
                                   pending_authentication.command_header)_lco_detached;
            return;
        }

        if (pending_authentication_->wait_started.get() >= 2000) {
            const ser::OperationResponseMessage resp{.operation_code = pending_authentication_->request.operation_code,
                                                     .return_code = ErrorCodes::Auth::AuthenticationTokenExpired,
                                                     .debug_message = "Authentication failure: Got no persistent peer data"};
            send(proto_->Serialize(resp, pending_authentication_->is_encrypted));
            peer_->disconnect();
            pending_authentication_.reset();
            return;
        }
    }

    if (pending_join_) {
        const auto pending_game = pending_join_->game;
        const bool generation_changed = pending_game && pending_join_->creation_generation != 0 &&
                                        pending_game->active_creation_generation() != pending_join_->creation_generation;
        const bool creation_finished = pending_game &&
                                       pending_game->is_created.load(std::memory_order_acquire) &&
                                       !pending_game->is_creating.load(std::memory_order_acquire) &&
                                       !generation_changed;
        const bool creation_aborted = pending_game &&
                                      !pending_game->is_created.load(std::memory_order_acquire) &&
                                      !pending_game->is_creating.load(std::memory_order_acquire);
        if (creation_finished) {
            auto pending_join = std::move(*pending_join_);
            pending_join_.reset();
            current_game_ = pending_join.game;
            operation_lock.unlock();
            HandleOperationRequest(std::move(pending_join.request), pending_join.is_encrypted,
                                   pending_join.command_header)_lco_detached;
            return;
        }

        if (!pending_game || creation_aborted || generation_changed || pending_join_->wait_started.get() >= 5000) {
            const ser::OperationResponseMessage resp{
                .operation_code = pending_join_->request.operation_code,
                .return_code = ErrorCodes::Matchmaking::GameIdNotExists,
                .debug_message = creation_aborted ? "Game creation was aborted" :
                                 (generation_changed ? "Game creation attempt expired" :
                                  (pending_game ? "Game creation timed out" : "Game no longer exists"))};
            send(proto_->Serialize(resp, pending_join_->is_encrypted),
                 enet::EnetSendOptions{.channel = pending_join_->command_header.channel_id});
            pending_join_.reset();
        }
    }

    operation_lock.unlock();
    HandlerBase::HandleSlowUpdate();
}

Awaitable<> GameServerHandler::HandleDisconnect() {
    ZoneScoped;

    std::unique_lock operation_lock(operation_mutex_);
    disconnected_.store(true, std::memory_order_release);
    pending_join_.reset();
    pending_authentication_.reset();

    // Move ownership into a local so the handler state is consistent while
    // plugins await, and the Game stays alive until cleanup finishes.
    if (auto game = std::move(current_game_)) {
        if (const auto creating_game = creation_game_.lock();
            creating_game && creation_generation_ != 0) {
            const bool aborted = creating_game->abort_creation_transaction(peer_,
                                                                           creation_generation_);
            creation_game_.reset();
            creation_generation_ = 0;
            pending_join_.reset();
            game_peer_ = nullptr;
            if (aborted)
                creating_game->trigger_lobby_update();
        }

        if (const auto snapshot = game->get_config_snapshot(); snapshot.peer_count != 0) {
            const int32_t actor_id = game->actor_id_for_peer(peer_).value_or(0);
            if (actor_id != 0 && game->has_peer_actor(actor_id)) {
                // Plugins receive an address-stable node: removed peers are
                // spliced into retired_peers instead of erased by remove_peer().
                GamePeer *leaving_peer = nullptr;
                {
                    std::lock_guard admission_lock(game->admission_mutex);
                    leaving_peer = game->find_peer(actor_id);
                }

                // Cleanup cache if enabled
                if (snapshot.flags & DictKeyCodes::RoutingAndEvents::CleanupCacheOnLeave) {
                    std::lock_guard admission_lock(game->admission_mutex);
                    game->event_cache.remove_if([actor_id](const Event& cached_event) {
                        return cached_event.sender_actor_id == actor_id;
                    });
                }

                if (!has_left_) {
                    // Call into plugins
                    GAME_PLUGINS_INVOKE({
                        OnLeaveGameCallInfo info{.leaver = leaving_peer};
                        ser::OperationRequestMessage req{.operation_code = OpCodes::Lite::Leave};
                        lco_await game->execute_plugin_chain(&PluginBase::OnLeave, req, info);
                    });
                }

                // Remove peer
                peer_->log->info("Removing peer from game...");
                const bool was_master = actor_id == snapshot.master_actor;
                bool removed_peer = false;
                {
                    std::lock_guard admission_lock(game->admission_mutex);
                    removed_peer = game->remove_peer(peer_);
                }
                if (!removed_peer)
                    peer_->log->warn("Failed to remove peer from game");
                game_peer_ = nullptr;
                game->trigger_lobby_update();

                // Broadcast leave event
                if (!(game->get_config_snapshot().flags & GameFlags::SuppressRoomEvents)) {
                    std::vector<int32_t> actor_ids;
                    {
                        std::lock_guard admission_lock(game->admission_mutex);
                        for (auto& game_peer : game->peers)
                            actor_ids.push_back(game_peer.actor_id);
                    }

                    Event event{.code = EventCodes::Leave, .sender_actor_id = actor_id, .receivers = ReceiverGroup::All};
                    event.top_params[DictKeyCodes::GameAndActor::ActorNo] = actor_id;
                    event.top_params[DictKeyCodes::GameAndActor::ActorList] = &actor_ids;
                    if (was_master)
                        event.top_params[DictKeyCodes::MetadataAndMisc::MasterClientId] = game->get_config_snapshot().master_actor;
                    game->broadcast_event(event);
                }
            }
        }

        creation_game_.reset();
        creation_generation_ = 0;
        game_peer_ = nullptr;
        game.reset();
    }

    lco_await HandlerBase::HandleDisconnect();
}

Awaitable<> GameServerHandler::HandleOperationRequest(ser::OperationRequestMessage&& req, bool is_encrypted, const enet::EnetCommandHeader& cmd_header) {
    ZoneScoped;

    std::unique_lock operation_lock(operation_mutex_);

    const auto ensure_joined_state = [&](bool joined = true) {
        bool has_joined_peer = false;
        if (current_game_)
            has_joined_peer = current_game_->actor_id_for_peer(peer_).has_value();
        if (has_joined_peer != joined) {
            const ser::OperationResponseMessage resp{
                .operation_code = req.operation_code, .return_code = ErrorCodes::Core::OperationNotAllowedInCurrentState, .debug_message = "Must join first"};
            send(proto_->Serialize(resp), enet::EnetSendOptions{cmd_header.channel_id});
            return false;
        }
        return true;
    };

    if (!peer_->is_authenticated()) {
        if (cmd_header.channel_id != 0)
            lco_return lco_await HandlerBase::HandleOperationRequest(std::move(req), is_encrypted, cmd_header);

        switch (req.operation_code) {

        case OpCodes::Auth::Authenticate:
        case OpCodes::Auth::AuthenticateOnce: {
            ZoneScopedN("HandleOperationRequest_Authenticate");

            if (const auto token = req.parameters[DictKeyCodes::LoadBalancing::Token].get_ptr<std::string>(); token && !server_manager_.has_persistent_peer(*token)) {
                if (pending_authentication_) {
                    const ser::OperationResponseMessage resp{.operation_code = req.operation_code,
                                                             .return_code = ErrorCodes::Core::OperationNotAllowedInCurrentState,
                                                             .debug_message = "Authentication already pending"};
                    send(proto_->Serialize(resp, is_encrypted), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                    lco_return;
                }
                pending_authentication_.emplace(PendingAuthentication{.request = std::move(req),
                                                                       .is_encrypted = is_encrypted,
                                                                       .command_header = cmd_header});
                lco_return;
            }

            // Try to authenticate
            auto resp = lco_await authenticate(server_manager_, *peer_, req, cmd_header, false);

            // Add position parameter if authentication was successful
            if (resp.return_code == ErrorCodes::Core::Ok)
                resp.parameters[DictKeyCodes::LoadBalancing::Position] = static_cast<int32_t>(0);

            // Send response
            send(proto_->Serialize(resp, is_encrypted));

            // Disconnect on error
            if (!peer_->is_authenticated()) {
                peer_->disconnect();
                lco_return;
            }

            // Set current game
            auto& pp = *peer_->persistent;
            auto expected_game = server_manager_.get_game(pp.get_invitation());
            if (expected_game) {
                pp.reset_owned_game_if_created();
                if (server_manager_.is_game_external(**expected_game)) {
                    peer_->log->error("Peer tried to join an external game! Forcing disconnect.");
                    const ser::OperationResponseMessage resp{.operation_code = req.operation_code,
                                                             .return_code = ErrorCodes::Matchmaking::ServerForbidden,
                                                             .debug_message = "Not invited to server"};
                    send(proto_->Serialize(resp), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                    peer_->disconnect();
                    lco_return;
                } else {
                    current_game_ = std::move(*expected_game);
                }
            } else {
                pp.reset_game();
                peer_->log->error("Persistent peer doesn't have valid game assigned! Forcing disconnect.");
                const ser::OperationResponseMessage resp{.operation_code = req.operation_code,
                                                         .return_code = ErrorCodes::Matchmaking::ServerForbidden,
                                                         .debug_message = "Not invited to any game server"};
                send(proto_->Serialize(resp), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                peer_->disconnect();
                lco_return;
            }

            lco_return;
        }
        }
    } else if (auto game = current_game_) {

        if (req.operation_code == OpCodes::Lite::RaiseEvent) {
            ZoneScopedN("HandleOperationRequest_RaiseEvent");

            using namespace DictKeyCodes::RoutingAndEvents;
            using DictKeyCodes::GameAndActor::ActorList;

            if (!ensure_joined_state())
                lco_return;

            const auto params = models::RaiseEvent::decode(req);
            if (!params) {
                send(proto_->Serialize(params.error()));
                lco_return;
            }

            const auto cache_op = params->get<Cache>();

            // Build event from a stable actor identity. The list node is not an
            // ownership handle and must not be dereferenced after an await.
            const int32_t sender_actor_id = game->actor_id_for_peer(peer_).value_or(0);
            if (sender_actor_id == 0) {
                lco_return;
            }
            Event event;
            event.sender_actor_id = sender_actor_id;
            event.code = params->get<Code>();
            if (auto it = req.parameters.find(Data); it != req.parameters.end())
                event.data = std::move(it->second);
            event.delivery_mode = enet::FlagsToEnetDeliveryMode(cmd_header.flags);
            event.interest_group = params->get<InterestGroup>();
            event.channel = cmd_header.channel_id;
            if (const auto *actors = params->get<ActorList>())
                event.receivers = *actors | std::ranges::to<std::unordered_set>();
            else
                event.receivers = params->get<DictKeyCodes::RoutingAndEvents::ReceiverGroup>();

            // Call into plugins
            GAME_PLUGINS_INVOKE({
                GamePeer *raiser = nullptr;
                {
                    std::lock_guard admission_lock(game->admission_mutex);
                    raiser = game->find_peer(sender_actor_id);
                }
                if (!raiser) {
                    lco_return;
                }
                OnRaiseEventCallInfo info{.raiser = raiser, .event = event, .cache_op = cache_op};
                const Result res = lco_await game->execute_plugin_chain(&PluginBase::OnRaiseEvent, req, info);

                if (res == Result::Cancel)
                    lco_return;
                if (res == Result::Fail) {
                    const ser::OperationResponseMessage resp{.operation_code = OpCodes::Lite::RaiseEvent,
                                                             .return_code = ErrorCodes::Matchmaking::PluginReportedError};
                    send(proto_->Serialize(resp), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                    lco_return;
                }
            });

            // Make sure client isn't attempting to raise a Photon event
            if (event.code > 220) {
                const ser::OperationResponseMessage resp{.operation_code = OpCodes::Lite::RaiseEvent,
                                                         .return_code = ErrorCodes::Core::OperationInvalid,
                                                         .debug_message = "Not allowed to raise Photon events (codes higher than 220)"};
                send(proto_->Serialize(resp), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                lco_return;
            }

            // RemoveFromRoomCache
            if (cache_op == CacheOperation::RemoveFromRoomCache) {
                std::vector<int32_t> filter_senders;
                // Use target actors option to specify the sender number
                if (const auto& actors = params->get<ActorList>())
                    filter_senders = *actors;

                ser::Hashtable filter_data;
                if (event.data.is<ser::HashtablePtr>())
                    if (auto ptr = event.data.get<ser::HashtablePtr>())
                        filter_data = *ptr;

                // Event code 0 is wildcard
                const bool wildcard_code = event.code == 0;

                std::lock_guard admission_lock(game->admission_mutex);
                game->event_cache.remove_if([&](const Event& cached_event) {
                    // Code Filter
                    if (!wildcard_code && cached_event.code != event.code)
                        return false;

                    // Sender Filter
                    if (!filter_senders.empty()) {
                        bool found = false;
                        for (int32_t id : filter_senders) {
                            if (id == cached_event.sender_actor_id) {
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                            return false;
                    }

                    // Data filter (subset match)
                    if (!filter_data.empty())
                        if (!Game::matches_filter(cached_event.data, filter_data))
                            return false;

                    return true;
                });
                lco_return; // Do NOT broadcast removal
            }

            // RemoveFromCacheForActorsLeft
            if (cache_op == CacheOperation::RemoveFromCacheForActorsLeft) {
                std::lock_guard admission_lock(game->admission_mutex);
                game->event_cache.remove_if([&](const Event& cached_event) {
                    return game->find_peer(cached_event.sender_actor_id) == nullptr && cached_event.sender_actor_id != 0; // Don't remove global
                });
                lco_return;
            }

            // Add To Cache
            const bool can_cache = (cache_op == CacheOperation::AddToRoomCache || cache_op == CacheOperation::AddToRoomCacheGlobal);
            if (can_cache && params->get<ActorList>() == nullptr &&
                params->get<DictKeyCodes::RoutingAndEvents::ReceiverGroup>() != ReceiverGroup::MasterClient &&
                params->get<DictKeyCodes::RoutingAndEvents::InterestGroup>() == 0) {
                // Make copy to allow potential change below to happen non-destructively
                Event cached_copy = event;

                if (cache_op == CacheOperation::AddToRoomCacheGlobal)
                    cached_copy.sender_actor_id = 0; // Can not be traced back

                std::lock_guard admission_lock(game->admission_mutex);
                game->event_cache.emplace_back(std::move(cached_copy));
            }

            // Broadcast
            game->broadcast_event(event);
            lco_return;
        }

        if (cmd_header.channel_id != 0)
            lco_return lco_await HandlerBase::HandleOperationRequest(std::move(req), is_encrypted, cmd_header);

        switch (req.operation_code) {

        case OpCodes::Matchmaking::CreateGame:
        case OpCodes::Matchmaking::JoinGame: {
            ZoneScopedN("HandleOperationRequest_JoinGame");

            // Common validation
            if (!ensure_joined_state(false))
                lco_return;

            const auto params = models::JoinOrCreateGame::decode(req);
            if (!params) {
                send(proto_->Serialize(params.error()));
                lco_return;
            }

            const uint8_t join_mode = params->get<DictKeyCodes::AuthAndLobby::CreateIfNotExists>();
            const bool can_create = req.operation_code == OpCodes::Matchmaking::CreateGame || join_mode == 1;

            // Validate game ID
            if (params->get<DictKeyCodes::GameAndActor::GameId>() != game->id) {
                const ser::OperationResponseMessage resp{.operation_code = req.operation_code,
                                                         .return_code = ErrorCodes::Matchmaking::GameIdNotExists,
                                                         .debug_message = "Token not valid for this Game ID"};
                send(proto_->Serialize(resp), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                lco_return;
            }

            // A creator may still be running before the room is ready. Queue
            // every other request first so ordinary JoinGame is not rejected during
            // that window.
            const bool creation_in_progress = game->is_creating.load(std::memory_order_acquire);
            if (creation_in_progress) {
                if (pending_join_) {
                    const ser::OperationResponseMessage resp{.operation_code = req.operation_code,
                                                             .return_code = ErrorCodes::Core::OperationNotAllowedInCurrentState,
                                                             .debug_message = "Join already pending"};
                    send(proto_->Serialize(resp), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                    lco_return;
                }

                peer_->log->debug("Room is being initialized, retaining join request for later processing");
                pending_join_.emplace(PendingJoin{.game = game,
                                                  .creation_generation = game->active_creation_generation(),
                                                  .request = std::move(req),
                                                  .is_encrypted = is_encrypted,
                                                  .command_header = cmd_header});
                lco_return;
            }

            // A normal JoinGame may never claim an uninitialized placeholder.
            // Only CreateGame or JoinGame(CreateIfNotExists=1) may initialize it.
            if (!game->is_created.load(std::memory_order_acquire) && !can_create) {
                const ser::OperationResponseMessage resp{.operation_code = req.operation_code,
                                                         .return_code = ErrorCodes::Matchmaking::GameIdNotExists,
                                                         .debug_message = "Game does not exist"};
                send(proto_->Serialize(resp, is_encrypted),
                     enet::EnetSendOptions{.channel = cmd_header.channel_id});
                lco_return;
            }

            // Claim creation before any await. Other handlers keep their complete
            // request and retry the same path after initialization finishes.
            const auto creation_owner = peer_;
            const uint64_t creation_generation = game->try_begin_creation(creation_owner);
            const bool is_master = creation_generation != 0;
            if (is_master) {
                creation_generation_ = creation_generation;
                creation_game_ = game;
            }
            if (!is_master && !game->is_created.load(std::memory_order_acquire)) {
                if (game->is_creating.load(std::memory_order_acquire)) {
                    if (pending_join_) {
                        const ser::OperationResponseMessage resp{.operation_code = req.operation_code,
                                                                 .return_code = ErrorCodes::Core::OperationNotAllowedInCurrentState,
                                                                 .debug_message = "Join already pending"};
                        send(proto_->Serialize(resp, is_encrypted),
                             enet::EnetSendOptions{.channel = cmd_header.channel_id});
                        lco_return;
                    }
                    pending_join_.emplace(PendingJoin{.game = game,
                                                      .creation_generation = game->active_creation_generation(),
                                                      .request = std::move(req),
                                                      .is_encrypted = is_encrypted,
                                                      .command_header = cmd_header});
                    lco_return;
                }

                const ser::OperationResponseMessage resp{.operation_code = req.operation_code,
                                                         .return_code = ErrorCodes::Matchmaking::GameIdNotExists,
                                                         .debug_message = can_create ? "Game creation was not completed" : "Game does not exist"};
                send(proto_->Serialize(resp, is_encrypted),
                     enet::EnetSendOptions{.channel = cmd_header.channel_id});
                lco_return;
            }

            bool creation_aborted = false;
            bool creation_committed = false;
            const uint64_t request_generation = creation_generation;
            std::vector<std::pair<std::string, uint64_t>> added_expected_users;
            const auto rollback_expected_users = [&]() {
                for (const auto& [expected_user, generation] : added_expected_users)
                    game->remove_expected_user_if_generation(expected_user, generation);
                added_expected_users.clear();
            };
            const auto rollback_join = [&]() -> void {
                // A committed join is owned by normal disconnect cleanup; never
                // erase its peer through the pre-commit rollback path.
                if (creation_committed || creation_aborted)
                    return;

                if (is_master) {
                    const bool aborted = game->abort_creation_transaction(creation_owner,
                                                                           request_generation);
                    creation_aborted = true;
                    game_peer_ = nullptr;
                    creation_game_.reset();
                    creation_generation_ = 0;
                    if (aborted)
                        game->trigger_lobby_update();
                    return;
                }

                creation_aborted = true;
                if (game_peer_)
                    game->remove_peer(peer_);
                game_peer_ = nullptr;
                rollback_expected_users();
            };
            auto ensure_reset = std::shared_ptr<void>(nullptr, [&](void*) {
                if (!creation_committed && !creation_aborted)
                    rollback_join();
            });

            if (disconnected_.load(std::memory_order_acquire)) {
                rollback_join();
                lco_return;
            }

            if (is_master) {
#ifdef LUXON_SERVER_ENABLE_PLUGINS
                if (game->plugins.empty())
#endif
                {
                    // Load given plugins if creating room
                    for (const std::string& plugin_name : params->get<DictKeyCodes::RpcAndPlugins::Plugins>()) {
#ifdef LUXON_SERVER_ENABLE_PLUGINS
                        if (disconnected_.load(std::memory_order_acquire)) {
                            rollback_join();
                            lco_return;
                        }
                        auto plugin = game_plugins::registry::instantiate(game.get(), plugin_name);
                        if (!plugin) {
                            peer_->log->warn("Attempting to load unknown game plugin: {}", plugin_name);
                            continue;
                        }

                        lco_await game->plugins.emplace_back(std::move(plugin))->OnAttach();
                        if (disconnected_.load(std::memory_order_acquire)) {
                            rollback_join();
                            lco_return;
                        }
#else
                peer_->log->warn("Attempting to load game plugin when plugins are disabled: {}", plugin_name);
#endif
                    }
                }
            } else {
                // Verify join if joining existing room
                const auto [join_validation_code, join_validation_message] = game->validate_join(peer_->persistent->user_id);
                if (join_validation_code != ErrorCodes::Core::Ok) {
                    const ser::OperationResponseMessage resp{.operation_code = OpCodes::Matchmaking::JoinGame,
                                                             .return_code = join_validation_code,
                                                             .debug_message = std::string(join_validation_message)};
                    send(proto_->Serialize(resp, is_encrypted), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                    lco_return;
                }
            }

            // Call into plugins
            GAME_PLUGINS_INVOKE({
                if (disconnected_.load(std::memory_order_acquire)) {
                    rollback_join();
                    lco_return;
                }
                Result res;

                if (!game->is_created.load(std::memory_order_acquire)) {
                    OnCreateGameCallInfo info{.creator = peer_,
                                              .is_join = req.operation_code == OpCodes::Matchmaking::JoinGame,
                                              .create_if_not_exist = static_cast<bool>(params->get<DictKeyCodes::AuthAndLobby::CreateIfNotExists>())};
                    res = lco_await game->execute_plugin_chain(&PluginBase::OnCreateGame, req, info);
                } else {
                    BeforeJoinGameCallInfo info{.joiner = peer_};
                    res = lco_await game->execute_plugin_chain(&PluginBase::BeforeJoin, req, info);
                }

                if (disconnected_.load(std::memory_order_acquire)) {
                    rollback_join();
                    lco_return;
                }

                if (res != Result::Continue) {
                    rollback_join();
                    const ser::OperationResponseMessage resp{.operation_code = req.operation_code,
                                                             .return_code = res == Result::Cancel
                                                                                ? ErrorCodes::Core::OperationNotAllowedInCurrentState
                                                                                : ErrorCodes::Matchmaking::PluginReportedError};
                    send(proto_->Serialize(resp, is_encrypted), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                    lco_return;
                }
            });

            if (is_master) {
                // Apply game settings before validating capacity. MaxPlayers must
                // participate in the first join's admission decision. Keep the
                // lease check and mutations in one admission critical section so
                // disconnect cannot interleave a stale creation attempt.
                bool settings_applied = false;
                {
                    std::lock_guard admission_lock(game->admission_mutex);
                    if (game->is_creation_active(creation_owner, request_generation)) {
                        if (auto player_ttl = params->get<DictKeyCodes::GameSettings::PlayerTTL>())
                            game->player_ttl = *player_ttl;
                        if (auto empty_room_ttl = params->get<DictKeyCodes::GameSettings::EmptyRoomTTL>())
                            game->empty_game_ttl = *empty_room_ttl;

                        if (auto flags = params->get<DictKeyCodes::GameSettings::GameFlags>())
                            game->flags = *flags;

                        auto set_flag = [&](int32_t flag, std::optional<bool> value) {
                            if (!value)
                                return;
                            if (*value)
                                game->flags |= flag;
                            else
                                game->flags &= ~flag;
                        };

                        set_flag(GameFlags::CheckUserOnJoin, params->get<DictKeyCodes::GameSettings::CheckUserOnJoin>());
                        set_flag(GameFlags::SuppressRoomEvents, params->get<DictKeyCodes::RoutingAndEvents::SuppressRoomEvents>());
                        set_flag(GameFlags::PublishUserId, params->get<DictKeyCodes::RoutingAndEvents::PublishUserId>());

                        if (const auto& game_props = params->get<DictKeyCodes::Properties::GameProperties>())
                            if (const auto max_players = game_props->find(GameProps::MaxPlayers); max_players != game_props->end())
                                max_players->second.store_if<uint8_t>(game->max_peers);
                        ++game->state_revision_;
                        settings_applied = true;
                    }
                }
                if (!settings_applied) {
                    rollback_join();
                    lco_return;
                }
            }

            if (disconnected_.load(std::memory_order_acquire) ||
                (is_master && !game->is_creation_active(creation_owner, request_generation))) {
                rollback_join();
                lco_return;
            }

            const auto& expected_users = params->get<GameProps::ExpectedUsers>();
            std::unique_lock admission_lock(game->admission_mutex);
            if (is_master && !game->is_creation_active(creation_owner, request_generation)) {
                admission_lock.unlock();
                rollback_join();
                lco_return;
            }
            const auto [expected_users_code, expected_users_message] =
                game->validate_join(peer_->persistent->user_id,
                                    expected_users, is_master);
            if (expected_users_code != ErrorCodes::Core::Ok) {
                admission_lock.unlock();
                rollback_join();
                peer_->disconnect();
                const ser::OperationResponseMessage resp{.operation_code = req.operation_code,
                                                         .return_code = expected_users_code,
                                                         .debug_message = std::string(expected_users_message)};
                send(proto_->Serialize(resp, is_encrypted), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                lco_return;
            }

            for (const auto& expected_user : expected_users) {
                if (expected_user.empty() || expected_user == peer_->persistent->user_id ||
                    game->find_peer(peer_))
                    continue;
                if (const auto reservation_generation = game->reserve_expected_user_with_generation(expected_user))
                    added_expected_users.emplace_back(expected_user, *reservation_generation);
            }

            // Create peer for game. Keep the local shared pointer stable across
            // plugin awaits and disconnect callbacks.
            auto game_peer = game->create_peer(peer_);
            if (!game_peer.is_valid()) {
                peer_->log->error("Game peer could not be created. Connection must terminate now.");
                admission_lock.unlock();
                rollback_join();
                peer_->disconnect();
                const ser::OperationResponseMessage resp{.operation_code = req.operation_code,
                                                         .return_code = ErrorCodes::Matchmaking::GameFull,
                                                         .debug_message = "Unable to allocate an actor"};
                send(proto_->Serialize(resp, is_encrypted), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                lco_return;
            }

            // Add peer to game
            if (disconnected_.load(std::memory_order_acquire) ||
                (is_master && !game->is_creation_active(creation_owner, request_generation))) {
                admission_lock.unlock();
                rollback_join();
                lco_return;
            }
            game_peer_ = is_master
                              ? game->add_peer_for_creation(std::move(game_peer), creation_owner, request_generation)
                              : game->add_peer(std::move(game_peer));
            if (!game_peer_) {
                peer_->log->error("Player could not be added to game. Connection must terminate now.");
                admission_lock.unlock();
                rollback_join();
                peer_->disconnect();
                const ser::OperationResponseMessage resp{.operation_code = req.operation_code,
                                                         .return_code = ErrorCodes::Matchmaking::GameFull,
                                                         .debug_message = "Unable to add player to game"};
                send(proto_->Serialize(resp, is_encrypted), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                lco_return;
            }
            peer_->log->info("Successfully joined game: {}", game->id);
            admission_lock.unlock();
            game->trigger_lobby_update();

            // Call into plugins
            bool broadcast_actor_props = true;
            GAME_PLUGINS_INVOKE({
                if (disconnected_.load(std::memory_order_acquire)) {
                    rollback_join();
                    lco_return;
                }
                int32_t joined_actor_id = 0;
                GamePeer *joiner = nullptr;
                {
                    std::lock_guard admission_lock(game->admission_mutex);
                    joiner = game->find_peer(peer_);
                    if (joiner)
                        joined_actor_id = joiner->actor_id;
                }
                if (!joiner) {
                    rollback_join();
                    lco_return;
                }
                OnJoinGameCallInfo info{.joiner = joiner};
                const Result res = lco_await game->execute_plugin_chain(&PluginBase::OnJoinGame, req, info);
                if (disconnected_.load(std::memory_order_acquire)) {
                    rollback_join();
                    lco_return;
                }
                // The list node pointer handed to the plugin may have been
                // retired while awaiting; re-resolve through the actor id.
                const auto current_actor = game->actor_id_for_peer(peer_);
                game_peer_ = current_actor && *current_actor == joined_actor_id
                                 ? game->find_peer(joined_actor_id)
                                 : nullptr;
                if (!game_peer_) {
                    rollback_join();
                    if (is_master)
                        peer_->disconnect();
                    lco_return;
                }

                if (res != Result::Continue) {
                    peer_->log->info("Reverting join: {}", game->id);
                    bool removed_peer = false;
                    if (!is_master) {
                        std::lock_guard admission_lock(game->admission_mutex);
                        removed_peer = game->remove_peer(peer_);
                    }
                    if (removed_peer)
                        game->trigger_lobby_update();
                    game_peer_ = nullptr;
                    rollback_join();
                    if (is_master)
                        peer_->disconnect();

                    const ser::OperationResponseMessage resp{.operation_code = req.operation_code,
                                                             .return_code = res == Result::Cancel
                                                                                ? ErrorCodes::Core::OperationNotAllowedInCurrentState
                                                                                : ErrorCodes::Matchmaking::PluginReportedError};
                    send(proto_->Serialize(resp, is_encrypted), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                    lco_return;
                }

                if (!info.publish_user_id.value_or(true)) {
                    std::lock_guard admission_lock(game->admission_mutex);
                    if (auto *current_peer = game->find_peer(joined_actor_id))
                        current_peer->actor_props.erase(ActorProps::UserId);
                }

                broadcast_actor_props = info.broadcast_actor_props.value_or(true);
            });

            // Update properties - CRITICAL FIX: Insert properties BEFORE capturing actor props snapshot
            if (disconnected_.load(std::memory_order_acquire) ||
                (is_master && !game->is_creation_active(creation_owner, request_generation))) {
                rollback_join();
                lco_return;
            }
            int32_t joined_actor_id = 0;
            {
                std::lock_guard admission_lock(game->admission_mutex);
                if (const auto *current_peer = game->find_peer(peer_))
                    joined_actor_id = current_peer->actor_id;
                if (joined_actor_id == 0) {
                    rollback_join();
                    lco_return;
                }
                if (const auto& actor_props = params->get<DictKeyCodes::Properties::ActorProperties>())
                    game->insert_actor_props(joined_actor_id, *actor_props);
                if (is_master)
                    if (const auto& game_props = params->get<DictKeyCodes::Properties::GameProperties>())
                        game->insert_game_props(*game_props);
            }
            if (disconnected_.load(std::memory_order_acquire) ||
                (is_master && !game->is_creation_active(creation_owner, request_generation)) ||
                joined_actor_id == 0) {
                rollback_join();
                lco_return;
            }

            // CRITICAL FIX: Capture actor props AFTER inserting this player's properties
            // This ensures all other players receive the complete property list including the new player
            ser::Hashtable all_actor_props;
            {
                std::lock_guard admission_lock(game->admission_mutex);
                all_actor_props = game->get_actor_props();
            }

            if (is_master) {
                if (disconnected_.load(std::memory_order_acquire) ||
                    !game->commit_creation(creation_owner,
                                           creation_generation)) {
                    rollback_join();
                    lco_return;
                }
                creation_committed = true;
                creation_game_.reset();
                creation_generation_ = 0;
                game->trigger_lobby_update();
            } else {
                creation_committed = true;
            }

            if (disconnected_.load(std::memory_order_acquire) || !game_peer_) {
                rollback_join();
                lco_return;
            }

            // Construct response
            if (joined_actor_id == 0) {
                rollback_join();
                lco_return;
            }
            ser::OperationResponseMessage resp;
            resp.operation_code = req.operation_code;
            resp.return_code = ErrorCodes::Core::Ok;

            const auto post_commit_state = game->get_config_snapshot();
            std::vector<int32_t> actor_ids;
            ser::Hashtable game_props;
            int32_t actor_no = 0;
            ser::Hashtable joined_actor_props;
            {
                std::lock_guard admission_lock(game->admission_mutex);
                for (auto& game_peer : game->peers)
                    actor_ids.push_back(game_peer.actor_id);
                if (const auto *current_peer = game->find_peer(joined_actor_id)) {
                    actor_no = current_peer->actor_id;
                    joined_actor_props = current_peer->actor_props;
                }
                game_props = game->get_game_props();
            }

            resp.parameters[DictKeyCodes::GameSettings::GameFlags] = static_cast<int32_t>(post_commit_state.flags);
            resp.parameters[DictKeyCodes::GameAndActor::ActorList] = &actor_ids;
            resp.parameters[DictKeyCodes::Properties::GameProperties] = std::move(game_props);
            resp.parameters[DictKeyCodes::GameAndActor::ActorNo] = actor_no;
            if (broadcast_actor_props)
                resp.parameters[DictKeyCodes::Properties::ActorProperties] = &all_actor_props;

            send(proto_->Serialize(resp, is_encrypted), enet::EnetSendOptions{.channel = cmd_header.channel_id});

            // Broadcast Join Event
            if (!disconnected_.load(std::memory_order_acquire) && actor_no != 0 &&
                !(post_commit_state.flags & GameFlags::SuppressRoomEvents)) {
                Event event{.code = EventCodes::Join, .sender_actor_id = actor_no, .receivers = ReceiverGroup::All};
                event.top_params[DictKeyCodes::GameAndActor::ActorList] = &actor_ids;
                event.top_params[DictKeyCodes::GameAndActor::ActorNo] = actor_no;
                if (broadcast_actor_props && !(post_commit_state.flags & GameFlags::SuppressPlayerInfo))
                    event.top_params[DictKeyCodes::Properties::ActorProperties] = std::move(joined_actor_props);

                game->broadcast_event(event);
            }

            // Flood the client with current state using a stable actor identity.
            if (!disconnected_.load(std::memory_order_acquire) && actor_no != 0)
                game->flood_peer_by_actor(actor_no);

            lco_return;
        }

        case OpCodes::Lite::Leave: {
            ZoneScopedN("HandleOperationRequest_Leave");

            // Call into plugins using a freshly resolved node; its address is
            // never used by handler code after the await.
            GAME_PLUGINS_INVOKE({
                GamePeer *leaver = nullptr;
                {
                    std::lock_guard admission_lock(game->admission_mutex);
                    leaver = game->find_peer(peer_);
                }
                if (!leaver)
                    lco_return;
                OnLeaveGameCallInfo info{.leaver = leaver};
                const Result res = lco_await game->execute_plugin_chain(&PluginBase::OnLeave, req, info);

                if (res == Result::Fail) {
                    const ser::OperationResponseMessage resp{.operation_code = OpCodes::Lite::Leave, .return_code = ErrorCodes::Matchmaking::PluginReportedError};
                    send(proto_->Serialize(resp, is_encrypted), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                    lco_return;
                }
            });

            // Send success response
            const ser::OperationResponseMessage resp{.operation_code = OpCodes::Lite::Leave, .return_code = ErrorCodes::Core::Ok};
            send(proto_->Serialize(resp, is_encrypted), enet::EnetSendOptions{.channel = cmd_header.channel_id});

            // Disconnect, handler will do the rest
            has_left_ = true;
            peer_->disconnect();

            lco_return;
        }

        case OpCodes::Lite::SetProperties: {
            ZoneScopedN("HandleOperationRequest_SetProperties");

            if (!ensure_joined_state())
                lco_return;

            const auto params = models::SetProperties::decode(req);
            if (!params) {
                send(proto_->Serialize(params.error()));
                lco_return;
            }

            bool broadcast = params->get<DictKeyCodes::RoutingAndEvents::Broadcast>();
            auto actor_id = params->get<DictKeyCodes::GameAndActor::ActorNo>();

            const auto& props = params->get<DictKeyCodes::Properties::Properties>();
            const auto& props_expected = params->get<DictKeyCodes::Properties::ExpectedValues>();

            // CRITICAL FIX: Validate properties are not empty or null
            if (!props || props->empty()) {
                peer_->log->warn("SetProperties received empty properties hashtable");
                const ser::OperationResponseMessage resp{.operation_code = OpCodes::Lite::SetProperties,
                                                     .return_code = ErrorCodes::Core::OperationInvalid,
                                                     .debug_message = "Properties cannot be empty"};
                send(proto_->Serialize(resp, is_encrypted), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                lco_return;
            }

            const int32_t sender_actor_id = game->actor_id_for_peer(peer_).value_or(0);
            if (sender_actor_id == 0)
                lco_return;

            // Call into plugins
            GAME_PLUGINS_INVOKE({
                GamePeer *setter = nullptr;
                {
                    std::lock_guard admission_lock(game->admission_mutex);
                    setter = game->find_peer(peer_);
                }
                if (!setter)
                    lco_return;
                BeforeSetPropertiesCallInfo info{
                    .setter = setter, .broadcast = broadcast, .target_actor_id = actor_id, .update = props, .expected = props_expected};
                const Result res = lco_await game->execute_plugin_chain(&PluginBase::BeforeSetProperties, req, info);

                if (res == Result::Fail) {
                    const ser::OperationResponseMessage resp{.operation_code = OpCodes::Lite::SetProperties,
                                                         .return_code = ErrorCodes::Matchmaking::PluginReportedError};
                    send(proto_->Serialize(resp, is_encrypted), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                    lco_return;
                }

                broadcast = info.broadcast;
                actor_id = info.target_actor_id;
            });

            // Set actor or game properties
            bool ok = true;
            if (actor_id) {
                ok = game->apply_actor_props(actor_id, *props,
                                             props_expected ? std::optional<ser::Hashtable>{*props_expected} : std::nullopt);
            } else {
                ok = game->apply_game_props(*props,
                                            props_expected ? std::optional<ser::Hashtable>{*props_expected} : std::nullopt);
            }

            // Empty response
            ser::OperationResponseMessage resp;
            resp.operation_code = OpCodes::Lite::SetProperties;
            resp.return_code = ok ? ErrorCodes::Core::Ok : ErrorCodes::Core::OperationInvalid;
            send(proto_->Serialize(resp, is_encrypted), enet::EnetSendOptions{.channel = cmd_header.channel_id});

            // Broadcast property updates - CRITICAL FIX: MUST broadcast immediately after property update
            // to ensure atomicity: all clients receive property update event before other events
            if (ok && broadcast) {
                auto event = game->create_property_update_event(sender_actor_id, *props, actor_id);
                game->broadcast_event(event);
            }

            // Call into plugins
            GAME_PLUGINS_INVOKE({
                GamePeer *setter = nullptr;
                {
                    std::lock_guard admission_lock(game->admission_mutex);
                    setter = game->find_peer(sender_actor_id);
                }
                if (!setter)
                    lco_return;
                OnSetPropertiesCallInfo info{
                    .setter = setter, .broadcast = broadcast, .target_actor_id = actor_id, .update = props, .expected = props_expected};
                const Result res = lco_await game->execute_plugin_chain(&PluginBase::OnSetProperties, req, info);

                if (res == Result::Fail)
                    peer_->log->error("Plugin reported error for SetProperties after properties were already set");
            });

            lco_return;
        }

        case OpCodes::Lite::ChangeInterestGroups: {
            ZoneScopedN("HandleOperationRequest_ChangeInterestGroups");

            if (!ensure_joined_state())
                lco_return;

            const auto params = models::ChangeInterestGroups::decode(req);
            if (!params) {
                send(proto_->Serialize(params.error()));
                lco_return;
            }

            const std::vector<uint8_t> adds = params->get<DictKeyCodes::RoutingAndEvents::Add>()
                                                  ? std::vector<uint8_t>(params->get<DictKeyCodes::RoutingAndEvents::Add>()->begin(),
                                                                         params->get<DictKeyCodes::RoutingAndEvents::Add>()->end())
                                                  : std::vector<uint8_t>{};
            const std::vector<uint8_t> removes = params->get<DictKeyCodes::RoutingAndEvents::Remove>()
                                                     ? std::vector<uint8_t>(params->get<DictKeyCodes::RoutingAndEvents::Remove>()->begin(),
                                                                            params->get<DictKeyCodes::RoutingAndEvents::Remove>()->end())
                                                     : std::vector<uint8_t>{};
            if (!game->apply_interest_groups(peer_, adds, removes))
                lco_return;

            lco_return;
        }
        }
    } else if (allow_unsolicited_) {
        // Unsolicited join or create
        if (req.operation_code == OpCodes::Matchmaking::JoinGame || req.operation_code == OpCodes::Matchmaking::CreateGame) {
            const auto params = models::JoinOrCreateGame::decode(req);
            if (!params) {
                send(proto_->Serialize(params.error()));
                lco_return;
            }

            // Find or create game
            auto& app = *peer_->persistent->app;
            const auto& app_games = app.get_games();
            auto game_id = params->get<DictKeyCodes::GameAndActor::GameId>();

            std::shared_ptr<Game> target_game;
            if (auto game_res = app_games.find(game_id); game_res != app_games.end()) {
                target_game = game_res->second.lock();
            } else {
                bool should_create = (req.operation_code == OpCodes::Matchmaking::CreateGame) || params->get<DictKeyCodes::AuthAndLobby::CreateIfNotExists>();

                if (should_create) {
                    auto new_game = app.get_lobby()->create_game(std::string(game_id), "", true);
                    if (!new_game) {
                        send(proto_->Serialize(new_game.error()));
                        lco_return;
                    }
                    target_game = *new_game;
                } else {
                    const ser::OperationResponseMessage resp{
                        .operation_code = req.operation_code, .return_code = ErrorCodes::Matchmaking::GameIdNotExists, .debug_message = "Game does not exist"};
                    send(proto_->Serialize(resp, is_encrypted), enet::EnetSendOptions{.channel = cmd_header.channel_id});
                    lco_return;
                }
            }

            // Set as current game and disallow unsolicited join to prevent infinite recursion
            peer_->persistent->invite(std::move(target_game), req.operation_code == OpCodes::Matchmaking::CreateGame);
            allow_unsolicited_ = false;

            // Now that invitation is populated it will execute main logic block
            lco_return lco_await HandleOperationRequest(std::move(req), is_encrypted, cmd_header);
        }
    }

    lco_return lco_await HandlerBase::HandleOperationRequest(std::move(req), is_encrypted, cmd_header);
}

} // namespace server
