// Copyright (c) 2026, the Luxon Server contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "game.hpp"
#include "global.hpp"
#include "logger.hpp"
#include "server_manager.hpp"
#include "peer_persistence.hpp"
#include "ipc_codes.hpp"
#include "coro_support.hpp"

#include <luxon/ser_interface.hpp>
#include <luxon/common_codes.hpp>
#include <tracy/Tracy.hpp>

namespace server {
void GameInfo::encode_game_info(ser::ParameterList& params) const {
    if (has_game_info()) {
        params[DictKeyCodes::GameAndActor::GameId] = std::string(id);
        params[DictKeyCodes::LoadBalancing::Address] = std::string(server_address);
    }
    lobby.encode_lobby_info(params);
}

bool GamePeer::disconnect() {
    if (auto peer_ = peer.lock()) {
        peer_->disconnect();
        return true;
    }

    return false;
}

std::expected<ser::ByteArray, ser::Error> Event::get_cached_data(ser::IProtocol& protocol) const {
    ZoneScoped;

    std::lock_guard cache_lock(*cache_mutex);
    const auto protocol_index = static_cast<size_t>(protocol.GetProtcolImplID());

    // Return the already serialized network packet for this protocol
    ser::ByteArray *cache_ptr{};
    if (protocol_index < cached_data.size()) {
        auto& cache = cached_data[protocol_index];
        if (!cache.empty())
            return cache;
        cache_ptr = &cache;
    } else {
        create_logger("Event::get_cached_data")
            ->warn("Trying to use protocol index {}, but maximum cache index is {}! THIS IS A BUG IN LUXON SERVER, PLEASE REPORT!", protocol_index,
                   cached_data.size());
    }

    // Build the base event message
    ser::EventMessage event_data{.event_code = code, .parameters = top_params};
    event_data.parameters[DictKeyCodes::GameAndActor::ActorNo] = static_cast<int32_t>(sender_actor_id);

    if (!data.is_null())
        event_data.parameters[DictKeyCodes::RoutingAndEvents::Data] = data.as_ref();

    // Serialize the complete packet
    auto expected_payload = protocol.Serialize(event_data, false);
    if (!expected_payload)
        return std::unexpected(expected_payload.error());

    // Cache it if it's a known protocol
    if (cache_ptr)
        *cache_ptr = *expected_payload;

    return *expected_payload;
}

Game::~Game() {
    // Call into plugins
    GAME_PLUGINS_INVOKE({
        OnCloseGameCallInfo info{.failed_on_create = !is_created.load(std::memory_order_acquire)};
        execute_plugin_chain(&PluginBase::OnCloseGame, info);
    });

    auto& server_manager = get_server_manager();
#ifdef LUXON_SERVER_ENABLE_MULTIPROCESSING
    ser::EventMessage ipc_event;
    ipc_event.event_code = IPCEventCodes::GameDelete;
    add_game_info(ipc_event.parameters);
    server_manager.ipc_broadcast(ipc_event);
#endif

    server_manager.get_logger().info("Game '{}' in app '{}' is being deleted", lobby->app->id, id);
}

Game::Game(std::shared_ptr<Lobby> lobby, std::string id, std::string_view server_address)
    : lobby(std::move(lobby)), id(std::move(id)), server_address(get_server_manager().get_static_endpoint_address_str(server_address)) {}

uint64_t Game::try_begin_creation(const std::shared_ptr<Peer>& owner) {
    std::lock_guard admission_lock(admission_mutex);
    std::lock_guard state_lock(creation_state_mutex_);

    if (!owner || is_created.load(std::memory_order_acquire) ||
        is_creating.load(std::memory_order_acquire) || active_peer_count() != 0)
        return 0;

    // Capture every rollback value before publishing the creation lease. If a
    // copy allocation throws, the room remains in its pre-creation state rather
    // than being left permanently marked as creating.
    creation_initial_is_created_ = is_created.load(std::memory_order_relaxed);
    creation_initial_flags_ = flags;
    creation_initial_is_open_ = is_open;
    creation_initial_is_visible_ = is_visible;
    creation_initial_player_ttl_ = player_ttl;
    creation_initial_empty_game_ttl_ = empty_game_ttl;
    creation_initial_max_peers_ = max_peers;
    creation_initial_master_actor_ = master_actor;
    creation_initial_custom_props_ = custom_props;
    creation_initial_lobby_props_ = lobby_props;
    creation_initial_last_actor_id_ = last_actor_id;
    creation_initial_dummy_peer_count_ = dummy_peer_count;
    creation_initial_event_cache_ = event_cache;
    creation_initial_expected_users_ = expected_users;
    creation_initial_expected_user_generations_ = expected_user_generations;
    creation_initial_inactive_players_ = inactive_players;
#ifdef LUXON_SERVER_ENABLE_PLUGINS
    creation_initial_plugin_count_ = plugins.size();
#endif

    bool expected = false;
    if (!is_creating.compare_exchange_strong(expected, true,
                                             std::memory_order_acq_rel,
                                             std::memory_order_acquire))
        return 0;

    active_creation_generation_ = ++next_creation_generation_;
    if (active_creation_generation_ == 0)
        active_creation_generation_ = ++next_creation_generation_;
    ++state_revision_;

    creation_owner_ = owner;
    creation_reserved_expected_user_generations_.clear();
    creation_removed_expected_user_generations_.clear();
    return active_creation_generation_;
}

bool Game::is_creation_active(const std::shared_ptr<Peer>& owner,
                              uint64_t generation) const {
    std::lock_guard state_lock(creation_state_mutex_);
    const auto current_owner = creation_owner_.lock();
    return is_creating.load(std::memory_order_acquire) &&
           owner && generation != 0 &&
           generation == active_creation_generation_ &&
           current_owner && current_owner.get() == owner.get();
}

bool Game::commit_creation(const std::shared_ptr<Peer>& owner,
                           uint64_t generation) {
    std::lock_guard admission_lock(admission_mutex);
    std::lock_guard state_lock(creation_state_mutex_);
    const auto current_owner = creation_owner_.lock();
    if (!is_creating.load(std::memory_order_acquire) ||
        !owner || generation == 0 ||
        generation != active_creation_generation_ ||
        !current_owner || current_owner.get() != owner.get() ||
        !find_peer(owner))
        return false;

    ++state_revision_;
    is_created.store(true, std::memory_order_release);
    creation_owner_.reset();
    is_creating.store(false, std::memory_order_release);
    return true;
}

uint64_t Game::active_creation_generation() const {
    std::lock_guard state_lock(creation_state_mutex_);
    return active_creation_generation_;
}

bool Game::is_joinable() const {
    std::lock_guard admission_lock(admission_mutex);
    return is_created.load(std::memory_order_acquire) &&
           !is_creating.load(std::memory_order_acquire) && is_visible && is_open;
}

std::optional<int32_t> Game::actor_id_for_peer(const std::shared_ptr<Peer>& peer) const {
    if (!peer)
        return std::nullopt;
    std::lock_guard admission_lock(admission_mutex);
    for (const auto& game_peer : peers)
        if (game_peer.active && game_peer.peer.lock() == peer)
            return game_peer.actor_id;
    return std::nullopt;
}

bool Game::has_peer_actor(int32_t actor_id) const {
    std::lock_guard admission_lock(admission_mutex);
    for (const auto& game_peer : peers)
        if (game_peer.active && game_peer.actor_id == actor_id)
            return true;
    return false;
}

uint64_t Game::state_revision() const {
    std::lock_guard admission_lock(admission_mutex);
    return state_revision_;
}

Game::ConfigSnapshot Game::get_config_snapshot() const {
    std::lock_guard admission_lock(admission_mutex);
    return ConfigSnapshot{.flags = flags,
                          .is_created = is_created.load(std::memory_order_acquire),
                          .is_open = is_open,
                          .is_visible = is_visible,
                          .max_peers = max_peers,
                          .master_actor = master_actor,
                          .player_ttl = player_ttl,
                          .empty_game_ttl = empty_game_ttl,
                          .last_actor_id = last_actor_id,
                          .dummy_peer_count = dummy_peer_count,
                          .peer_count = peers.size()};
}

size_t Game::active_peer_count() const {
    std::lock_guard admission_lock(admission_mutex);
    return peers.size();
}

void Game::set_config_state(uint8_t new_flags, bool new_is_open,
                            bool new_is_visible, uint8_t new_max_peers,
                            int32_t new_master_actor) {
    std::lock_guard admission_lock(admission_mutex);
    flags = new_flags;
    is_open = new_is_open;
    is_visible = new_is_visible;
    max_peers = new_max_peers;
    master_actor = new_master_actor;
    ++state_revision_;
}

bool Game::has_expected_user(std::string_view user_id) const {
    std::lock_guard admission_lock(admission_mutex);
    return expected_users.contains(std::string(user_id));
}

bool Game::abort_creation_transaction(const std::shared_ptr<Peer>& owner,
                                      uint64_t generation) {
    std::lock_guard admission_lock(admission_mutex);
    std::lock_guard state_lock(creation_state_mutex_);
    const auto current_owner = creation_owner_.lock();
    if (!is_creating.load(std::memory_order_acquire) ||
        !owner || generation == 0 ||
        generation != active_creation_generation_ ||
        !current_owner || current_owner.get() != owner.get())
        return false;

    // Retire every transaction node instead of erasing it so pointers handed
    // to plugin call-info structures stay address-stable for the Game's
    // lifetime. add_peer_for_creation() only ever admits the owner, but an
    // FFI plugin could have inserted additional peers meanwhile.
    for (auto it = peers.begin(); it != peers.end();) {
        if (auto current_peer = it->peer.lock(); current_peer) {
            it->active = false;
            retired_peers.splice(retired_peers.end(), peers, it);
            it = peers.begin();
        } else {
            ++it;
        }
    }
    inactive_players = creation_initial_inactive_players_;

    // Roll back only reservations owned by this creation attempt. Existing
    // reservations are left to their own generation-checked expiry callbacks,
    // and restored reservations get fresh generations so stale callbacks can
    // never delete them.
    for (const auto& [user_id, reservation_generation] : creation_reserved_expected_user_generations_)
        remove_expected_user_if_generation(user_id, reservation_generation);
    for (const auto& [user_id, unused_old_generation] : creation_removed_expected_user_generations_) {
        if (!expected_users.contains(user_id))
            (void)reserve_expected_user_with_generation(user_id);
    }
    creation_reserved_expected_user_generations_.clear();
    creation_removed_expected_user_generations_.clear();
    // The reservation counter itself stays monotonic so an old expiry callback
    // can never match a reservation created after this rollback.
    flags = creation_initial_flags_;
    is_open = creation_initial_is_open_;
    is_visible = creation_initial_is_visible_;
    player_ttl = creation_initial_player_ttl_;
    empty_game_ttl = creation_initial_empty_game_ttl_;
    max_peers = creation_initial_max_peers_;
    master_actor = creation_initial_master_actor_;
    custom_props = creation_initial_custom_props_;
    lobby_props = creation_initial_lobby_props_;
    last_actor_id = creation_initial_last_actor_id_;
    dummy_peer_count = creation_initial_dummy_peer_count_;
    event_cache = creation_initial_event_cache_;
#ifdef LUXON_SERVER_ENABLE_PLUGINS
    plugins.resize(creation_initial_plugin_count_);
#endif

    // Publish the restored state only after every ordinary field is consistent.
    ++state_revision_;
    creation_owner_.reset();
    is_created.store(creation_initial_is_created_, std::memory_order_release);
    is_creating.store(false, std::memory_order_release);
    return true;
}

void Game::add_game_info(ser::ParameterList& params) const {
    params[DictKeyCodes::GameAndActor::GameId] = id;
    params[DictKeyCodes::LoadBalancing::Address] = std::string(server_address);
    lobby->add_lobby_info(params);
}

GameInfo Game::get_game_info() const {
    GameInfo fres(lobby->get_lobby_info());
    fres.id = id;
    fres.server_address = server_address;
    return fres;
}

bool Game::matches_game_info(const GameInfo& info) const {
    if (info.id != id)
        return false;

    if (info.lobby.app.id != lobby->app->id)
        return false;

    if (info.lobby.app.version != lobby->app->version)
        return false;

    return true;
}

GamePeer Game::create_peer(std::shared_ptr<Peer> peer) {
    ZoneScoped;

    std::lock_guard admission_lock(admission_mutex);
    if (!peer || !peer->persistent)
        return {};

    GamePeer fres{.peer = peer, .owner_game = weak_from_this()};

    // Find a free actor number. Check every valid actor exactly once so
    // the actor immediately preceding last_actor_id is not skipped after it leaves.
    for (int attempt = 0; attempt < 0xfe; ++attempt) {
        if (last_actor_id >= 0xfe)
            last_actor_id = 0;
        ++last_actor_id;
        if (!find_peer(last_actor_id)) {
            fres.actor_id = last_actor_id;
            break;
        }
    }

    if (!fres.is_valid())
        return {};

    // Add user id
    if (flags & GameFlags::PublishUserId)
        fres.actor_props[ActorProps::UserId] = peer->persistent->user_id;

    return fres;
}

GamePeer Game::create_peer_for_actor(std::shared_ptr<Peer> peer, int32_t actor_id) {
    ZoneScoped;

    std::lock_guard admission_lock(admission_mutex);
    if (!peer || !peer->persistent || actor_id <= 0 || actor_id >= 0xfe)
        return {};
    if (find_peer(actor_id))
        return {};

    GamePeer fres{.peer = peer, .owner_game = weak_from_this(), .actor_id = actor_id};

    // Add user id
    if (flags & GameFlags::PublishUserId)
        fres.actor_props[ActorProps::UserId] = peer->persistent->user_id;

    return fres;
}


std::optional<uint64_t> Game::reserve_expected_user_with_generation(std::string user_id, unsigned ttl_ms) {
    if (user_id.empty())
        return std::nullopt;

    std::lock_guard admission_lock(admission_mutex);
    if (!expected_users.emplace(user_id).second)
        return std::nullopt;

    uint64_t generation = ++next_expected_user_generation;
    if (generation == 0)
        generation = ++next_expected_user_generation;
    expected_user_generations[user_id] = generation;
    if (is_creating.load(std::memory_order_acquire))
        creation_reserved_expected_user_generations_[user_id] = generation;
    get_server_manager().add_scheduled_task(ttl_ms, [game = shared_from_this(), user_id = std::move(user_id), generation]() {
        std::lock_guard admission_lock(game->admission_mutex);
        if (const auto it = game->expected_user_generations.find(user_id);
            it != game->expected_user_generations.end() && it->second == generation) {
            if (game->is_creating.load(std::memory_order_acquire)) {
                const auto owned = game->creation_reserved_expected_user_generations_.find(user_id);
                if (owned == game->creation_reserved_expected_user_generations_.end() || owned->second != generation)
                    game->creation_removed_expected_user_generations_[user_id] = generation;
            }
            game->expected_user_generations.erase(it);
            game->expected_users.erase(user_id);
        }
    });
    return generation;
}

bool Game::reserve_expected_user(std::string user_id, unsigned ttl_ms) {
    return reserve_expected_user_with_generation(std::move(user_id), ttl_ms).has_value();
}

void Game::remove_expected_user(std::string_view user_id) {
    std::lock_guard admission_lock(admission_mutex);
    const std::string key(user_id);
    if (const auto it = expected_user_generations.find(key); it != expected_user_generations.end()) {
        if (is_creating.load(std::memory_order_acquire)) {
            const auto owned = creation_reserved_expected_user_generations_.find(key);
            if (owned == creation_reserved_expected_user_generations_.end() || owned->second != it->second)
                creation_removed_expected_user_generations_[key] = it->second;
        }
        expected_user_generations.erase(it);
    }
    expected_users.erase(key);
}

bool Game::remove_expected_user_if_generation(std::string_view user_id, uint64_t generation) {
    std::lock_guard admission_lock(admission_mutex);
    const std::string key(user_id);
    const auto it = expected_user_generations.find(key);
    if (it == expected_user_generations.end() || it->second != generation)
        return false;
    expected_user_generations.erase(it);
    expected_users.erase(key);
    return true;
}

bool Game::apply_interest_groups(const std::shared_ptr<Peer>& peer,
                                 const std::vector<uint8_t>& add,
                                 const std::vector<uint8_t>& remove) {
    std::lock_guard admission_lock(admission_mutex);
    auto *game_peer = find_peer(peer);
    if (!game_peer)
        return false;

    if (remove.empty()) {
        game_peer->interest_groups.reset();
    } else {
        for (const uint8_t group : remove)
            game_peer->interest_groups.reset(group);
    }
    if (add.empty()) {
        game_peer->interest_groups.set();
    } else {
        for (const uint8_t group : add)
            game_peer->interest_groups.set(group);
    }
    return true;
}

GamePeer *Game::add_peer(GamePeer&& game_peer) {
    ZoneScoped;

    std::lock_guard admission_lock(admission_mutex);
    // A placeholder that has not begun its creation transaction must never
    // gain members; otherwise a failed creation could never be restarted.
    if (is_creating.load(std::memory_order_acquire) &&
        !is_created.load(std::memory_order_acquire))
        return nullptr;
    auto peer = game_peer.peer.lock();
    if (!peer || !peer->persistent)
        return nullptr;

    // Make sure actor_id is unique
    if (find_peer(game_peer.actor_id))
        return nullptr;

    // Check user id uniqueness if enabled
    if (flags & GameFlags::CheckUserOnJoin)
        for (const auto& that_game_peer : peers)
            if (auto that_peer = that_game_peer.peer.lock())
                if (that_peer->persistent && that_peer->persistent->user_id == peer->persistent->user_id)
                    return nullptr;

    // Add peer to list
    auto& fres = peers.emplace_back(std::move(game_peer));
    fres.owner_game = weak_from_this();

    // Remove user from expected users and invalidate its expiry callback.
    remove_expected_user(peer->persistent->user_id);

    return &fres;
}

GamePeer *Game::add_peer_for_creation(GamePeer&& game_peer,
                                      const std::shared_ptr<Peer>& owner,
                                      uint64_t generation) {
    std::lock_guard admission_lock(admission_mutex);
    std::lock_guard state_lock(creation_state_mutex_);
    const auto current_owner = creation_owner_.lock();
    if (!is_creating.load(std::memory_order_acquire) ||
        !owner || generation == 0 || generation != active_creation_generation_ ||
        !current_owner || current_owner.get() != owner.get())
        return nullptr;

    auto peer = game_peer.peer.lock();
    if (!peer || !peer->persistent || peer.get() != owner.get() || find_peer(game_peer.actor_id))
        return nullptr;
    if (flags & GameFlags::CheckUserOnJoin)
        for (const auto& that_game_peer : peers)
            if (auto that_peer = that_game_peer.peer.lock())
                if (that_peer->persistent && that_peer->persistent->user_id == peer->persistent->user_id)
                    return nullptr;

    auto& fres = peers.emplace_back(std::move(game_peer));
    fres.owner_game = weak_from_this();
    remove_expected_user(peer->persistent->user_id);
    return &fres;
}

bool Game::remove_peer(const std::shared_ptr<Peer>& peer) {
    ZoneScoped;

    if (!peer)
        return false;

    int32_t leaving_actor_id = 0;
    bool call_before_close = false;
    {
        std::lock_guard admission_lock(admission_mutex);

        // Clean up peer list until ineffective.
    restart:
        for (auto it = peers.begin(); it != peers.end(); ++it) {
            if (auto this_peer = it->peer.lock()) {
                if (this_peer.get() == peer.get()) {
                    leaving_actor_id = it->actor_id;

                    if (flags & GameFlags::DeleteCacheOnLeave)
                        event_cache.remove_if([leaving_actor_id](const Event& ev) {
                            return ev.sender_actor_id == leaving_actor_id && ev.sender_actor_id != 0;
                        });

                    it->active = false;
                    retired_peers.splice(retired_peers.end(), peers, it);
                    goto restart;
                }
            } else {
                // Retire stale peers instead of invalidating plugin handles.
                it->active = false;
                retired_peers.splice(retired_peers.end(), peers, it);
                goto restart;
            }
        }

        if (!leaving_actor_id)
            return false;

        if (peers.empty()) {
            if (empty_game_ttl > 0)
                lobby->app->server_manager.add_scheduled_task(empty_game_ttl, [game = shared_from_this()]() {});
            call_before_close = true;
        } else if (leaving_actor_id == master_actor) {
            master_actor = peers.front().actor_id;
        }
    }

#ifdef LUXON_SERVER_ENABLE_PLUGINS
    if (call_before_close) {
        BeforeCloseGameCallInfo info{.failed_on_create = !is_created.load(std::memory_order_acquire)};
        execute_plugin_chain(&PluginBase::BeforeCloseGame, info);
    }
#endif

    return true;
}

bool Game::flood_peer_by_actor(int32_t actor_id) {
    std::lock_guard admission_lock(admission_mutex);
    auto *game_peer = find_peer(actor_id);
    return game_peer ? flood_peer(game_peer) : false;
}

bool Game::flood_peer(GamePeer *game_peer) {
    ZoneScoped;
    std::lock_guard admission_lock(admission_mutex);
    if (!game_peer || !game_peer->active)
        return false;

    // Send cached events to the new peer
    if (auto peer = game_peer->peer.lock()) {
        size_t count = 0;

        // CRITICAL FIX: First, send the current property state as a property update event
        // This ensures the new player receives all current room and actor properties BEFORE
        // other cached events, preventing NullReferenceException when accessing properties
        {
            auto current_game_props = get_game_props();
            auto current_actor_props = get_actor_props();

            // Send room properties update (actor_id = 0)
            if (!current_game_props.empty()) {
                Event props_event{.code = EventCodes::PropertiesUpdate,
                                 .sender_actor_id = 0,  // System event
                                 .delivery_mode = enet::EnetDeliveryMode::Reliable,
                                 .receivers = std::unordered_set<int32_t>{game_peer->actor_id}};
                props_event.top_params[DictKeyCodes::GameAndActor::TargetActorNo] = static_cast<int32_t>(0);
                props_event.top_params[DictKeyCodes::Properties::Properties] = std::move(current_game_props);

                const auto expected_props_payload = props_event.get_cached_data(*peer->protocol);
                if (!expected_props_payload)
                    peer->log->warn("Failed to serialize game properties flood event: {}", expected_props_payload.error().message);
                else
                    peer->send(*expected_props_payload, enet::EnetSendOptions{.channel = 0, .mode = enet::EnetDeliveryMode::Reliable});

                ++count;
            }

            // Send actor properties update
            if (!current_actor_props.empty()) {
                Event props_event{.code = EventCodes::PropertiesUpdate,
                                 .sender_actor_id = 0,  // System event
                                 .delivery_mode = enet::EnetDeliveryMode::Reliable,
                                 .receivers = std::unordered_set<int32_t>{game_peer->actor_id}};
                props_event.top_params[DictKeyCodes::GameAndActor::TargetActorNo] = static_cast<int32_t>(0);
                props_event.top_params[DictKeyCodes::Properties::Properties] = std::move(current_actor_props);

                const auto expected_props_payload = props_event.get_cached_data(*peer->protocol);
                if (!expected_props_payload)
                    peer->log->warn("Failed to serialize actor properties flood event: {}", expected_props_payload.error().message);
                else
                    peer->send(*expected_props_payload, enet::EnetSendOptions{.channel = 0, .mode = enet::EnetDeliveryMode::Reliable});

                ++count;
            }
        }

        // Then send other cached events
        for (const auto& event : event_cache) {
            // Actor events don't go back to the sender
            if (event.sender_actor_id != 0 && event.sender_actor_id == game_peer->actor_id)
                continue;

            // Skip PropertiesUpdate events in cache - we already sent current state above
            if (event.code == EventCodes::PropertiesUpdate)
                continue;

            // Cached events are re-sent as if they were fresh to the joining player
            const auto expected_event_payload = event.get_cached_data(*peer->protocol);
            if (!expected_event_payload)
                peer->log->warn("Failed to serialize flooded event: {}", expected_event_payload.error().message);
            else
                peer->send(*expected_event_payload, enet::EnetSendOptions{.channel = event.channel, .mode = event.delivery_mode});

            ++count;
        }

        peer->log->info("Client successfully flooded with {} events (including current property state)", count);
        return true;
    }
    return false;
}

GamePeer *Game::find_peer(int32_t actor_id) {
    ZoneScoped;

    for (auto& game_peer : peers)
        if (game_peer.active && game_peer.actor_id == actor_id)
            return &game_peer;
    return nullptr;
}

GamePeer *Game::find_peer(const std::shared_ptr<Peer>& peer) {
    ZoneScoped;

    for (auto& game_peer : peers)
        if (game_peer.active && game_peer.peer.lock() == peer)
            return &game_peer;
    return nullptr;
}

void Game::broadcast_event(Event& event) {
    ZoneScoped;
    std::lock_guard admission_lock(admission_mutex);

    enet::EnetSendOptions send_options{.channel = event.channel, .mode = event.delivery_mode};

    // Set default recipients
    if (event.receivers.index() == 0)
        event.receivers = ReceiverGroup::Others;

    // Dispatcher
    const auto dispatch = [&](GamePeer& game_peer) {
        if (auto peer = game_peer.peer.lock()) {
            const auto expected_event_payload = event.get_cached_data(*peer->protocol);
            if (!expected_event_payload)
                peer->log->warn("Failed to serialize event: {}", expected_event_payload.error().message);
            else
                peer->send(*expected_event_payload, send_options);
        }
    };

    // Send to all recipients
    if (auto *receiver_group = std::get_if<uint8_t>(&event.receivers)) {
        if (*receiver_group == ReceiverGroup::MasterClient) {
            // Send to master client
            if (auto *game_peer = find_peer(master_actor))
                if (game_peer->interest_groups.test(event.interest_group))
                    dispatch(*game_peer);
        } else {
            // Send to others (or all)
            for (auto& game_peer : peers) {
                if (*receiver_group == ReceiverGroup::Others && game_peer.actor_id == event.sender_actor_id)
                    continue;
                if (!game_peer.interest_groups.test(event.interest_group))
                    continue;
                dispatch(game_peer);
            }
        }
    } else if (auto *actors = std::get_if<std::unordered_set<int32_t>>(&event.receivers)) {
        // Send to given actors
        for (const int32_t actor_id : *actors) {
            auto *game_peer = find_peer(actor_id);
            if (!game_peer)
                continue;
            if (!game_peer->interest_groups.test(event.interest_group))
                continue;
            dispatch(*game_peer);
        }
    }
}

std::pair<int16_t, std::string_view> Game::validate_join(const std::string& user_id,
                                                        size_t new_expected_users_count,
                                                        bool allow_uncreated) const {
    ZoneScoped;
    std::lock_guard admission_lock(admission_mutex);

    // Return error if game hasn't been created yet unless the caller is
    // validating the initial creator before publication.
    if (!is_created.load(std::memory_order_acquire) && !allow_uncreated)
        return {ErrorCodes::Matchmaking::GameIdNotExists, "Game does not exist"};

    // Return error if game is closed
    if (!is_open)
        return {ErrorCodes::Matchmaking::GameClosed, "Game is closed"};

    // Check capacity (peers + expected users). The application-wide cap must
    // still apply when the room itself has no MaxPlayers limit.
    const size_t current_count = peers.size() + dummy_peer_count;
    const size_t reserved_count = expected_users.size();
    const bool joining_user_is_reserved = expected_users.contains(user_id);
    const size_t needed_slots = (joining_user_is_reserved ? 0 : 1) + new_expected_users_count;
    const size_t final_peer_count = current_count + reserved_count + needed_slots;

    if (max_peers > 0 && final_peer_count > max_peers)
        return {ErrorCodes::Matchmaking::GameFull, "Game is full"};

    if (const auto max_game_peers = lobby->app->get_settings().max_peers_per_game)
        if (final_peer_count > max_game_peers)
            return {ErrorCodes::Matchmaking::GameFull, "Game is full"};

    if (final_peer_count > 0xfe)
        return {ErrorCodes::Matchmaking::ActorListFull, "Game is full"};

    // Check user id uniqueness if enabled
    if (flags & GameFlags::CheckUserOnJoin)
        for (const auto& that_game_peer : peers)
            if (auto that_peer = that_game_peer.peer.lock())
                if (that_peer->persistent && that_peer->persistent->user_id == user_id)
                    return {ErrorCodes::Matchmaking::JoinFail::JoinFailedPeerAlreadyJoined, "Game already joined"};

    return {ErrorCodes::Core::Ok, {}};
}

std::pair<int16_t, std::string_view> Game::validate_join(const std::string& user_id,
                                                        const std::vector<std::string>& new_expected_users,
                                                        bool allow_uncreated) const {
    std::lock_guard admission_lock(admission_mutex);
    std::unordered_set<std::string> unique_expected;
    unique_expected.reserve(new_expected_users.size());
    for (const auto& expected_user : new_expected_users)
        if (!expected_user.empty() && expected_user != user_id && !expected_users.contains(expected_user))
            unique_expected.emplace(expected_user);

    return validate_join(user_id, unique_expected.size(), allow_uncreated);
}

void Game::trigger_lobby_update() {
    ZoneScoped;

    const uint64_t revision = state_revision();
    const bool created = is_created.load(std::memory_order_acquire);
    const bool creating = is_creating.load(std::memory_order_acquire);

    // Call handlers for both publication and removal transitions.
    auto shared_this = shared_from_this();
    for (auto& handler : lobby->game_list_update_handlers)
        handler.game_update(shared_this);

#ifdef LUXON_SERVER_ENABLE_MULTIPROCESSING
    // Send IPC event
    ser::EventMessage ipc_event;
    ipc_event.event_code = IPCEventCodes::GameUpdate;
    add_game_info(ipc_event.parameters);
    ipc_event.parameters[DictKeyCodes::RoutingAndEvents::Broadcast] = created;
    // ser::Value has no uint64_t alternative; revisions travel as int64_t
    // across IPC and are re-widened by the receiver.
    ipc_event.parameters[IPCDictKeyCodes::Revision] = static_cast<int64_t>(revision);
    ipc_event.parameters[IPCDictKeyCodes::IsCreating] = creating;
    {
        std::vector<std::string> expected_users_snapshot;
        bool visible = false;
        {
            std::lock_guard admission_lock(admission_mutex);
            expected_users_snapshot.assign(expected_users.begin(), expected_users.end());
            visible = is_visible;
        }
        ipc_event.parameters[IPCDictKeyCodes::ExpectedUsers] = std::move(expected_users_snapshot);
        // Visibility is not part of lobby game props, so it travels as its
        // own key to keep cross-process lobby filtering consistent.
        ipc_event.parameters[IPCDictKeyCodes::IsVisible] = visible;
    }
    ipc_event.parameters[DictKeyCodes::Properties::GameProperties] = std::make_shared<ser::Hashtable>(get_lobby_game_props());
    if (!server_address.empty())
        ipc_event.parameters[DictKeyCodes::LoadBalancing::Address] = std::string(server_address);
    get_server_manager().ipc_broadcast(ipc_event);
#endif
}

#define PROP_MAP                                                                                                                                               \
    PROP_MAP_ENTRY(MaxPlayers, uint8_t, max_peers, true);                                                                                                      \
    PROP_MAP_ENTRY(IsVisible, bool, is_visible, false);                                                                                                        \
    PROP_MAP_ENTRY(IsOpen, bool, is_open, true);                                                                                                               \
    PROP_MAP_ENTRY(PlayerTTL, int32_t, player_ttl, false);                                                                                                     \
    PROP_MAP_ENTRY(EmptyGameTTL, int32_t, empty_game_ttl, false);                                                                                              \
    PROP_MAP_ENTRY(MasterClientId, int32_t, master_actor, false);                                                                                              \
    PROP_MAP_ENTRY(LobbyProperties, std::vector<std::string>, lobby_props, true)

ser::Value Game::get_game_prop(const ser::Value& key) {
    ZoneScoped;
    std::lock_guard admission_lock(admission_mutex);

    if (key.is<uint8_t>()) {
        switch (key.get<uint8_t>()) {
#define PROP_MAP_ENTRY(game_param, type, var, updates_lobby)                                                                                                   \
    case GameProps::game_param:                                                                                                                                \
        return var;
            PROP_MAP
#undef PROP_MAP_ENTRY
        case GameProps::PlayerCount:
            return static_cast<uint8_t>(peers.size() + dummy_peer_count);
        }
    }

    if (auto res = custom_props.find(key); res != custom_props.end())
        return res->second;
    return ser::Value(); // null
}

ser::Hashtable Game::get_lobby_game_props() const {
    ZoneScoped;
    std::lock_guard admission_lock(admission_mutex);

    ser::Hashtable fres;
    fres[GameProps::PlayerCount] = static_cast<uint8_t>(peers.size() + dummy_peer_count);
    fres[GameProps::IsOpen] = is_open;
    fres[GameProps::MaxPlayers] = max_peers;

    for (const auto& key : lobby_props)
        if (custom_props.contains(key))
            fres.emplace(key, custom_props.at(key));

    return fres;
}

ser::Hashtable Game::get_game_props(bool no_custom) {
    ZoneScoped;
    std::lock_guard admission_lock(admission_mutex);

    auto fres = no_custom ? ser::Hashtable{} : custom_props;
#define PROP_MAP_ENTRY(game_param, type, var, updates_lobby) fres[GameProps::game_param] = static_cast<type>(var);
    PROP_MAP
#undef PROP_MAP_ENTRY
    fres[GameProps::PlayerCount] = static_cast<uint8_t>(peers.size() + dummy_peer_count);

    return fres;
}

ser::Hashtable Game::get_actor_props() {
    ZoneScoped;
    std::lock_guard admission_lock(admission_mutex);

    ser::Hashtable fres;
    for (const auto& game_peer : peers)
        fres[game_peer.actor_id] = game_peer.actor_props;
    return fres;
}

ser::Hashtable Game::get_actor_props_for(int32_t actor_id) {
    ZoneScoped;
    std::lock_guard admission_lock(admission_mutex);

    if (const auto *game_peer = find_peer(actor_id))
        return game_peer->actor_props;
    return {};
}

void Game::insert_game_props(ser::Hashtable update) {
    ZoneScoped;
    std::unique_lock admission_lock(admission_mutex);

    const bool delete_null = flags & GameFlags::DeleteNullProps;
    const bool was_visible = is_visible;
    bool update_lobby = false;
    bool changed = false;

    for (const auto& [key, value] : update) {
        if (key.is<uint8_t>()) {
            // Update built in props
            switch (key.get<uint8_t>()) {
#define PROP_MAP_ENTRY(game_param, type, var, updates_lobby)                                                                                                   \
    case GameProps::game_param:                                                                                                                                \
        /* If changing master isn't allowed, just ignore the attempt */                                                                                        \
        if constexpr (GameProps::game_param == GameProps::MasterClientId)                                                                                      \
            if (!lobby->app->get_settings().allow_change_master)                                                                                               \
                break;                                                                                                                                         \
        {                                                                                                                                                      \
            /* store_if only reports a type match, so compare against the  */                                                                                  \
            /* previous value: applying an identical IPC echo must not bump */                                                                                 \
            /* the revision or re-broadcast, or processes ping-pong forever. */                                                                                \
            type old_value = var;                                                                                                                              \
            const bool stored = value.store_if<type>(var);                                                                                                     \
            const bool value_changed = stored && !(old_value == var);                                                                                          \
            update_lobby |= value_changed && updates_lobby;                                                                                                    \
            changed |= value_changed;                                                                                                                          \
        }                                                                                                                                                      \
        break;
                PROP_MAP
#undef PROP_MAP_ENTRY
            }
        } else {
            // Update custom props
            if (!key.is_null()) {
                const auto existing = custom_props.find(key);
                if (existing == custom_props.end() || !(existing->second == value)) {
                    custom_props[key] = value;
                    changed = true;
                }
            } else if (delete_null) {
                if (auto res = custom_props.find(key); res != custom_props.end()) {
                    custom_props.erase(res);
                    changed = true;
                }
            }
        }
    }

    if (changed)
        ++state_revision_;

    // A visibility transition removes the game from lobby lists immediately
    // and is republished in both directions via the full lobby/IPC update.
    const bool visibility_changed = was_visible != is_visible;
    if (visibility_changed && was_visible) {
        for (auto& handler : lobby->game_list_update_handlers)
            handler.game_delete(this);
    }

    // update_lobby triggers IPC serialization; run it after releasing the
    // admission lock so cross-process work never happens under the room lock.
    const bool should_update_lobby = update_lobby || visibility_changed;
    admission_lock.unlock();
    if (should_update_lobby)
        trigger_lobby_update();
}

bool Game::expect_game_props(ser::Hashtable expected) {
    ZoneScoped;
    std::lock_guard admission_lock(admission_mutex);

    bool ok = true;
#define PROP_MAP_ENTRY(game_param, type, var, updates_lobby)                                                                                                   \
    if (expected.contains(GameProps::game_param))                                                                                                              \
        ok &= expected[GameProps::game_param].is_equal<type>(var);
    PROP_MAP
#undef PROP_MAP_ENTRY
    if (!ok)
        return false;

    for (const auto& [key, value] : expected) {
#define PROP_MAP_ENTRY(game_param, type, var, updates_lobby)                                                                                                   \
    if (key == GameProps::game_param)                                                                                                                          \
        continue;
        PROP_MAP
#undef PROP_MAP_ENTRY

        const auto custom_prop = custom_props.find(key);
        if (custom_prop == custom_props.end()) {
            if (!value.is_null() || !(flags & GameFlags::DeleteNullProps))
                return false;
        } else if (custom_prop->second != value) {
            return false;
        }
    }

    return true;
}

bool Game::apply_game_props(const ser::Hashtable& update,
                            const std::optional<ser::Hashtable>& expected) {
    std::unique_lock admission_lock(admission_mutex);
    if (expected && !expect_game_props(*expected))
        return false;
    insert_game_props(update);
    return true;
}

bool Game::apply_actor_props(int32_t actor_id, const ser::Hashtable& update,
                             const std::optional<ser::Hashtable>& expected) {
    std::lock_guard admission_lock(admission_mutex);
    if (expected && !expect_actor_props(actor_id, *expected))
        return false;
    return insert_actor_props(actor_id, update);
}

bool Game::insert_actor_props(int32_t actor_id, const ser::Hashtable& update) {
    ZoneScoped;
    std::lock_guard admission_lock(admission_mutex);

    auto *game_peer = find_peer(actor_id);
    if (!game_peer)
        return false;

    auto& actor_props = game_peer->actor_props;

    const bool delete_null = flags & GameFlags::DeleteNullProps;
    for (const auto& [key, value] : update) {
        if (!key.is_null())
            actor_props[key] = value;
        else if (delete_null)
            if (auto res = actor_props.find(key); res != actor_props.end())
                actor_props.erase(res);
    }

    return true;
}

bool Game::expect_actor_props(int32_t actor_id, const ser::Hashtable& expected) {
    ZoneScoped;
    std::lock_guard admission_lock(admission_mutex);

    const auto *game_peer = find_peer(actor_id);
    if (!game_peer)
        return false;

    const auto& actor_props = game_peer->actor_props;
    for (const auto& [key, value] : expected)
        if ((!actor_props.contains(key) && (!value.is_null() || !(flags & GameFlags::DeleteNullProps))) || actor_props.at(key) != value)
            return false;

    return true;
}

Event Game::create_property_update_event(int32_t actor_id, ser::Hashtable props, int32_t target_actor_id) {
    std::lock_guard admission_lock(admission_mutex);
    Event event{.code = EventCodes::PropertiesUpdate,
                .sender_actor_id = actor_id,
                .receivers = (flags & GameFlags::BroadcastPropsChangeToAll) ? ReceiverGroup::All : ReceiverGroup::Others};
    event.top_params[DictKeyCodes::GameAndActor::TargetActorNo] = target_actor_id;
    event.top_params[DictKeyCodes::Properties::Properties] = std::move(props);
    return event;
}

bool Game::matches_filter(const ser::Value& event_data, const ser::Hashtable& filter) {
    ZoneScoped;

    // If filter is empty, it's a match
    if (filter.empty())
        return true;

    // If event has no data but filter is not empty, no match
    if (!event_data.is<ser::HashtablePtr>())
        return false;

    const auto& data_ptr = event_data.get<ser::HashtablePtr>();
    if (!data_ptr)
        return false;
    const auto& data = *data_ptr;

    // Subset check
    for (const auto& [key, val] : filter) {
        if (!data.contains(key))
            return false;
        if (data.at(key) != val)
            return false;
    }

    return true;
}

GameInfo Game::decode_game_info(const ser::ParameterList& params) {
    GameInfo fres(Lobby::decode_lobby_info(params));
    for (const auto& [key, val] : params) {
        if (key == DictKeyCodes::GameAndActor::GameId)
            fres.id = val.get<std::string>();
        if (key == DictKeyCodes::LoadBalancing::Address)
            fres.server_address = val.get<std::string>();
    }
    return fres;
}
} // namespace server
