// Copyright (c) 2026, the Luxon Server contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "global.hpp"
#include "apps.hpp"
#include "lobby.hpp"
#include "peer.hpp"
#include "coro_support.hpp"
#ifdef LUXON_SERVER_ENABLE_PLUGINS
#include "game_plugin_base.hpp"
#endif

#include <memory>
#include <atomic>
#include <mutex>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <list>
#include <variant>
#include <optional>
#include <bitset>
#include <array>
#include <utility>
#include <luxon/ser_types.hpp>
#include <luxon/ser_protocol_id.hpp>
#include <luxon/enet_peer.hpp>

#ifdef LUXON_SERVER_ENABLE_PLUGINS
#define GAME_PLUGINS_INVOKE(...)                                                                                                                               \
    {                                                                                                                                                          \
        using namespace game_plugins;                                                                                                                          \
        __VA_ARGS__                                                                                                                                            \
    }
#else
#define GAME_PLUGINS_INVOKE(...)
#endif

namespace luxon::ser {
class IProtocol;
}

namespace server {
class App;
struct Lobby;
struct Peer;
struct Game;

struct GameInfo {
    LobbyInfo lobby;

    std::string_view id, server_address;

    void encode_game_info(ser::ParameterList& params) const;
    bool has_game_info() const { return lobby.has_lobby_info() && !id.empty(); }
};

struct InterestGroups {
    std::bitset<256> bitset_{1};

    bool test(uint8_t v) const {
        if (v == 0)
            return true;

        return bitset_.test(v);
    }

    void set(uint8_t v, bool enable = true) { bitset_.set(v, enable); }
    void set() { bitset_.set(); }

    void reset(uint8_t v) { bitset_.reset(v); }
    void reset() { bitset_.reset(); }

    std::string to_string() const {
        auto bs = bitset_;
        bs.set(0);
        return bs.to_string();
    }
};

struct GamePeer {
    std::weak_ptr<Peer> peer;
    // Owning room used by FFI handles to detect retired/stale nodes.
    std::weak_ptr<Game> owner_game;
    int32_t actor_id{};
    ser::Hashtable actor_props;
    InterestGroups interest_groups;
    bool active = true;

    bool is_valid() const { return active && actor_id > 0; }
    bool disconnect();
};

struct Event {
    uint8_t code;
    int32_t sender_actor_id;
    enet::EnetDeliveryMode delivery_mode = enet::EnetDeliveryMode::Reliable;
    uint8_t channel{};
    std::variant<std::monostate, uint8_t, std::unordered_set<int32_t>> receivers{};
    uint8_t interest_group{};
    ser::Value data;
    ser::Dictionary top_params;

    mutable std::array<ser::ByteArray, static_cast<size_t>(ser::ProtocolImplID::__length)> cached_data;
    // Event packets can be serialized concurrently for several recipients.
    // Event must remain an aggregate: the designated-initializer call sites
    // and the implicit default constructor both depend on it. The implicitly
    // generated copy/move share cache_mutex between copies, which is exactly
    // what the cache protocol requires.
    mutable std::shared_ptr<std::mutex> cache_mutex = std::make_shared<std::mutex>();

    std::expected<ser::ByteArray, ser::Error> get_cached_data(ser::IProtocol& protocol) const;

    ser::Hashtable& make_params_hashtable() { return *(data = std::make_shared<ser::Hashtable>()).get<ser::HashtablePtr>(); }
    ser::Hashtable *get_params_hashtable() {
        if (data.is<ser::HashtablePtr>())
            return data.get<ser::HashtablePtr>().get();
        return nullptr;
    }
};

struct Game : std::enable_shared_from_this<Game> {
    const std::shared_ptr<Lobby> lobby;
    const std::string id;
    const std::string_view server_address;

    ~Game();

#ifdef LUXON_SERVER_ENABLE_PLUGINS
    std::vector<std::unique_ptr<game_plugins::PluginBase>> plugins;
#endif

    uint8_t flags = 3; // CheckUserOnJoin | DeleteCacheOnLeave
    std::atomic_bool is_created = false;
    bool is_open = true;
    bool is_visible = true;
    int32_t player_ttl = 0;
    int32_t empty_game_ttl = 0;
    uint8_t max_peers = 0;
    int32_t master_actor = 1;
    int32_t last_actor_id = 0;
    std::unordered_set<std::string> expected_users;
    std::unordered_map<std::string, uint64_t> expected_user_generations;
    uint64_t next_expected_user_generation = 0;
    ser::Hashtable custom_props;
    std::vector<std::string> lobby_props;
    std::list<Event> event_cache;
    
    // Player state tracking for reconnection support.
    struct InactivePlayerInfo {
        int32_t actor_id;
        std::string user_id;
        ser::Hashtable actor_props;
        uint64_t inactive_since = 0;
        uint32_t generation = 0;
    };
    std::unordered_map<std::string, InactivePlayerInfo> inactive_players;

    // Snapshots used to roll back a failed first-join transaction.
    bool creation_initial_is_created_{};
    int32_t creation_initial_last_actor_id_{};
    uint8_t creation_initial_dummy_peer_count_{};
    std::list<Event> creation_initial_event_cache_;
    std::unordered_set<std::string> creation_initial_expected_users_;
    std::unordered_map<std::string, uint64_t> creation_initial_expected_user_generations_;
    std::unordered_map<std::string, InactivePlayerInfo> creation_initial_inactive_players_;
    std::unordered_map<std::string, uint64_t> creation_reserved_expected_user_generations_;
    std::unordered_map<std::string, uint64_t> creation_removed_expected_user_generations_;

    // True while the first join request is initializing the room. Pending requests
    // remain on their own handlers so they can resume with the original context.
    std::atomic_bool is_creating = false;
    mutable std::mutex creation_state_mutex_;
    std::weak_ptr<Peer> creation_owner_;
    uint64_t next_creation_generation_{};
    uint64_t active_creation_generation_{};
    uint64_t state_revision_{};
    uint8_t creation_initial_flags_{};
    bool creation_initial_is_open_{};
    bool creation_initial_is_visible_{};
    int32_t creation_initial_player_ttl_{};
    int32_t creation_initial_empty_game_ttl_{};
    uint8_t creation_initial_max_peers_{};
    int32_t creation_initial_master_actor_{};
    ser::Hashtable creation_initial_custom_props_;
    std::vector<std::string> creation_initial_lobby_props_;
#ifdef LUXON_SERVER_ENABLE_PLUGINS
    size_t creation_initial_plugin_count_{};
#endif

    // Serializes the admission/actor-allocation portion of joins. Plugin hooks may
    // suspend outside this lock, but the final capacity check and insertion are
    // performed while holding it.
    mutable std::recursive_mutex admission_mutex;

    uint64_t try_begin_creation(const std::shared_ptr<Peer>& owner);
    bool is_creation_active(const std::shared_ptr<Peer>& owner,
                            uint64_t generation) const;
    bool commit_creation(const std::shared_ptr<Peer>& owner,
                         uint64_t generation);
    bool abort_creation_transaction(const std::shared_ptr<Peer>& owner,
                                    uint64_t generation);

    uint64_t active_creation_generation() const;
    bool is_joinable() const;
    std::optional<int32_t> actor_id_for_peer(const std::shared_ptr<Peer>& peer) const;
    bool has_peer_actor(int32_t actor_id) const;
    /// Monotonic counter bumped by every externally visible state transition.
    /// Callers must hold admission_mutex while writing state_revision_ directly.
    uint64_t state_revision() const;

    struct ConfigSnapshot {
        uint8_t flags{};
        bool is_created{};
        bool is_open{};
        bool is_visible{};
        uint8_t max_peers{};
        int32_t master_actor{};
        int32_t player_ttl{};
        int32_t empty_game_ttl{};
        int32_t last_actor_id{};
        uint8_t dummy_peer_count{};
        size_t peer_count{};
    };

    ConfigSnapshot get_config_snapshot() const;
    size_t active_peer_count() const;
    void set_config_state(uint8_t new_flags, bool new_is_open,
                          bool new_is_visible, uint8_t new_max_peers,
                          int32_t new_master_actor);
    bool has_expected_user(std::string_view user_id) const;


    Game(std::shared_ptr<Lobby> lobby, std::string id, std::string_view server_address);

    std::list<GamePeer> peers;
    // Removed nodes are retired instead of erased so plugin call-info pointers
    // remain address-stable until the Game itself is destroyed.
    std::list<GamePeer> retired_peers;
    uint8_t dummy_peer_count = 0;

    ///
    /// \brief Returns the server manager that is managing this game
    /// \return Reference to server manager
    ///
    ServerManager& get_server_manager() const { return lobby->app->server_manager; }

    ///
    /// \brief Adds appid, appver, lobbyid, lobbytype, gameid to parameter list
    /// \param Parameter list to add info to
    ///
    void add_game_info(ser::ParameterList& params) const;

    ///
    /// \brief Gets GameInfo for current game
    /// \return Struct containing game identification information
    ///
    GameInfo get_game_info() const;

    ///
    /// \brief Checks if game info matches this game
    /// \param Info to check against
    /// \return True if info is for this game
    ///
    bool matches_game_info(const GameInfo& info) const;

    ///
    /// \brief Creates a GamePeer that can later be added to the game
    /// \param peer The peer that's going to be behind the GamePeer
    /// \return Complete GamePeer
    /// \note Returned GamePeer must not be added to any other game
    /// \note Returned GamePeer has actor_id set, but actor_id won't be fully reserved. It might be taken by another GamePeer after a long time.
    ///
    GamePeer create_peer(std::shared_ptr<Peer> peer);
    /// Creates a GamePeer with a specific actor id (used for rejoin flows).
    GamePeer create_peer_for_actor(std::shared_ptr<Peer> peer, int32_t actor_id);
    ///
    /// \brief Adds given GamePeer to game
    /// \param game_peer GamePeer to add to game, must've been previously been created by the same Game using create_peer()
    /// \return Pointer to added GamePeer if successful, otherwise nullptr
    /// \note Fails if actor_id is already taken or CheckUserOnJoin flag is set and user id is already taken
    ///
    GamePeer *add_peer(GamePeer&& game_peer);
    GamePeer *add_peer_for_creation(GamePeer&& game_peer,
                                    const std::shared_ptr<Peer>& owner,
                                    uint64_t generation);
    ///
    /// \brief Adds a temporary expected user reservation with generation-safe expiry.
    /// \return True when a new reservation was added.
    ///
    std::optional<uint64_t> reserve_expected_user_with_generation(std::string user_id,
                                                                  unsigned ttl_ms = 30000);
    bool reserve_expected_user(std::string user_id, unsigned ttl_ms = 30000);
    ///
    /// \brief Removes an expected user reservation and invalidates its expiry callback.
    ///
    void remove_expected_user(std::string_view user_id);
    bool remove_expected_user_if_generation(std::string_view user_id,
                                            uint64_t generation);
    /// Applies additive/removal interest-group changes to the caller's actor.
    bool apply_interest_groups(const std::shared_ptr<Peer>& peer,
                               const std::vector<uint8_t>& add,
                               const std::vector<uint8_t>& remove);
    ///
    /// \brief Removes peer's GamePeer from game
    /// \param peer Peer whos GamePeer to remove
    /// \return True if a GamePeer was removed, otherwise false
    ///
    bool remove_peer(const std::shared_ptr<Peer>& peer);
    ///
    /// \brief Floods given peer with cached events
    /// \param game_peer GamePeer to flood
    /// \return True if flooding was successful, otherwise false
    ///
    bool flood_peer(GamePeer *game_peer);
    bool flood_peer_by_actor(int32_t actor_id);
    ///
    /// \brief Finds game peer with given actor_id
    /// \param actor_id Actor_id to look for
    /// \return Pointer to GamePeer with given actor_id if successful, otherwise nullptr
    ///
    // The returned pointer is valid only while admission_mutex is held by the caller.
    GamePeer *find_peer(int32_t actor_id);
    ///
    /// \brief Finds game peer with given peer
    /// \param peer Peer to look for
    /// \return Pointer to GamePeer with given actor_id if successful, otherwise nullptr
    ///
    // The returned pointer is valid only while admission_mutex is held by the caller.
    GamePeer *find_peer(const std::shared_ptr<Peer>& peer);
    ///
    /// \brief Broadcasts an event to the game
    /// \param event Event to broadcast
    ///
    void broadcast_event(Event& event);
    ///
    /// \brief Checks if user + given amount of expected users can join
    /// \param user_id User ID of primary user trying to join
    /// \param new_expected_users_count Amount of users to calculate in as well
    /// \return ErrorCode value and error string
    ///
    std::pair<int16_t, std::string_view> validate_join(const std::string& user_id,
                                                       size_t new_expected_users_count = 0,
                                                       bool allow_uncreated = false) const;
    std::pair<int16_t, std::string_view> validate_join(const std::string& user_id,
                                                       const std::vector<std::string>& new_expected_users,
                                                       bool allow_uncreated = false) const;

    ///
    /// \brief Updates the game in the lobby's game list
    ///
    void trigger_lobby_update();

    ///
    /// \brief Gets well-known or custom game property
    /// \param key Property to get
    /// \return Value of property, null-value if not found
    ///
    ser::Value get_game_prop(const ser::Value& key);
    ///
    /// \brief Gets all well-known game properties that are to be shown in lobby
    /// \return Hashtable with well-known keys/value property pairs
    ///
    ser::Hashtable get_lobby_game_props() const;
    ///
    /// \brief Gets all game properties
    /// \param no_custom Excludes custom properties
    /// \return Hashtable with keys/value property pairs
    ///
    ser::Hashtable get_game_props(bool no_custom = false);
    ///
    /// \brief Gets all properties from all actors
    /// \return Hashtable with actor->properties pairs containing key/value property pairs
    ///
    ser::Hashtable get_actor_props();
    /// Consistent snapshot of an actor's properties for external consumers.
    ser::Hashtable get_actor_props_for(int32_t actor_id);
    ///
    /// \brief Merges a hashtable with given properties into game properties
    /// \param update Hashtable with keys/value property pairs
    ///
    void insert_game_props(ser::Hashtable update);
    ///
    /// \brief Checks if the expectation of given properties is met
    /// \param expected Hashtable with keys/value property pairs
    /// \return True if expectation is met, otherwise false
    ///
    bool expect_game_props(ser::Hashtable expected);
    ///
    /// \brief Merges a hashtable with given properties into given actors properties
    /// \param actor_id actor_id of actor whos properties to access
    /// \param update Hashtable with keys/value property pairs
    /// \return True if given actor was found and its properties updated
    ///
    bool insert_actor_props(int32_t actor_id, const ser::Hashtable& update);
    /// Atomically validates expected properties and applies the update.
    bool apply_game_props(const ser::Hashtable& update,
                          const std::optional<ser::Hashtable>& expected);
    bool apply_actor_props(int32_t actor_id, const ser::Hashtable& update,
                           const std::optional<ser::Hashtable>& expected);
    ///
    /// \brief Checks if the expectation of given properties is met
    /// \param actor_id actor_id of actor whos properties to access
    /// \param expected Hashtable with keys/value property pairs
    /// \return True if expectation is met, otherwise false
    ///
    bool expect_actor_props(int32_t actor_id, const ser::Hashtable& expected);
    /// \brief Generates property update event for network distribution
    ///
    /// \param actor_id ID of the actor initiating the update (use 0 if update is triggered by server logic)
    /// \param props A collection of key/value pairs representing the properties to be updated on clients
    /// \param target_actor_id ID of specific actor whose properties are being modified, pass 0 to target global game properties instead of a specific actor
    /// \return A constructed Event object ready to be broadcasted to clients
    Event create_property_update_event(int32_t actor_id, ser::Hashtable props, int32_t target_actor_id = 0);

#ifdef LUXON_SERVER_ENABLE_PLUGINS
    template <typename InfoStruct>
    Awaitable<game_plugins::Result>
    execute_plugin_chain(Awaitable<game_plugins::Result> (game_plugins::PluginBase::*method)(luxon::ser::OperationRequestMessage&, InfoStruct&),
                         luxon::ser::OperationRequestMessage& req, InfoStruct& info) {
        for (const auto& plugin : plugins) {
            auto invocation = ((*plugin).*method)(req, info);
            game_plugins::Result result = lco_await std::move(invocation);
            if (result != game_plugins::Result::Continue)
                lco_return result;
        }

        lco_return game_plugins::Result::Continue;
    }

    template <typename InfoStruct>
    game_plugins::Result execute_plugin_chain(game_plugins::Result (game_plugins::PluginBase::*method)(InfoStruct&), InfoStruct& info) {
        for (const auto& plugin : plugins) {
            game_plugins::Result result = ((*plugin).*method)(info);
            if (result != game_plugins::Result::Continue)
                return result;
        }

        return game_plugins::Result::Continue;
    }
#endif

    // Helper to check if event data matches a filter hashtable
    static bool matches_filter(const ser::Value& event_data, const ser::Hashtable& filter);

    // Helper to decode game info
    static GameInfo decode_game_info(const ser::ParameterList& params);
};
} // namespace server
