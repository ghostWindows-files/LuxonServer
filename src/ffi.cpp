// Copyright (c) 2026, the Luxon Server contributors
// SPDX-License-Identifier: BSD-3-Clause

#define FFI_BUILD_IMPL

#include "luxon_server_ffi.h"
#include "luxon/server/server_manager.hpp"
#include "luxon/server/handler_gameserver.hpp"
#include "luxon/server/game.hpp"
#include "luxon/server/peer_persistence.hpp"
#include "luxon/server/lobby.hpp"
#include "luxon/server/peer.hpp"
#include "luxon/server/logger.hpp"
#include "luxon/server/coro_support.hpp"
#include "luxon/ser_ipc_binary.hpp"
#include "luxon/ser_types.hpp"
#ifdef LUXON_SERVER_ENABLE_PLUGINS
#include "luxon/server/game_plugin_registry.hpp"
#include "luxon/server/game_plugin_base.hpp"
#include <atomic>
#endif
#include "luxon/server/auth_plugin_registry.hpp"
#ifdef LUXON_SERVER_ENABLE_HOOKPOINTS
#include "luxon/server/handler_masterserver.hpp"
#include "luxon/server/apps.hpp"
#endif
#ifdef LUXON_SERVER_ENABLE_MULTIPROCESSING
#include "luxon/server/ipc.hpp"
#endif

#include <algorithm>
#include <string>
#include <vector>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <span>
#include <exception>
#include <typeinfo>
#include <cstring>
#include <cstddef>
#include <luxon/common_codes.hpp>
#ifdef LUXON_SERVER_ENABLE_COROUTINES
#include <FfiAwaiter.hpp>
#endif

/* ============================================================================
 * INTERNAL FFI STATE & ERROR MANAGEMENT
 * ============================================================================ */

static std::unordered_map<std::string, std::shared_ptr<server::logger>> g_logger_registry;
static std::mutex g_registry_mutex;

#if !defined(FFI_WASM) && !defined(__wasm__)
static LuxonServerImports g_imports{};
#endif

// Thread-local storage for passing exception info across the FFI safely
static thread_local bool t_has_error = false;
static thread_local std::string t_last_error_type;
static thread_local std::string t_last_error_msg;

static void set_ffi_error(const char *type_name, const char *msg) {
    t_has_error = true;
    t_last_error_type = type_name ? type_name : "unknown_type";
    t_last_error_msg = msg ? msg : "Unknown exception occurred";
}

static void clear_ffi_error() {
    t_has_error = false;
    t_last_error_type.clear();
    t_last_error_msg.clear();
}

// Exception boundary execution wrapper for functions returning a value
template <typename Ret, typename Func> inline Ret ffi_safe_call(Ret default_val, Func&& func) {
    clear_ffi_error();
    try {
        return func();
    } catch (const std::exception& e) {
        set_ffi_error(typeid(e).name(), e.what());
        return default_val;
    } catch (...) {
        set_ffi_error("unknown", "Non-standard exception thrown");
        return default_val;
    }
}

// Exception boundary execution wrapper for void functions
template <typename Func> inline void ffi_safe_exec(Func&& func) {
    clear_ffi_error();
    try {
        func();
    } catch (const std::exception& e) {
        set_ffi_error(typeid(e).name(), e.what());
    } catch (...) {
        set_ffi_error("unknown", "Non-standard exception thrown");
    }
}

// Safely decodes abstract handle representation to target native pointer structure
template <typename T, typename HandleT> inline T *unwrap(HandleT handle) {
#if defined(FFI_WASM) || defined(__wasm__)
    return reinterpret_cast<T *>(static_cast<uintptr_t>(handle));
#else
    return reinterpret_cast<T *>(handle);
#endif
}

// Encodes target native pointer structure to the targeted handle signature
template <typename HandleT, typename T> inline HandleT wrap(T *ptr) {
#if defined(FFI_WASM) || defined(__wasm__)
    return static_cast<uint32_t>(reinterpret_cast<uintptr_t>(ptr));
#else
    return reinterpret_cast<HandleT>(ptr);
#endif
}

template <typename HandleT> inline HandleT wrap(std::nullptr_t) {
#if defined(FFI_WASM) || defined(__wasm__)
    return 0u;
#else
    return static_cast<HandleT>(nullptr);
#endif
}

#ifdef LUXON_SERVER_ENABLE_COROUTINES
struct FfiCoroContext;
using FfiCoroStack = std::stack<FfiCoroContext *>;
static thread_local FfiCoroStack t_coro_stack;

class FfiCoroContext {
    FfiCoroStack& stack_;

public:
    FfiCoroContext(FfiCoroStack& stack) : stack_(stack) { stack_.push(this); }
    ~FfiCoroContext() { pop(); }

    FfiCoroContext(const FfiCoroContext&) = delete;
    FfiCoroContext& operator=(const FfiCoroContext&) = delete;

    FfiCoroContext(FfiCoroContext&&) = delete;
    FfiCoroContext& operator=(FfiCoroContext&&) = delete;

    void pop() {
        if (stack_.empty())
            return;
        if (stack_.top() == this)
            stack_.pop();
    }

    void *awaiter_handle = nullptr;
};
#endif

#ifdef LUXON_SERVER_ENABLE_PLUGINS
static std::atomic<uint32_t> g_next_plugin_id{1};

class FfiGamePlugin : public server::game_plugins::PluginBase {
public:
    uint32_t plugin_id_;

    FfiGamePlugin(server::Game *game, std::string_view plugin_name, uint32_t plugin_id)
        : server::game_plugins::PluginBase(game, plugin_name), plugin_id_(plugin_id) {}

    static inline void update_req_from_updated_msg(luxon::ser::OperationRequestMessage& req, const luxon::ser::Message& msg) {
        if (msg.is_owning())
            if (auto *new_req = msg.get_if<luxon::ser::OperationRequestMessage>())
                req = *new_req;
    }

    server::Awaitable<> OnAttach() override {
        // TODO: Make coroutine compatible
#if defined(FFI_WASM) || defined(__wasm__)
        gamePluginOnAttach(plugin_id_, wrap<GameHandle>(game_));
#else
        if (g_imports.gamePluginOnAttach)
            g_imports.gamePluginOnAttach(plugin_id_, wrap<GameHandle>(game_));
#endif
        lco_return;
    }

    server::Awaitable<server::game_plugins::Result> OnCreateGame(luxon::ser::OperationRequestMessage& req,
                                                                 server::game_plugins::OnCreateGameCallInfo& info) override {
#ifdef LUXON_SERVER_ENABLE_COROUTINES
        FfiCoroContext ctx(t_coro_stack);
#endif

        uint8_t res = 0;
        luxon::ser::Message msg_ref(&req);
#if defined(FFI_WASM) || defined(__wasm__)
        res = gamePluginOnCreateGame(plugin_id_, wrap<GameHandle>(game_), wrap<SerMessageHandle>(&msg_ref), wrap<PeerHandle>(info.creator.get()), info.is_join,
                                     info.create_if_not_exist);
#else
        if (g_imports.gamePluginOnCreateGame)
            res = g_imports.gamePluginOnCreateGame(plugin_id_, wrap<GameHandle>(game_), wrap<SerMessageHandle>(&msg_ref), wrap<PeerHandle>(info.creator.get()),
                                                   info.is_join, info.create_if_not_exist);
#endif

#ifdef LUXON_SERVER_ENABLE_COROUTINES
        ctx.pop();

        if (res == LUXON_PLUGIN_RESULT_ASYNC)
            res = co_await basiccoro::ffi_await<uint8_t>([&ctx](void *opaque_handle) { ctx.awaiter_handle = opaque_handle; });
#endif

        update_req_from_updated_msg(req, msg_ref);
        lco_return static_cast<server::game_plugins::Result>(res);
    }

    server::Awaitable<server::game_plugins::Result> BeforeJoin(luxon::ser::OperationRequestMessage& req,
                                                               server::game_plugins::BeforeJoinGameCallInfo& info) override {
#ifdef LUXON_SERVER_ENABLE_COROUTINES
        FfiCoroContext ctx(t_coro_stack);
#endif

        uint8_t res = 0;
        luxon::ser::Message msg_ref(&req);
#if defined(FFI_WASM) || defined(__wasm__)
        res = gamePluginBeforeJoin(plugin_id_, wrap<GameHandle>(game_), wrap<SerMessageHandle>(&msg_ref), wrap<PeerHandle>(info.joiner.get()));
#else
        if (g_imports.gamePluginBeforeJoin)
            res = g_imports.gamePluginBeforeJoin(plugin_id_, wrap<GameHandle>(game_), wrap<SerMessageHandle>(&msg_ref), wrap<PeerHandle>(info.joiner.get()));
#endif

#ifdef LUXON_SERVER_ENABLE_COROUTINES
        ctx.pop();

        if (res == LUXON_PLUGIN_RESULT_ASYNC)
            res = co_await basiccoro::ffi_await<uint8_t>([&ctx](void *opaque_handle) { ctx.awaiter_handle = opaque_handle; });
#endif

        update_req_from_updated_msg(req, msg_ref);
        lco_return static_cast<server::game_plugins::Result>(res);
    }

    server::Awaitable<server::game_plugins::Result> OnJoinGame(luxon::ser::OperationRequestMessage& req,
                                                               server::game_plugins::OnJoinGameCallInfo& info) override {
#ifdef LUXON_SERVER_ENABLE_COROUTINES
        FfiCoroContext ctx(t_coro_stack);
#endif

        uint8_t res = 0;
        luxon::ser::Message msg_ref(&req);
#if defined(FFI_WASM) || defined(__wasm__)
        res = gamePluginOnJoinGame(plugin_id_, wrap<GameHandle>(game_), wrap<SerMessageHandle>(&msg_ref), wrap<GamePeerHandle>(info.joiner));
#else
        if (g_imports.gamePluginOnJoinGame)
            res = g_imports.gamePluginOnJoinGame(plugin_id_, wrap<GameHandle>(game_), wrap<SerMessageHandle>(&msg_ref), wrap<GamePeerHandle>(info.joiner));
#endif

#ifdef LUXON_SERVER_ENABLE_COROUTINES
        ctx.pop();

        if (res == LUXON_PLUGIN_RESULT_ASYNC)
            res = co_await basiccoro::ffi_await<uint8_t>([&ctx](void *opaque_handle) { ctx.awaiter_handle = opaque_handle; });
#endif

        update_req_from_updated_msg(req, msg_ref);
        lco_return static_cast<server::game_plugins::Result>(res);
    }

    server::Awaitable<server::game_plugins::Result> OnLeave(luxon::ser::OperationRequestMessage& req,
                                                            server::game_plugins::OnLeaveGameCallInfo& info) override {
#ifdef LUXON_SERVER_ENABLE_COROUTINES
        FfiCoroContext ctx(t_coro_stack);
#endif

        uint8_t res = 0;
        luxon::ser::Message msg_ref(&req);
#if defined(FFI_WASM) || defined(__wasm__)
        res = gamePluginOnLeave(plugin_id_, wrap<GameHandle>(game_), wrap<SerMessageHandle>(&msg_ref), wrap<GamePeerHandle>(info.leaver));
#else
        if (g_imports.gamePluginOnLeave)
            res = g_imports.gamePluginOnLeave(plugin_id_, wrap<GameHandle>(game_), wrap<SerMessageHandle>(&msg_ref), wrap<GamePeerHandle>(info.leaver));
#endif

#ifdef LUXON_SERVER_ENABLE_COROUTINES
        ctx.pop();

        if (res == LUXON_PLUGIN_RESULT_ASYNC)
            res = co_await basiccoro::ffi_await<uint8_t>([&ctx](void *opaque_handle) { ctx.awaiter_handle = opaque_handle; });
#endif

        update_req_from_updated_msg(req, msg_ref);
        lco_return static_cast<server::game_plugins::Result>(res);
    }

    server::Awaitable<server::game_plugins::Result> OnRaiseEvent(luxon::ser::OperationRequestMessage& req,
                                                                 server::game_plugins::OnRaiseEventCallInfo& info) override {
#ifdef LUXON_SERVER_ENABLE_COROUTINES
        FfiCoroContext ctx(t_coro_stack);
#endif

        uint8_t res = 0;
        luxon::ser::Message msg_ref(&req);
#if defined(FFI_WASM) || defined(__wasm__)
        res = gamePluginOnRaiseEvent(plugin_id_, wrap<GameHandle>(game_), wrap<SerMessageHandle>(&msg_ref), wrap<GamePeerHandle>(info.raiser),
                                     wrap<EventHandle>(&info.event), info.cache_op);
#else
        if (g_imports.gamePluginOnRaiseEvent)
            res = g_imports.gamePluginOnRaiseEvent(plugin_id_, wrap<GameHandle>(game_), wrap<SerMessageHandle>(&msg_ref), wrap<GamePeerHandle>(info.raiser),
                                                   wrap<EventHandle>(&info.event), info.cache_op);
#endif

#ifdef LUXON_SERVER_ENABLE_COROUTINES
        ctx.pop();

        if (res == LUXON_PLUGIN_RESULT_ASYNC)
            res = co_await basiccoro::ffi_await<uint8_t>([&ctx](void *opaque_handle) { ctx.awaiter_handle = opaque_handle; });
#endif

        update_req_from_updated_msg(req, msg_ref);
        lco_return static_cast<server::game_plugins::Result>(res);
    }

    server::Awaitable<server::game_plugins::Result> BeforeSetProperties(luxon::ser::OperationRequestMessage& req,
                                                                        server::game_plugins::BeforeSetPropertiesCallInfo& info) override {
#ifdef LUXON_SERVER_ENABLE_COROUTINES
        FfiCoroContext ctx(t_coro_stack);
#endif

        uint8_t res = 0;
        luxon::ser::Value up_val(info.update);
        luxon::ser::Value exp_val(info.expected);
        luxon::ser::Message msg_ref(&req);
#if defined(FFI_WASM) || defined(__wasm__)
        res = gamePluginBeforeSetProperties(plugin_id_, wrap<GameHandle>(game_), wrap<SerMessageHandle>(&msg_ref), wrap<GamePeerHandle>(info.setter),
                                            info.broadcast, info.target_actor_id, wrap<SerValueHandle>(&up_val), wrap<SerValueHandle>(&exp_val));
#else
        if (g_imports.gamePluginBeforeSetProperties)
            res = g_imports.gamePluginBeforeSetProperties(plugin_id_, wrap<GameHandle>(game_), wrap<SerMessageHandle>(&msg_ref),
                                                          wrap<GamePeerHandle>(info.setter), info.broadcast, info.target_actor_id,
                                                          wrap<SerValueHandle>(&up_val), wrap<SerValueHandle>(&exp_val));
#endif

#ifdef LUXON_SERVER_ENABLE_COROUTINES
        ctx.pop();

        if (res == LUXON_PLUGIN_RESULT_ASYNC)
            res = co_await basiccoro::ffi_await<uint8_t>([&ctx](void *opaque_handle) { ctx.awaiter_handle = opaque_handle; });
#endif

        update_req_from_updated_msg(req, msg_ref);
        lco_return static_cast<server::game_plugins::Result>(res);
    }

    server::Awaitable<server::game_plugins::Result> OnSetProperties(luxon::ser::OperationRequestMessage& req,
                                                                    server::game_plugins::OnSetPropertiesCallInfo& info) override {
#ifdef LUXON_SERVER_ENABLE_COROUTINES
        FfiCoroContext ctx(t_coro_stack);
#endif

        uint8_t res = 0;
        luxon::ser::Value up_val(info.update);
        luxon::ser::Value exp_val(info.expected);
        luxon::ser::Message msg_ref(&req);
#if defined(FFI_WASM) || defined(__wasm__)
        res = gamePluginOnSetProperties(plugin_id_, wrap<GameHandle>(game_), wrap<SerMessageHandle>(&msg_ref), wrap<GamePeerHandle>(info.setter),
                                        info.broadcast, info.target_actor_id, wrap<SerValueHandle>(&up_val), wrap<SerValueHandle>(&exp_val));
#else
        if (g_imports.gamePluginOnSetProperties)
            res = g_imports.gamePluginOnSetProperties(plugin_id_, wrap<GameHandle>(game_), wrap<SerMessageHandle>(&msg_ref), wrap<GamePeerHandle>(info.setter),
                                                      info.broadcast, info.target_actor_id, wrap<SerValueHandle>(&up_val), wrap<SerValueHandle>(&exp_val));
#endif

#ifdef LUXON_SERVER_ENABLE_COROUTINES
        ctx.pop();

        if (res == LUXON_PLUGIN_RESULT_ASYNC)
            res = co_await basiccoro::ffi_await<uint8_t>([&ctx](void *opaque_handle) { ctx.awaiter_handle = opaque_handle; });
#endif

        update_req_from_updated_msg(req, msg_ref);
        lco_return static_cast<server::game_plugins::Result>(res);
    }

    server::game_plugins::Result BeforeCloseGame(server::game_plugins::BeforeCloseGameCallInfo& info) override {
        uint8_t res = 0;
#if defined(FFI_WASM) || defined(__wasm__)
        res = gamePluginBeforeCloseGame(plugin_id_, wrap<GameHandle>(game_), info.failed_on_create);
#else
        if (g_imports.gamePluginBeforeCloseGame)
            res = g_imports.gamePluginBeforeCloseGame(plugin_id_, wrap<GameHandle>(game_), info.failed_on_create);
#endif
        return static_cast<server::game_plugins::Result>(res);
    }

    server::game_plugins::Result OnCloseGame(server::game_plugins::OnCloseGameCallInfo& info) override {
        uint8_t res = 0;
#if defined(FFI_WASM) || defined(__wasm__)
        res = gamePluginOnCloseGame(plugin_id_, wrap<GameHandle>(game_), info.failed_on_create);
#else
        if (g_imports.gamePluginOnCloseGame)
            res = g_imports.gamePluginOnCloseGame(plugin_id_, wrap<GameHandle>(game_), info.failed_on_create);
#endif
        return static_cast<server::game_plugins::Result>(res);
    }
};
#endif

extern "C" {
#if !defined(FFI_WASM) && !defined(__wasm__)
void luxonSetServerImports(const LuxonServerImports *imports) {
    if (imports) {
        g_imports = *imports;
    } else {
        std::memset(&g_imports, 0, sizeof(g_imports));
    }
}
#endif
}

#ifdef LUXON_SERVER_ENABLE_PLUGINS
/* ============================================================================
 * AUTHENTICATION THUNKS & REGISTRATION
 * ============================================================================ */

template <unsigned Type>
static server::Awaitable<server::auth_plugins::registry::AuthResult>
auth_callback_thunk(server::ServerManager&, const std::string& requested_user_id, const std::string& params, const std::string& data,
                    const std::optional<std::string>& secret, const std::optional<std::string>& auth_url) {
#ifdef LUXON_SERVER_ENABLE_COROUTINES
    FfiCoroContext ctx(t_coro_stack);
#endif

    char out_user_id[256];
    out_user_id[0] = '\0';
    luxon::ser::Message err_msg{luxon::ser::OperationResponseMessage{}};
    uint8_t res = 0;

    const char *c_secret = secret ? secret->c_str() : nullptr;
    const char *c_auth_url = auth_url ? auth_url->c_str() : nullptr;

#if defined(FFI_WASM) || defined(__wasm__)
    res = authPluginOnAuthenticate(Type, requested_user_id.c_str(), params.c_str(), data.c_str(), c_secret, c_auth_url, out_user_id, sizeof(out_user_id),
                                   wrap<SerMessageHandle>(&err_msg));
#else
    if (g_imports.authPluginOnAuthenticate) {
        res = g_imports.authPluginOnAuthenticate(Type, requested_user_id.c_str(), params.c_str(), data.c_str(), c_secret, c_auth_url, out_user_id,
                                                 sizeof(out_user_id), wrap<SerMessageHandle>(&err_msg));
    } else {
        lco_return std::unexpected(luxon::ser::OperationResponseMessage{.return_code = luxon::ErrorCodes::Core::InternalServerError,
                                                                        .debug_message = "Authentication handler registered but not implemented"});
    }
#endif

#ifdef LUXON_SERVER_ENABLE_COROUTINES
    ctx.pop();

    if (res == LUXON_AUTH_RESULT_ASYNC)
        res = co_await basiccoro::ffi_await<uint8_t>([&ctx](void *opaque_handle) { ctx.awaiter_handle = opaque_handle; });
#endif

    if (res == LUXON_AUTH_RESULT_SUCCESS) {
        lco_return std::string(out_user_id);
    } else {
        if (auto *resp = err_msg.get_if<luxon::ser::OperationResponseMessage>())
            lco_return std::unexpected(*resp);
        lco_return std::unexpected(luxon::ser::OperationResponseMessage{.return_code = luxon::ErrorCodes::Core::InternalServerError,
                                                                        .debug_message = "Authentication handler did not return an operation response"});
    }
}

template <size_t... Is> static server::auth_plugins::registry::AuthCallback get_auth_thunk(unsigned type, std::index_sequence<Is...>) {
    static constexpr server::auth_plugins::registry::AuthCallback thunks[] = {&auth_callback_thunk<static_cast<unsigned>(Is)>...};
    if (type < sizeof(thunks) / sizeof(thunks[0])) {
        return thunks[type];
    }
    return nullptr;
}
#endif

/* ============================================================================
 * ERROR EXPOSED API IMPLEMENTATION
 * ============================================================================ */

extern "C" {
bool luxonHasError() { return t_has_error; }

const char *luxonGetLastErrorType() { return t_last_error_type.c_str(); }

const char *luxonGetLastErrorMessage() { return t_last_error_msg.c_str(); }

void luxonClearLastError() { clear_ffi_error(); }

/* ============================================================================
 * COROUTINES IMPLEMENTATION
 * ============================================================================ */

CoroutineHandle luxonGetActiveCoroutine() {
#ifdef LUXON_SERVER_ENABLE_COROUTINES
    if (t_coro_stack.empty()) {
        return wrap<CoroutineHandle>(nullptr);
    }
    // Return the top-most context on the stack
    return wrap<CoroutineHandle>(t_coro_stack.top());
#else
    return wrap<CoroutineHandle>(nullptr);
#endif
}

void luxonResumeCoroutine(CoroutineHandle handle, uint8_t result) {
#ifdef LUXON_SERVER_ENABLE_COROUTINES
    auto *ctx = unwrap<FfiCoroContext>(handle);
    if (ctx && ctx->awaiter_handle)
        basiccoro::FfiAwaiter<uint8_t, void (*)(void *)>::resume(ctx->awaiter_handle, result);
#endif
}

void luxonResumeCoroutineSoon(ServerManagerHandle manager, CoroutineHandle handle, uint8_t result) { luxonResumeCoroutineLater(manager, handle, result, 0); }

void luxonResumeCoroutineLater(ServerManagerHandle manager, CoroutineHandle handle, uint8_t result, uint32_t delay_ms) {
#ifdef LUXON_SERVER_ENABLE_COROUTINES
    auto *serman = unwrap<server::ServerManager>(manager);
    auto *ctx = unwrap<FfiCoroContext>(handle);
    if (serman && ctx && ctx->awaiter_handle)
        serman->add_scheduled_task(
            delay_ms, [awaiter_handle = ctx->awaiter_handle, result]() { basiccoro::FfiAwaiter<uint8_t, void (*)(void *)>::resume(awaiter_handle, result); });
#endif
}

void luxonDestroyCoroutine(CoroutineHandle handle) {
#ifdef LUXON_SERVER_ENABLE_COROUTINES
    auto *ctx = unwrap<FfiCoroContext>(handle);
    if (ctx && ctx->awaiter_handle)
        basiccoro::FfiAwaiter<uint8_t, void (*)(void *)>::cancel(ctx);
#endif
}

/* ============================================================================
 * BYTE ARRAY IMPLEMENTATION
 * ============================================================================ */

using luxon::ser::ByteArray;

ByteArrayHandle createByteArray() { return wrap<ByteArrayHandle>(new ByteArray); }

void destroyByteArray(ByteArrayHandle val) {
    if (val)
        delete unwrap<ByteArray>(val);
}

ffi_size_t copyFromByteArray(ByteArrayHandle val, uint8_t *out_buf, ffi_size_t max_len) {
    if (!val)
        return 0;

    auto *array = unwrap<ByteArray>(val);

    ffi_size_t copy_len = std::min(static_cast<ffi_size_t>(array->size()), max_len);

    std::span<uint8_t> out{out_buf, copy_len};
    std::copy(array->begin(), array->begin() + copy_len, out.begin());

    return copy_len;
}

void appendToByteArray(ByteArrayHandle val, const uint8_t *buf, ffi_size_t len) {
    if (!val)
        return;

    auto *array = unwrap<ByteArray>(val);
    std::span<const uint8_t> in_data{buf, len};

    array->insert(array->end(), in_data.begin(), in_data.end());
}

void copyToByteArray(ByteArrayHandle val, const uint8_t *buf, ffi_size_t len) {
    if (!val)
        return;

    auto *array = unwrap<ByteArray>(val);
    std::span<const uint8_t> in_data{buf, len};

    array->clear();
    array->insert(array->end(), in_data.begin(), in_data.end());
}

ffi_size_t getByteArraySize(ByteArrayHandle val) {
    if (auto *array = unwrap<ByteArray>(val))
        return array->size();
    return 0;
}

void *getByteArrayDataPtr(ByteArrayHandle val) {
    if (auto *array = unwrap<ByteArray>(val))
        return array->data();
    return nullptr;
}

void pushToByteArray(ByteArrayHandle val, uint8_t byte) {
    if (auto *array = unwrap<ByteArray>(val))
        return array->push_back(byte);
}

uint8_t getInByteArray(ByteArrayHandle val, ffi_size_t index) {
    if (auto *array = unwrap<ByteArray>(val))
        if (index < array->size())
            return (*array)[index];
    return 0;
}

void setInByteArray(ByteArrayHandle val, ffi_size_t index, uint8_t byte) {
    if (auto *array = unwrap<ByteArray>(val)) {
        if (index >= array->size())
            array->resize(index + 1);
        (*array)[index] = byte;
    }
}

/* ============================================================================
 * SER MESSAGE INTERFACE IMPLEMENTATION
 * ============================================================================ */

SerMessageHandle createSerMessage() {
    return ffi_safe_call<SerMessageHandle>(wrap<SerMessageHandle>(nullptr), [] { return wrap<SerMessageHandle>(new luxon::ser::Message()); });
}

void destroySerMessage(SerMessageHandle val) {
    ffi_safe_exec([=] {
        if (val)
            delete unwrap<luxon::ser::Message>(val);
    });
}

ByteArrayHandle serializeSerMessage(SerMessageHandle val) {
    if (!val)
        return wrap<ByteArrayHandle>(nullptr);
    return ffi_safe_call<ByteArrayHandle>(wrap<ByteArrayHandle>(nullptr), [=] {
        auto *v = unwrap<luxon::ser::Message>(val);
        luxon::ser::IPCBinaryProtocol proto;
        auto res = proto.Serialize(*v);
        if (!res.has_value())
            return wrap<ByteArrayHandle>(nullptr);
        return wrap<ByteArrayHandle>(new ByteArray(std::move(*res)));
    });
}

bool deserializeSerMessage(const uint8_t *buf, ffi_size_t len, SerMessageHandle out_val) {
    if (!buf || !out_val)
        return false;
    return ffi_safe_call<bool>(false, [=] {
        luxon::ser::IPCBinaryProtocol proto;
        auto res = proto.Deserialize(std::span<const uint8_t>{buf, len});
        if (!res.has_value())
            return false;

        auto *target = unwrap<luxon::ser::Message>(out_val);
        *target = *res;
        return true;
    });
}

bool deserializeSerMessageFromByteArray(ByteArrayHandle val, SerMessageHandle out_val) {
    if (!val)
        return false;
    auto *array = unwrap<ByteArray>(val);
    return deserializeSerMessage(array->data(), array->size(), out_val);
}

/* ============================================================================
 * SER VALUE INTERFACE IMPLEMENTATION
 * ============================================================================ */

SerValueHandle createSerValue() {
    return ffi_safe_call<SerValueHandle>(wrap<SerValueHandle>(nullptr), [] { return wrap<SerValueHandle>(new luxon::ser::Value()); });
}

void destroySerValue(SerValueHandle val) {
    ffi_safe_exec([=] {
        if (val)
            delete unwrap<luxon::ser::Value>(val);
    });
}

ByteArrayHandle serializeSerValue(SerValueHandle val) {
    if (!val)
        return wrap<ByteArrayHandle>(nullptr);
    return ffi_safe_call<ByteArrayHandle>(wrap<ByteArrayHandle>(nullptr), [=] {
        auto *v = unwrap<luxon::ser::Value>(val);
        luxon::ser::IPCBinaryProtocol proto;
        luxon::ser::ByteWriter writer;
        auto res = proto.EncodeValue(writer, *v);
        if (!res.has_value())
            return wrap<ByteArrayHandle>(nullptr);
        return wrap<ByteArrayHandle>(new ByteArray(writer.take()));
    });
}

bool deserializeSerValue(const uint8_t *buf, ffi_size_t len, SerValueHandle out_val) {
    if (!buf || !out_val)
        return false;
    return ffi_safe_call<bool>(false, [=] {
        luxon::ser::IPCBinaryProtocol proto;
        luxon::ser::ByteReader reader(std::span<const uint8_t>{buf, len});
        auto res = proto.DecodeValue(reader);
        if (!res.has_value())
            return false;

        auto *target = unwrap<luxon::ser::Value>(out_val);
        *target = *res;
        return true;
    });
}

bool deserializeSerValueFromByteArray(ByteArrayHandle val, SerValueHandle out_val) {
    if (!val)
        return false;
    auto *array = unwrap<ByteArray>(val);
    return deserializeSerValue(array->data(), array->size(), out_val);
}

/* ============================================================================
 * LOGGER INTERFACE IMPLEMENTATION
 * ============================================================================ */

void luxonEnableCustomLogSink(bool enable) {
#ifndef LUXON_SERVER_USE_SPDLOG
    if (enable) {
        server::custom_log_sink = [](server::log_level level, std::string message) {
#if defined(FFI_WASM) || defined(__wasm__)
            customLogSink(static_cast<int32_t>(level), message.c_str());
#else
            if (g_imports.customLogSink)
                g_imports.customLogSink(static_cast<int32_t>(level), message.c_str());
#endif
        };
    } else {
        server::custom_log_sink = nullptr;
    }
#endif
}

LoggerHandle getOrCreateLogger(const char *name) {
    if (!name)
        return wrap<LoggerHandle>(nullptr);
    return ffi_safe_call<LoggerHandle>(wrap<LoggerHandle>(nullptr), [=] {
        std::lock_guard<std::mutex> lock(g_registry_mutex);
        std::string sname(name);
        auto it = g_logger_registry.find(sname);
        if (it == g_logger_registry.end()) {
            auto logger = server::create_logger(sname);
            it = g_logger_registry.emplace(sname, logger).first;
        }
        return wrap<LoggerHandle>(it->second.get());
    });
}

void setLoggerLevel(LoggerHandle logger, int32_t level) {
    if (auto *l = unwrap<server::logger>(logger)) {
        ffi_safe_exec([=] {
            l->set_level(static_cast<server::log_level>(level));
        });
    }
}

#define IMPL_LOG_METHOD(fn_name, lvl_method)                                                                                                                   \
    void fn_name(LoggerHandle logger, const char *message) {                                                                                                   \
        if (!message)                                                                                                                                          \
            return;                                                                                                                                            \
        if (auto *l = unwrap<server::logger>(logger)) {                                                                                                        \
            ffi_safe_exec([=] { l->lvl_method("{}", message); });                                                                                              \
        }                                                                                                                                                      \
    }

IMPL_LOG_METHOD(loggerTrace, trace)
IMPL_LOG_METHOD(loggerDebug, debug)
IMPL_LOG_METHOD(loggerInfo, info)
IMPL_LOG_METHOD(loggerWarn, warn)
IMPL_LOG_METHOD(loggerError, error)
IMPL_LOG_METHOD(loggerCritical, critical)

/* ============================================================================
 * PEER INTERFACE IMPLEMENTATION
 * ============================================================================ */

bool peerIsAuthenticated(PeerHandle peer) {
    return ffi_safe_call<bool>(false, [=] {
        auto *p = unwrap<server::Peer>(peer);
        return p ? p->is_authenticated() : false;
    });
}

LuxonServerProtocol peerGetTransportProtocol(PeerHandle peer) {
    return ffi_safe_call<LuxonServerProtocol>(LUXON_PROTOCOL_UDP, [=] {
        auto *p = unwrap<server::Peer>(peer);
        return p ? static_cast<LuxonServerProtocol>(p->transport_protocol) : LUXON_PROTOCOL_UDP;
    });
}

void peerSend(PeerHandle peer, const uint8_t *payload, ffi_size_t length, uint8_t channel, LuxonDeliveryMode delivery_mode) {
    if (!payload || length == 0)
        return;
    ffi_safe_exec([=] {
        if (auto *p = unwrap<server::Peer>(peer)) {
            luxon::ser::ByteArray arr(payload, payload + length);
            luxon::enet::EnetSendOptions opt{};
            opt.channel = channel;
            opt.mode = static_cast<luxon::enet::EnetDeliveryMode>(delivery_mode);
            p->send(arr, opt);
        }
    });
}

void peerDisconnect(PeerHandle peer) {
    ffi_safe_exec([=] {
        if (auto *p = unwrap<server::Peer>(peer))
            p->disconnect();
    });
}

const char *peerGetUserId(PeerHandle peer) {
    if (auto *p = unwrap<server::Peer>(peer))
        if (auto& pp = p->persistent)
            return pp->user_id.c_str();
    return nullptr;
}

GameHandle peerGetCurrentGame(PeerHandle peer) {
    if (auto *p = unwrap<server::Peer>(peer)) {
        if (auto& pp = p->persistent) {
            if (!pp->app)
                return wrap<GameHandle>(nullptr);
            for (const auto& connection : pp->app->server_manager.get_connections()) {
                if (!connection)
                    continue;
                if (auto *game_server_handler = dynamic_cast<server::GameServerHandler *>(connection.get()))
                    return wrap<GameHandle>(game_server_handler->get_current_game().get());
            }
        }
    }
    return wrap<GameHandle>(nullptr);
}

GameHandle peerGetInvitedGame(PeerHandle peer) {
    if (auto *p = unwrap<server::Peer>(peer)) {
        if (auto& pp = p->persistent) {
            if (!pp->app)
                return wrap<GameHandle>(nullptr);
            // Don't give out a dangling pointer
            std::weak_ptr<server::Game> weak_game = pp->app->server_manager.get_game(*pp->app, pp->get_invitation()).value_or(nullptr);
            return wrap<GameHandle>(weak_game.lock().get());
        }
    }
    return wrap<GameHandle>(nullptr);
}

/* ============================================================================
 * GAME PEER INTERFACE IMPLEMENTATION
 * ============================================================================ */

GamePeerHandle createGamePeerContainer() {
    return ffi_safe_call<GamePeerHandle>(wrap<GamePeerHandle>(nullptr), [] { return wrap<GamePeerHandle>(new server::GamePeer()); });
}

void destroyGamePeerContainer(GamePeerHandle game_peer) {
    ffi_safe_exec([=] {
        if (game_peer)
            delete unwrap<server::GamePeer>(game_peer);
    });
}

bool gamePeerIsValid(GamePeerHandle game_peer) {
    return ffi_safe_call<bool>(false, [=] {
        auto *gp = unwrap<server::GamePeer>(game_peer);
        // A peer handle is only meaningful while its node is still active in
        // its owning room; retired nodes must read as invalid.
        return gp ? gp->is_valid() && !gp->owner_game.expired() : false;
    });
}

int32_t gamePeerGetActorId(GamePeerHandle game_peer) {
    return ffi_safe_call<int32_t>(0, [=] {
        auto *gp = unwrap<server::GamePeer>(game_peer);
        return gp ? gp->actor_id : 0;
    });
}

bool gamePeerHasInterestGroup(GamePeerHandle game_peer, uint8_t group) {
    return ffi_safe_call<bool>(false, [=] {
        auto *gp = unwrap<server::GamePeer>(game_peer);
        return gp && gamePeerIsValid(game_peer) ? gp->interest_groups.test(group) : false;
    });
}

void gamePeerSetInterestGroup(GamePeerHandle game_peer, uint8_t group, bool enable) {
    ffi_safe_exec([=] {
        auto *gp = unwrap<server::GamePeer>(game_peer);
        if (gp && gamePeerIsValid(game_peer))
            gp->interest_groups.set(group, enable);
    });
}

void gamePeerGetInterestGroupsMask(GamePeerHandle game_peer, uint32_t *out_mask_words, ffi_size_t max_words) {
    if (!out_mask_words || max_words == 0)
        return;
    ffi_safe_exec([=] {
        auto *gp = unwrap<server::GamePeer>(game_peer);
        if (gp && gamePeerIsValid(game_peer)) {
            ffi_size_t limit = std::min<ffi_size_t>(max_words, 8);
            for (ffi_size_t w = 0; w < limit; ++w) {
                uint32_t word = 0;
                for (int b = 0; b < 32; ++b) {
                    if (gp->interest_groups.test(w * 32 + b))
                        word |= (1U << b);
                }
                out_mask_words[w] = word;
            }
            for (ffi_size_t w = limit; w < max_words; ++w)
                out_mask_words[w] = 0;
        }
    });
}

void gamePeerSetInterestGroupsMask(GamePeerHandle game_peer, const uint32_t *mask_words, ffi_size_t word_count) {
    if (!mask_words || word_count == 0)
        return;
    ffi_safe_exec([=] {
        auto *gp = unwrap<server::GamePeer>(game_peer);
        if (gp && gamePeerIsValid(game_peer)) {
            gp->interest_groups.reset();
            ffi_size_t limit = std::min<ffi_size_t>(word_count, 8);
            for (ffi_size_t w = 0; w < limit; ++w) {
                uint32_t word = mask_words[w];
                for (int b = 0; b < 32; ++b) {
                    if (word & (1U << b))
                        gp->interest_groups.set(w * 32 + b);
                }
            }
        }
    });
}

void gamePeerGetActorProps(GamePeerHandle game_peer, SerValueHandle out_props) {
    if (!out_props)
        return;
    ffi_safe_exec([=] {
        auto *gp = unwrap<server::GamePeer>(game_peer);
        if (gp && gamePeerIsValid(game_peer))
            *unwrap<luxon::ser::Value>(out_props) = gp->actor_props;
    });
}

void gamePeerSetActorProps(GamePeerHandle game_peer, SerValueHandle props) {
    if (!props)
        return;
    ffi_safe_exec([=] {
        auto *gp = unwrap<server::GamePeer>(game_peer);
        if (gp && gamePeerIsValid(game_peer)) {
            auto *v = unwrap<luxon::ser::Value>(props);
            if (v->is<luxon::ser::HashtablePtr>()) {
                if (auto ptr = v->get<luxon::ser::HashtablePtr>())
                    gp->actor_props = *ptr;
            }
        }
    });
}

PeerHandle gamePeerGetBasePeer(GamePeerHandle game_peer) {
    return ffi_safe_call<PeerHandle>(wrap<PeerHandle>(nullptr), [=] {
        if (auto *gp = unwrap<server::GamePeer>(game_peer)) {
            if (gamePeerIsValid(game_peer))
                if (auto sp = gp->peer.lock())
                    return wrap<PeerHandle>(sp.get());
        }
        return wrap<PeerHandle>(nullptr);
    });
}

bool gamePeerDisconnect(GamePeerHandle game_peer) {
    return ffi_safe_call<bool>(false, [=] {
        auto *gp = unwrap<server::GamePeer>(game_peer);
        return gp && gamePeerIsValid(game_peer) ? gp->disconnect() : false;
    });
}

/* ============================================================================
 * EVENT INTERFACE IMPLEMENTATION
 * ============================================================================ */

EventHandle createEvent() {
    return ffi_safe_call<EventHandle>(wrap<EventHandle>(nullptr), [] { return wrap<EventHandle>(new server::Event()); });
}

void destroyEvent(EventHandle event) {
    ffi_safe_exec([=] {
        if (event)
            delete unwrap<server::Event>(event);
    });
}

void eventSetRoutingMetadata(EventHandle event, uint8_t code, int32_t sender_actor_id, LuxonDeliveryMode delivery_mode, uint8_t channel,
                             uint8_t interest_group) {
    ffi_safe_exec([=] {
        if (auto *ev = unwrap<server::Event>(event)) {
            ev->code = code;
            ev->sender_actor_id = sender_actor_id;
            ev->delivery_mode = static_cast<luxon::enet::EnetDeliveryMode>(delivery_mode);
            ev->channel = channel;
            ev->interest_group = interest_group;
        }
    });
}

void eventGetRoutingMetadata(EventHandle event, uint8_t *out_code, int32_t *out_sender_id, LuxonDeliveryMode *out_mode, uint8_t *out_channel,
                             uint8_t *out_group) {
    ffi_safe_exec([=] {
        if (auto *ev = unwrap<server::Event>(event)) {
            if (out_code)
                *out_code = ev->code;
            if (out_sender_id)
                *out_sender_id = ev->sender_actor_id;
            if (out_mode)
                *out_mode = static_cast<LuxonDeliveryMode>(ev->delivery_mode);
            if (out_channel)
                *out_channel = ev->channel;
            if (out_group)
                *out_group = ev->interest_group;
        }
    });
}

void eventSetReceiversAll(EventHandle event) {
    ffi_safe_exec([=] {
        if (auto *ev = unwrap<server::Event>(event))
            ev->receivers = std::monostate{};
    });
}

void eventSetReceiversGroup(EventHandle event, uint8_t group) {
    ffi_safe_exec([=] {
        if (auto *ev = unwrap<server::Event>(event))
            ev->receivers = group;
    });
}

void eventSetReceiversActors(EventHandle event, const int32_t *actor_ids, ffi_size_t count) {
    if (!actor_ids && count > 0)
        return;
    ffi_safe_exec([=] {
        if (auto *ev = unwrap<server::Event>(event)) {
            std::unordered_set<int32_t> actors(actor_ids, actor_ids + count);
            ev->receivers = std::move(actors);
        }
    });
}

LuxonEventReceiversType eventGetReceiversType(EventHandle event) {
    return ffi_safe_call<LuxonEventReceiversType>(LUXON_RECEIVERS_ALL, [=] {
        if (auto *ev = unwrap<server::Event>(event)) {
            if (std::holds_alternative<uint8_t>(ev->receivers))
                return LUXON_RECEIVERS_GROUP;
            if (std::holds_alternative<std::unordered_set<int32_t>>(ev->receivers))
                return LUXON_RECEIVERS_ACTORS;
        }
        return LUXON_RECEIVERS_ALL;
    });
}

uint8_t eventGetReceiversGroup(EventHandle event) {
    return ffi_safe_call<uint8_t>(0, [=] -> uint8_t {
        if (auto *ev = unwrap<server::Event>(event)) {
            if (auto *g = std::get_if<uint8_t>(&ev->receivers))
                return *g;
        }
        return 0;
    });
}

void eventGetReceiversActors(EventHandle event, int32_t *out_actor_ids, ffi_size_t max_count, ffi_size_t *out_written) {
    if (!out_written)
        return;
    *out_written = 0;
    if (!out_actor_ids || max_count == 0)
        return;
    ffi_safe_exec([=] {
        if (auto *ev = unwrap<server::Event>(event)) {
            if (auto *set = std::get_if<std::unordered_set<int32_t>>(&ev->receivers)) {
                ffi_size_t cur = 0;
                for (int32_t id : *set) {
                    if (cur >= max_count)
                        break;
                    out_actor_ids[cur++] = id;
                }
                *out_written = cur;
            }
        }
    });
}

void eventSetData(EventHandle event, SerValueHandle data) {
    if (!data)
        return;
    ffi_safe_exec([=] {
        if (auto *ev = unwrap<server::Event>(event)) {
            ev->data = *unwrap<luxon::ser::Value>(data);
        }
    });
}

void eventGetData(EventHandle event, SerValueHandle out_data) {
    if (!out_data)
        return;
    ffi_safe_exec([=] {
        if (auto *ev = unwrap<server::Event>(event)) {
            *unwrap<luxon::ser::Value>(out_data) = ev->data;
        }
    });
}

void eventSetTopParams(EventHandle event, SerValueHandle top_params) {
    if (!top_params)
        return;
    ffi_safe_exec([=] {
        if (auto *ev = unwrap<server::Event>(event)) {
            auto *v = unwrap<luxon::ser::Value>(top_params);
            if (v->is<luxon::ser::Dictionary>()) {
                ev->top_params = v->get<luxon::ser::Dictionary>();
            }
        }
    });
}

void eventGetTopParams(EventHandle event, SerValueHandle out_top_params) {
    if (!out_top_params)
        return;
    ffi_safe_exec([=] {
        if (auto *ev = unwrap<server::Event>(event)) {
            *unwrap<luxon::ser::Value>(out_top_params) = ev->top_params;
        }
    });
}

/* ============================================================================
 * GAME INTERFACE IMPLEMENTATION
 * ============================================================================ */

void gameGetId(GameHandle game, char *out_id, ffi_size_t max_len) {
    if (!out_id || max_len == 0)
        return;
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            ffi_size_t len = std::min<ffi_size_t>(max_len - 1, g->id.size());
            std::memcpy(out_id, g->id.data(), len);
            out_id[len] = '\0';
        } else {
            out_id[0] = '\0';
        }
    });
    if (luxonHasError()) {
        out_id[0] = '\0';
    }
}

void gameGetConfigState(GameHandle game, uint8_t *out_flags, bool *out_is_created, bool *out_is_open, bool *out_is_visible, uint8_t *out_max_peers,
                        int32_t *out_master_actor) {
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            const auto state = g->get_config_snapshot();
            if (out_flags)
                *out_flags = state.flags;
            if (out_is_created)
                *out_is_created = state.is_created;
            if (out_is_open)
                *out_is_open = state.is_open;
            if (out_is_visible)
                *out_is_visible = state.is_visible;
            if (out_max_peers)
                *out_max_peers = state.max_peers;
            if (out_master_actor)
                *out_master_actor = state.master_actor;
        }
    });
}

void gameSetConfigState(GameHandle game, uint8_t flags, bool is_open, bool is_visible, uint8_t max_peers, int32_t master_actor) {
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game))
            g->set_config_state(flags, is_open, is_visible, max_peers, master_actor);
    });
}

int32_t gameGetMasterActor(GameHandle game) {
    return ffi_safe_call<int32_t>(0, [=] {
        auto *g = unwrap<server::Game>(game);
        return g ? g->get_config_snapshot().master_actor : 0;
    });
}

void gameSetMasterActor(GameHandle game, int32_t actor_id) {
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            const auto state = g->get_config_snapshot();
            g->set_config_state(state.flags, state.is_open, state.is_visible,
                                state.max_peers, actor_id);
        }
    });
}

int32_t gameGetLastActorId(GameHandle game) {
    return ffi_safe_call<int32_t>(0, [=] {
        auto *g = unwrap<server::Game>(game);
        return g ? g->get_config_snapshot().last_actor_id : 0;
    });
}

uint8_t gameGetMaxPeers(GameHandle game) {
    return ffi_safe_call<uint8_t>(0, [=] {
        auto *g = unwrap<server::Game>(game);
        return g ? g->get_config_snapshot().max_peers : 0;
    });
}

void gameSetMaxPeers(GameHandle game, uint8_t max_peers) {
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            const auto state = g->get_config_snapshot();
            g->set_config_state(state.flags, state.is_open, state.is_visible,
                                max_peers, state.master_actor);
        }
    });
}

void gameGetTtlConfig(GameHandle game, int32_t *out_player_ttl, int32_t *out_empty_ttl) {
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            const auto state = g->get_config_snapshot();
            if (out_player_ttl)
                *out_player_ttl = state.player_ttl;
            if (out_empty_ttl)
                *out_empty_ttl = state.empty_game_ttl;
        }
    });
}

void gameSetTtlConfig(GameHandle game, int32_t player_ttl, int32_t empty_ttl) {
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            std::lock_guard admission_lock(g->admission_mutex);
            g->player_ttl = player_ttl;
            g->empty_game_ttl = empty_ttl;
            ++g->state_revision_;
        }
    });
}

void gameGetCustomProps(GameHandle game, SerValueHandle out_custom_props) {
    if (!out_custom_props)
        return;
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            std::lock_guard admission_lock(g->admission_mutex);
            *unwrap<luxon::ser::Value>(out_custom_props) = g->custom_props;
        }
    });
}

void gameSetCustomProps(GameHandle game, SerValueHandle custom_props) {
    if (!custom_props)
        return;
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            auto *v = unwrap<luxon::ser::Value>(custom_props);
            if (v->is<luxon::ser::HashtablePtr>()) {
                if (auto ptr = v->get<luxon::ser::HashtablePtr>()) {
                    std::lock_guard admission_lock(g->admission_mutex);
                    g->custom_props = *ptr;
                }
            }
        }
    });
}

void gameGetLobbyPropsToSerValue(GameHandle game, SerValueHandle out_keys_array) {
    if (!out_keys_array)
        return;
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            std::lock_guard admission_lock(g->admission_mutex);
            *unwrap<luxon::ser::Value>(out_keys_array) = g->lobby_props;
        }
    });
}

void gameSetLobbyPropsFromSerValue(GameHandle game, SerValueHandle keys_array) {
    if (!keys_array)
        return;
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            auto *v = unwrap<luxon::ser::Value>(keys_array);
            if (v->is<std::vector<std::string>>()) {
                std::lock_guard admission_lock(g->admission_mutex);
                g->lobby_props = v->get<std::vector<std::string>>();
            }
        }
    });
}

void gameGetExpectedUsersToSerValue(GameHandle game, SerValueHandle out_list) {
    if (!out_list)
        return;
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            std::lock_guard admission_lock(g->admission_mutex);
            std::vector<std::string> vec(g->expected_users.begin(), g->expected_users.end());
            *unwrap<luxon::ser::Value>(out_list) = vec;
        }
    });
}

void gameSetExpectedUsersFromSerValue(GameHandle game, SerValueHandle list) {
    if (!list)
        return;
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            auto *v = unwrap<luxon::ser::Value>(list);
            if (v->is<std::vector<std::string>>()) {
                const auto& vec = v->get<std::vector<std::string>>();
                std::lock_guard admission_lock(g->admission_mutex);
                std::vector<std::string> previous_expected_users(
                    g->expected_users.begin(),
                    g->expected_users.end());
                for (const auto& expected_user : previous_expected_users)
                    g->remove_expected_user(expected_user);
                for (const auto& expected_user : vec)
                    if (!expected_user.empty())
                        g->reserve_expected_user(expected_user);
            }
        }
    });
}

void gameAddExpectedUser(GameHandle game, const char *user_id) {
    if (!user_id)
        return;
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game))
            g->reserve_expected_user(user_id);
    });
}

void gameRemoveExpectedUser(GameHandle game, const char *user_id) {
    if (!user_id)
        return;
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game))
            g->remove_expected_user(user_id);
    });
}

ffi_size_t gameGetPeerCount(GameHandle game) {
    return ffi_safe_call<ffi_size_t>(0, [=] {
        auto *g = unwrap<server::Game>(game);
        return g ? static_cast<ffi_size_t>(g->active_peer_count()) : 0;
    });
}

GamePeerHandle gameCreatePeer(GameHandle game, PeerHandle peer) {
    return ffi_safe_call<GamePeerHandle>(wrap<GamePeerHandle>(nullptr), [=] {
        auto *g = unwrap<server::Game>(game);
        auto *p = unwrap<server::Peer>(peer);
        if (!g || !p)
            return wrap<GamePeerHandle>(nullptr);

        std::shared_ptr<server::Peer> sp(p, [](server::Peer *) {});
        return wrap<GamePeerHandle>(new server::GamePeer(g->create_peer(sp)));
    });
}

GamePeerHandle gameAddPeer(GameHandle game, GamePeerHandle game_peer) {
    return ffi_safe_call<GamePeerHandle>(wrap<GamePeerHandle>(nullptr), [=] {
        auto *g = unwrap<server::Game>(game);
        auto *gp_heap = unwrap<server::GamePeer>(game_peer);
        if (!g || !gp_heap)
            return wrap<GamePeerHandle>(nullptr);

        server::GamePeer *persistent_slot = nullptr;
        {
            std::lock_guard admission_lock(g->admission_mutex);
            persistent_slot = g->add_peer(std::move(*gp_heap));
        }
        delete gp_heap;
        return wrap<GamePeerHandle>(persistent_slot);
    });
}

bool gameRemovePeer(GameHandle game, PeerHandle peer) {
    return ffi_safe_call<bool>(false, [=] {
        auto *g = unwrap<server::Game>(game);
        auto *p = unwrap<server::Peer>(peer);
        if (!g || !p)
            return false;

        std::shared_ptr<server::Peer> sp(p, [](server::Peer *) {});
        return g->remove_peer(sp);
    });
}

bool gameFloodPeer(GameHandle game, GamePeerHandle game_peer) {
    return ffi_safe_call<bool>(false, [=] {
        auto *g = unwrap<server::Game>(game);
        auto *gp = unwrap<server::GamePeer>(game_peer);
        if (!g || !gp)
            return false;
        // Resolve through the actor id so a retired node handle cannot be
        // flooded after it has left the active peer list.
        std::lock_guard admission_lock(g->admission_mutex);
        return g->flood_peer_by_actor(gp->actor_id);
    });
}

GamePeerHandle gameFindPeerByActorId(GameHandle game, int32_t actor_id) {
    return ffi_safe_call<GamePeerHandle>(wrap<GamePeerHandle>(nullptr), [=] {
        auto *g = unwrap<server::Game>(game);
        if (!g)
            return wrap<GamePeerHandle>(nullptr);
        std::lock_guard admission_lock(g->admission_mutex);
        return wrap<GamePeerHandle>(g->find_peer(actor_id));
    });
}

GamePeerHandle gameFindPeerByBasePeer(GameHandle game, PeerHandle peer) {
    return ffi_safe_call<GamePeerHandle>(wrap<GamePeerHandle>(nullptr), [=] {
        auto *g = unwrap<server::Game>(game);
        auto *p = unwrap<server::Peer>(peer);
        if (!g || !p)
            return wrap<GamePeerHandle>(nullptr);

        std::shared_ptr<server::Peer> sp(p, [](server::Peer *) {});
        std::lock_guard admission_lock(g->admission_mutex);
        return wrap<GamePeerHandle>(g->find_peer(sp));
    });
}

void gameBroadcastEvent(GameHandle game, EventHandle event) {
    ffi_safe_exec([=] {
        auto *g = unwrap<server::Game>(game);
        auto *ev = unwrap<server::Event>(event);
        if (g && ev)
            g->broadcast_event(*ev);
    });
}

bool gameValidateJoin(GameHandle game, const char *user_id, ffi_size_t new_expected_users_count, int16_t *out_err_code, char *out_err_str,
                      ffi_size_t max_err_len) {
    if (!user_id)
        return false;
    return ffi_safe_call<bool>(false, [=] {
        auto *g = unwrap<server::Game>(game);
        if (!g)
            return false;

        auto res = g->validate_join(user_id, new_expected_users_count);
        if (out_err_code)
            *out_err_code = res.first;

        if (res.first != 0) {
            if (out_err_str && max_err_len > 0) {
                ffi_size_t cplen = std::min<ffi_size_t>(max_err_len - 1, res.second.size());
                std::memcpy(out_err_str, res.second.data(), cplen);
                out_err_str[cplen] = '\0';
            }
            return false;
        }
        if (out_err_str && max_err_len > 0)
            out_err_str[0] = '\0';
        return true;
    });
}

void gameTriggerLobbyUpdate(GameHandle game) {
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game))
            g->trigger_lobby_update();
    });
}

void gameGetGameProp(GameHandle game, SerValueHandle key, SerValueHandle out_val) {
    if (!key || !out_val)
        return;
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            *unwrap<luxon::ser::Value>(out_val) = g->get_game_prop(*unwrap<luxon::ser::Value>(key));
        }
    });
}

void gameGetLobbyGameProps(GameHandle game, SerValueHandle out_hashtable) {
    if (!out_hashtable)
        return;
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            *unwrap<luxon::ser::Value>(out_hashtable) = g->get_lobby_game_props();
        }
    });
}

void gameGetGameProps(GameHandle game, bool no_custom, SerValueHandle out_hashtable) {
    if (!out_hashtable)
        return;
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            *unwrap<luxon::ser::Value>(out_hashtable) = g->get_game_props(no_custom);
        }
    });
}

void gameGetActorProps(GameHandle game, SerValueHandle out_hashtable) {
    if (!out_hashtable)
        return;
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            *unwrap<luxon::ser::Value>(out_hashtable) = g->get_actor_props();
        }
    });
}

void gameInsertGameProps(GameHandle game, SerValueHandle update, bool broadcast) {
    if (!update)
        return;
    ffi_safe_exec([=] {
        if (auto *g = unwrap<server::Game>(game)) {
            auto *v = unwrap<luxon::ser::Value>(update);
            if (v->is<luxon::ser::HashtablePtr>()) {
                if (auto ptr = v->get<luxon::ser::HashtablePtr>()) {
                    g->insert_game_props(*ptr);
                    if (broadcast) {
                        auto event = g->create_property_update_event(0, *ptr);
                        g->broadcast_event(event);
                    }
                }
            }
        }
    });
}

bool gameExpectGameProps(GameHandle game, SerValueHandle expected) {
    if (!expected)
        return false;
    return ffi_safe_call<bool>(false, [=] {
        auto *g = unwrap<server::Game>(game);
        auto *v = unwrap<luxon::ser::Value>(expected);
        if (g && v->is<luxon::ser::HashtablePtr>()) {
            if (auto ptr = v->get<luxon::ser::HashtablePtr>()) {
                return g->expect_game_props(*ptr);
            }
        }
        return false;
    });
}

bool gameInsertActorProps(GameHandle game, int32_t actor_id, SerValueHandle update, bool broadcast) {
    if (!update)
        return false;
    return ffi_safe_call<bool>(false, [=] {
        auto *g = unwrap<server::Game>(game);
        auto *v = unwrap<luxon::ser::Value>(update);
        if (g && v->is<luxon::ser::HashtablePtr>()) {
            if (auto ptr = v->get<luxon::ser::HashtablePtr>()) {
                if (g->insert_actor_props(actor_id, *ptr)) {
                    if (broadcast) {
                        auto event = g->create_property_update_event(0, *ptr, actor_id);
                        g->broadcast_event(event);
                    }
                    return true;
                }
                return false;
            }
        }
        return false;
    });
}

bool gameExpectActorProps(GameHandle game, int32_t actor_id, SerValueHandle expected) {
    if (!expected)
        return false;
    return ffi_safe_call<bool>(false, [=] {
        auto *g = unwrap<server::Game>(game);
        auto *v = unwrap<luxon::ser::Value>(expected);
        if (g && v->is<luxon::ser::HashtablePtr>()) {
            if (auto ptr = v->get<luxon::ser::HashtablePtr>()) {
                return g->expect_actor_props(actor_id, *ptr);
            }
        }
        return false;
    });
}

bool gameMatchesFilter(SerValueHandle event_data, SerValueHandle filter) {
    if (!event_data || !filter)
        return false;
    return ffi_safe_call<bool>(false, [=] {
        auto *d = unwrap<luxon::ser::Value>(event_data);
        auto *f = unwrap<luxon::ser::Value>(filter);
        if (f->is<luxon::ser::HashtablePtr>()) {
            if (auto ptr = f->get<luxon::ser::HashtablePtr>()) {
                return server::Game::matches_filter(*d, *ptr);
            }
        }
        return false;
    });
}

/* ============================================================================
 * LOBBY INTERFACE IMPLEMENTATION
 * ============================================================================ */

LobbyHandle createLobby(const char *name, uint8_t type) {
    if (!name)
        return wrap<LobbyHandle>(nullptr);
    return ffi_safe_call<LobbyHandle>(wrap<LobbyHandle>(nullptr), [=] { return wrap<LobbyHandle>(new server::Lobby(nullptr, name, type)); });
}

void destroyLobby(LobbyHandle lobby) {
    ffi_safe_exec([=] {
        if (lobby)
            delete unwrap<server::Lobby>(lobby);
    });
}

GameHandle lobbyCreateGame(LobbyHandle lobby, const char *id, bool or_get, SerMessageHandle out_err_resp) {
    if (!id)
        return wrap<GameHandle>(nullptr);
    return ffi_safe_call<GameHandle>(wrap<GameHandle>(nullptr), [=] {
        auto *l = unwrap<server::Lobby>(lobby);
        if (!l)
            return wrap<GameHandle>(nullptr);

        auto res = l->create_game(id, l->app->server_manager.get_random_server_base_address(server::ServerType::GameServer), or_get);
        if (res.has_value()) {
            return wrap<GameHandle>(res->get());
        } else {
            if (out_err_resp)
                *unwrap<luxon::ser::Message>(out_err_resp) = res.error();
            return wrap<GameHandle>(nullptr);
        }
    });
}

ffi_size_t lobbyGetPeerCount(LobbyHandle lobby) {
    return ffi_safe_call<ffi_size_t>(0, [=] {
        auto *l = unwrap<server::Lobby>(lobby);
        return l ? static_cast<ffi_size_t>(l->get_peer_count()) : 0;
    });
}

ffi_size_t lobbyGetMasterPeerCount(LobbyHandle lobby) {
    return ffi_safe_call<ffi_size_t>(0, [=] {
        auto *l = unwrap<server::Lobby>(lobby);
        return l ? static_cast<ffi_size_t>(l->get_master_peer_count()) : 0;
    });
}

bool lobbyQueryLobbies(LobbyHandle lobby, const char *sql_queries, char *out_buf, ffi_size_t max_len, ffi_size_t *out_written) {
    if (!sql_queries || !out_buf || !out_written)
        return false;
    return ffi_safe_call<bool>(false, [=] {
        auto *l = unwrap<server::Lobby>(lobby);
        if (!l)
            return false;

        auto vec = l->query_lobbies(sql_queries);
        ffi_size_t cur = 0;
        for (const auto& s : vec) {
            if (cur + s.size() + 1 > max_len)
                return false;
            std::memcpy(out_buf + cur, s.data(), s.size());
            cur += static_cast<ffi_size_t>(s.size());
            out_buf[cur++] = '\0';
        }
        if (vec.empty() && max_len > 0) {
            out_buf[0] = '\0';
            cur = 1;
        }
        *out_written = cur;
        return true;
    });
}

/* ============================================================================
 * SERVER MANAGER CONFIG BUILDER IMPLEMENTATION
 * ============================================================================ */

ServerManagerConfigHandle createServerManagerConfig() {
    return ffi_safe_call<ServerManagerConfigHandle>(wrap<ServerManagerConfigHandle>(nullptr),
                                                    [] { return wrap<ServerManagerConfigHandle>(new server::ServerManagerConfig()); });
}

void destroyServerManagerConfig(ServerManagerConfigHandle config) {
    ffi_safe_exec([=] {
        if (config)
            delete unwrap<server::ServerManagerConfig>(config);
    });
}

ServerManagerConfigHandle loadServerManagerConfigFromFile(const char *config_file_path) {
    if (!config_file_path)
        return wrap<ServerManagerConfigHandle>(nullptr);
    return ffi_safe_call<ServerManagerConfigHandle>(wrap<ServerManagerConfigHandle>(nullptr), [=] {
        auto cfg = server::ServerManager::load_config_from_file(config_file_path);
        return wrap<ServerManagerConfigHandle>(new server::ServerManagerConfig(std::move(cfg)));
    });
}

ServerManagerConfigHandle parseServerManagerConfig(const char *yaml_config_contents) {
    if (!yaml_config_contents)
        return wrap<ServerManagerConfigHandle>(nullptr);
    return ffi_safe_call<ServerManagerConfigHandle>(wrap<ServerManagerConfigHandle>(nullptr), [=] {
        auto cfg = server::ServerManager::parse_config(yaml_config_contents);
        return wrap<ServerManagerConfigHandle>(new server::ServerManagerConfig(std::move(cfg)));
    });
}

void serverManagerConfigAddServer(ServerManagerConfigHandle config, LuxonServerType type, uint16_t port) {
    ffi_safe_exec([=] {
        if (auto *c = unwrap<server::ServerManagerConfig>(config)) {
            c->add_server(static_cast<server::ServerType>(type), port);
        }
    });
}

void serverManagerConfigAddServerAndEndpoint(ServerManagerConfigHandle config, LuxonServerType type, uint16_t port, const char *external_udp_address) {
    if (!external_udp_address)
        return;
    ffi_safe_exec([=] {
        if (auto *c = unwrap<server::ServerManagerConfig>(config)) {
            c->add_server(static_cast<server::ServerType>(type), port, external_udp_address);
        }
    });
}

void serverManagerConfigSetLimits(ServerManagerConfigHandle config, bool enable_ipv6, unsigned max_connections, unsigned max_game_peers,
                                  uint32_t tick_time_budget) {
    ffi_safe_exec([=] {
        if (auto *c = unwrap<server::ServerManagerConfig>(config)) {
            c->enable_ipv6 = enable_ipv6;
            c->max_connections = max_connections;
            c->max_game_peers = max_game_peers;
            c->tick_time_budget = tick_time_budget;
        }
    });
}

void serverManagerConfigSetSettingsDatabasePath(ServerManagerConfigHandle config, const char *path) {
    ffi_safe_exec([=] {
        if (auto *c = unwrap<server::ServerManagerConfig>(config)) {
#ifdef LUXON_SERVER_ENABLE_SETTINGS_DATABASE
            c->settings_database_path = path ? path : "";
#endif
        }
    });
}

void serverManagerConfigEnableHttp(ServerManagerConfigHandle config, const char *address, uint16_t port) {
    ffi_safe_exec([=] {
        if (auto *c = unwrap<server::ServerManagerConfig>(config)) {
#ifdef LUXON_SERVER_ENABLE_WEBSERVER
            c->enable_http(address ? address : "0.0.0.0", port);
#endif
        }
    });
}

void serverManagerConfigDisableHttp(ServerManagerConfigHandle config) {
    ffi_safe_exec([=] {
        if (auto *c = unwrap<server::ServerManagerConfig>(config)) {
#ifdef LUXON_SERVER_ENABLE_WEBSERVER
            c->disable_http();
#endif
        }
    });
}

/* ============================================================================
 * SERVER MANAGER INTERFACE IMPLEMENTATION
 * ============================================================================ */

ServerManagerHandle createServerManagerFromConfig(ServerManagerConfigHandle config) {
    return ffi_safe_call<ServerManagerHandle>(wrap<ServerManagerHandle>(nullptr), [=] {
        auto *c = unwrap<server::ServerManagerConfig>(config);
        if (!c)
            return wrap<ServerManagerHandle>(nullptr);
        auto *manager = new server::ServerManager(std::move(*c));
        delete c;
        return wrap<ServerManagerHandle>(manager);
    });
}

ServerManagerHandle createServerManagerFromFile(const char *config_file_path) {
    if (!config_file_path)
        return wrap<ServerManagerHandle>(nullptr);
    return ffi_safe_call<ServerManagerHandle>(wrap<ServerManagerHandle>(nullptr),
                                              [=] { return wrap<ServerManagerHandle>(new server::ServerManager(config_file_path)); });
}

ServerManagerHandle createServerManagerFromContents(const char *yaml_config_contents) {
    if (!yaml_config_contents)
        return wrap<ServerManagerHandle>(nullptr);
    return ffi_safe_call<ServerManagerHandle>(wrap<ServerManagerHandle>(nullptr), [=] {
        auto cfg = server::ServerManager::parse_config(yaml_config_contents);
        return wrap<ServerManagerHandle>(new server::ServerManager(std::move(cfg)));
    });
}

ServerManagerHandle createServerManagerFromIPC(const char *ipc_fd) {
#ifdef LUXON_SERVER_ENABLE_MULTIPROCESSING
    if (!ipc_fd || !*ipc_fd)
        return wrap<ServerManagerHandle>(nullptr);
    return ffi_safe_call<ServerManagerHandle>(wrap<ServerManagerHandle>(nullptr), [=] {
        auto real_ipc_fd = server::IPC::socket_from_string(ipc_fd);
        if (!real_ipc_fd || *real_ipc_fd == server::IPC::INVALID_OS_SOCKET)
            return wrap<ServerManagerHandle>(nullptr);
        return wrap<ServerManagerHandle>(new server::ServerManager((server::IPC(*real_ipc_fd))));
    });
#else
    return wrap<ServerManagerHandle>(nullptr);
#endif
}

void destroyServerManager(ServerManagerHandle manager) {
    ffi_safe_exec([=] {
        if (manager)
            delete unwrap<server::ServerManager>(manager);
    });
}

void serverManagerRun(ServerManagerHandle manager) {
    ffi_safe_exec([=] {
        if (auto *m = unwrap<server::ServerManager>(manager))
            m->run();
    });
}

bool serverManagerRunOnce(ServerManagerHandle manager) {
    return ffi_safe_call<bool>(false, [=] {
        auto *m = unwrap<server::ServerManager>(manager);
        return m ? m->run_once() : false;
    });
}

void serverManagerStop(ServerManagerHandle manager) {
    ffi_safe_exec([=] {
        if (auto *m = unwrap<server::ServerManager>(manager))
            m->stop();
    });
}

bool serverManagerGetEndpointOf(ServerManagerHandle manager, LuxonServerType server_type, LuxonServerProtocol server_proto, char *out_address,
                                ffi_size_t max_len) {
    if (!out_address || max_len == 0)
        return false;
    return ffi_safe_call<bool>(false, [=] {
        auto *m = unwrap<server::ServerManager>(manager);
        if (!m)
            return false;

        std::string_view ep = m->get_random_server_address(static_cast<server::ServerType>(server_type), static_cast<server::ServerProtocol>(server_proto));
        if (ep.size() >= max_len)
            return false;

        std::memcpy(out_address, ep.data(), ep.size());
        out_address[ep.size()] = '\0';
        return true;
    });
}

ffi_size_t serverManagerGetConnectionCount(ServerManagerHandle manager) {
    return ffi_safe_call<ffi_size_t>(0, [=] {
        auto *m = unwrap<server::ServerManager>(manager);
        return m ? static_cast<ffi_size_t>(m->get_connection_count()) : 0;
    });
}

unsigned serverManagerGetMaxConnections(ServerManagerHandle manager) {
    return ffi_safe_call<unsigned>(0, [=] {
        auto *m = unwrap<server::ServerManager>(manager);
        return m ? m->get_max_connections() : 0;
    });
}

uint8_t serverManagerGetMaxGamePeers(ServerManagerHandle manager) {
    return ffi_safe_call<uint8_t>(0, [=] {
        auto *m = unwrap<server::ServerManager>(manager);
        return m ? m->get_max_game_peers() : 0;
    });
}

bool serverManagerEnableMultiprocessing(ServerManagerHandle manager) {
#ifdef LUXON_SERVER_ENABLE_MULTIPROCESSING
    return ffi_safe_call<uint8_t>(0, [=] {
        auto *m = unwrap<server::ServerManager>(manager);
        if (!m)
            return false;
#if defined(FFI_WASM) || defined(__wasm__)
        m->handle_start_subprocess = [](const std::string& fd) { multiProcessingLaunchIPCChild(fd.c_str()); };
#else
        if (!g_imports.multiProcessingLaunchIPCChild)
            return false;
        m->handle_start_subprocess = [] (const std::string& fd) { g_imports.multiProcessingLaunchIPCChild(fd.c_str()); };
#endif
        return true;
    });
#else
    return false;
#endif
}

/* ============================================================================
 * HANDLER & GAME EXTENSIONS IMPLEMENTATION
 * ============================================================================ */

ServerManagerHandle handlerBaseGetServerManager(HandlerBaseHandle handler) {
    return ffi_safe_call<ServerManagerHandle>(wrap<ServerManagerHandle>(nullptr), [=] {
        if (auto *h = unwrap<server::HandlerBase>(handler)) {
            return wrap<ServerManagerHandle>(&h->get_server_manager());
        }
        return wrap<ServerManagerHandle>(nullptr);
    });
}

ServerManagerHandle gameGetServerManager(GameHandle game) {
    return ffi_safe_call<ServerManagerHandle>(wrap<ServerManagerHandle>(nullptr), [=] {
        if (auto *g = unwrap<server::Game>(game)) {
            return wrap<ServerManagerHandle>(&g->get_server_manager());
        }
        return wrap<ServerManagerHandle>(nullptr);
    });
}

/* ============================================================================
 * COMMAND RESTARTER INTERFACE IMPLEMENTATION
 * ============================================================================ */

bool serverManagerShouldAbortActiveCommand(ServerManagerHandle manager) { return false; }

void serverManagerMarkCommandCommited(ServerManagerHandle manager) {}

CommandRestarterHandle serverManagerTakeCommandRestarter(ServerManagerHandle manager) {
    return wrap<CommandRestarterHandle>(nullptr);
}

void commandRestarterRestart(CommandRestarterHandle restarter) {}

void destroyCommandRestarter(CommandRestarterHandle restarter) {}

/* ============================================================================
 * PLUGINS & HOOKPOINTS REGISTRATION IMPLEMENTATION
 * ============================================================================ */

uint32_t registerGamePlugin(const char *plugin_name) {
    if (!plugin_name)
        return 0;
#ifdef LUXON_SERVER_ENABLE_PLUGINS
    return ffi_safe_call<uint32_t>(0, [=] {
        uint32_t id = g_next_plugin_id.fetch_add(1);
        std::string name_str(plugin_name);
        bool ok =
            server::game_plugins::registry::register_(name_str, [id, name_str](server::Game *g) { return std::make_unique<FfiGamePlugin>(g, name_str, id); });
        return ok ? id : 0;
    });
#else
    return 0;
#endif
}

bool registerAuthPlugin(unsigned type) {
#ifdef LUXON_SERVER_ENABLE_PLUGINS
    return ffi_safe_call<bool>(false, [=] {
        // Instantiate static non-capturing thunks for provider types 0 through 63
        auto callback = get_auth_thunk(type, std::make_index_sequence<64>{});
        if (!callback) {
            // Type requested is out of supported sequence bounds
            return false;
        }
        return server::auth_plugins::registry::register_(type, callback);
    });
#else
    return false;
#endif
}

void registerHookpoints(ServerManagerHandle manager) {
#ifdef LUXON_SERVER_ENABLE_HOOKPOINTS
    ffi_safe_exec([=] {
        auto *m = unwrap<server::ServerManager>(manager);
        if (!m)
            return;

        using server::Awaitable;

        m->hookpoints.MasterServer_HandleOperationRequest_JoinGame = [](server::MasterServerHandler& handler, const std::string& game_id,
                                                                        bool is_join) -> Awaitable<bool> {
#ifdef LUXON_SERVER_ENABLE_COROUTINES
            FfiCoroContext ctx(t_coro_stack);
#endif

            uint8_t res = 0;

#if defined(FFI_WASM) || defined(__wasm__)
            res = hookpointMasterServerHandleOperationRequestJoinGame(wrap<HandlerBaseHandle>(&handler), game_id.c_str(), is_join);
#else
            if (g_imports.hookpointMasterServerHandleOperationRequestJoinGame)
                res = g_imports.hookpointMasterServerHandleOperationRequestJoinGame(wrap<HandlerBaseHandle>(&handler), game_id.c_str(), is_join);
#endif

#ifdef LUXON_SERVER_ENABLE_COROUTINES
            ctx.pop();

            if (res == LUXON_HOOKPOINT_RESULT_ASYNC)
                res = co_await basiccoro::ffi_await<uint8_t>([&ctx](void *opaque_handle) { ctx.awaiter_handle = opaque_handle; });
#endif

            lco_return res == LUXON_HOOKPOINT_RESULT_OVERRIDE;
        };

        m->hookpoints.MasterServer_HandleOperationRequest_CreateGame = [](server::MasterServerHandler& handler, const std::string& game_id) -> Awaitable<bool> {
#ifdef LUXON_SERVER_ENABLE_COROUTINES
            FfiCoroContext ctx(t_coro_stack);
#endif

            uint8_t res = 0;

#if defined(FFI_WASM) || defined(__wasm__)
            res = hookpointMasterServerHandleOperationRequestCreateGame(wrap<HandlerBaseHandle>(&handler), game_id.c_str());
#else
            if (g_imports.hookpointMasterServerHandleOperationRequestCreateGame)
                res = g_imports.hookpointMasterServerHandleOperationRequestCreateGame(wrap<HandlerBaseHandle>(&handler), game_id.c_str());
#endif

#ifdef LUXON_SERVER_ENABLE_COROUTINES
            ctx.pop();

            if (res == LUXON_HOOKPOINT_RESULT_ASYNC)
                res = co_await basiccoro::ffi_await<uint8_t>([&ctx](void *opaque_handle) { ctx.awaiter_handle = opaque_handle; });
#endif

            lco_return res == LUXON_HOOKPOINT_RESULT_OVERRIDE;
        };

        m->hookpoints.HandlerBase_HandleENetCommand_OnMessage = [](server::HandlerBase& handler, luxon::ser::Message& message,
                                                                   luxon::enet::EnetCommandHeader& /*header*/) -> Awaitable<bool> {
#ifdef LUXON_SERVER_ENABLE_COROUTINES
            FfiCoroContext ctx(t_coro_stack);
#endif

            uint8_t res = 0;

#if defined(FFI_WASM) || defined(__wasm__)
            res = hookpointHandlerBaseHandleENetCommandOnMessage(wrap<HandlerBaseHandle>(&handler), wrap<SerMessageHandle>(&message));
#else
            if (g_imports.hookpointHandlerBaseHandleENetCommandOnMessage)
                res = g_imports.hookpointHandlerBaseHandleENetCommandOnMessage(wrap<HandlerBaseHandle>(&handler), wrap<SerMessageHandle>(&message));
#endif

#ifdef LUXON_SERVER_ENABLE_COROUTINES
            ctx.pop();

            if (res == LUXON_HOOKPOINT_RESULT_ASYNC)
                res = co_await basiccoro::ffi_await<uint8_t>([&ctx](void *opaque_handle) { ctx.awaiter_handle = opaque_handle; });
#endif

            lco_return res == LUXON_HOOKPOINT_RESULT_OVERRIDE;
        };

        m->hookpoints.App_load_app_settings = [](server::App& app, server::AppSettings& /*settings*/, bool& success) -> bool {
            // TODO: Implement
            return false;
        };
    });
#endif
}
} // extern "C"
