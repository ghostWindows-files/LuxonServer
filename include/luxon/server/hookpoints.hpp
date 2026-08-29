// Copyright (c) 2026, the Luxon Server contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#ifdef LUXON_SERVER_ENABLE_HOOKPOINTS
#include "global.hpp"
#include "handler_base.hpp"
#include "coro_support.hpp"

#include <string>
#include <functional>
#include <luxon/ser_types.hpp>

namespace luxon::enet {
class EnetCommandHeader;
}

namespace server {
class HandlerBase;
class MasterServerHandler;
class App;
struct AppSettings;

struct Hookpoints {
    std::function<Awaitable<bool>(MasterServerHandler&, const std::string&, bool)> MasterServer_HandleOperationRequest_JoinGame;
    std::function<Awaitable<bool>(MasterServerHandler&, const std::string&)> MasterServer_HandleOperationRequest_CreateGame;
    std::function<Awaitable<bool>(HandlerBase&, ser::Message&, enet::EnetCommandHeader&)> HandlerBase_HandleENetCommand_OnMessage;
    std::function<bool(App&, AppSettings&, bool& success)> App_load_app_settings;
};
} // namespace server

#define LUXON_SERVER_HOOKPOINT_CSM(custom_server_manager, name, ...)                                                                                           \
    if (custom_server_manager.hookpoints.name && (lco_await custom_server_manager.hookpoints.name(*this, __VA_ARGS__)))                                        \
    lco_return
#define LUXON_SERVER_HOOKPOINT_CSM_SYNC(custom_server_manager, name, ...)                                                                                      \
    if (custom_server_manager.hookpoints.name && (custom_server_manager.hookpoints.name(*this, __VA_ARGS__)))                                                  \
    return
#define LUXON_SERVER_HOOKPOINT(name, ...) LUXON_SERVER_HOOKPOINT_CSM(server_manager_, name, __VA_ARGS__)
#define LUXON_SERVER_HOOKPOINT_SYNC(name, ...) LUXON_SERVER_HOOKPOINT_CSM_SYNC(server_manager_, name, __VA_ARGS__)
#else
#define LUXON_SERVER_HOOKPOINT_CSM(...)
#define LUXON_SERVER_HOOKPOINT_CSM_SYNC(...)
#define LUXON_SERVER_HOOKPOINT(...)
#define LUXON_SERVER_HOOKPOINT_SYNC(...)
#endif
