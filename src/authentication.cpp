// Copyright (c) 2026, the Luxon Server contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "authentication.hpp"
#include "global.hpp"
#include "peer.hpp"
#include "peer_persistence.hpp"
#include "apps.hpp"
#include "data_model.hpp"
#include "server_manager.hpp"
#ifdef LUXON_SERVER_ENABLE_PLUGINS
#include "auth_plugin_registry.hpp"
#endif

#include <format>
#include <algorithm>
#include <random>
#include <luxon/ser_interface.hpp>
#include <luxon/common_codes.hpp>
#include <tracy/Tracy.hpp>

namespace server {
namespace models {
using namespace DictKeyCodes::LoadBalancing;

using TokenAuth = Model<Parameter<std::string, Token>>;

using StandardAuth =
    Model<Parameter<std::string, ApplicationId>, Parameter<uint8_t, DictKeyCodes::AuthAndLobby::ClientAuthenticationType, false, DefaultConst<0>>,
          Parameter<std::string, DictKeyCodes::AuthAndLobby::ClientAuthenticationParameters, false, DefaultString<"">>,
          Parameter<std::string, AppVersion, true>, Parameter<std::string, UserId, true>>;
} // namespace models

namespace {
std::string generate_user_id() {
    static std::mt19937 gen{std::random_device{}()};
    const std::string_view charset = "0123456789ABCDEF";
    std::uniform_int_distribution<size_t> dist(0, charset.size() - 1);

    std::string fres(32, '\0');
    std::ranges::generate(fres, [&] { return charset[dist(gen)]; });
    return fres;
}

std::string get_anonymous_user_id(unsigned custom_anonymous_uid_mode, std::string_view prefix, std::string requested_uid) {
    if (custom_anonymous_uid_mode == AppSettings::CustomAnonymousUIDMode::ForceRandom)
        return std::format("{}{}", prefix, generate_user_id());

    if (requested_uid.empty())
        requested_uid = generate_user_id();

    if (custom_anonymous_uid_mode == AppSettings::CustomAnonymousUIDMode::AllowWithPrefix)
        return std::format("{}{}", prefix, requested_uid);
    else
        return requested_uid;
}
} // namespace

Awaitable<ser::OperationResponseMessage> authenticate(ServerManager& server_manager, Peer& peer, const ser::OperationRequestMessage& req,
                                                      const enet::EnetCommandHeader& cmd_header, bool refresh_token) {
    ZoneScoped;

    // Stop if maximum connection count is reached
    if (server_manager.get_max_connections() != 0 && server_manager.get_connection_count() >= server_manager.get_max_connections()) {
        lco_return{.operation_code = req.operation_code,
                   .return_code = ErrorCodes::Throttling::MaxCcuReached,
                   .debug_message = std::format("Max CCU of {} reached", server_manager.get_max_connections())};
    }

    // Decide on algorithm based on the presence of the Token parameter
    const bool token_auth = req.parameters.contains(DictKeyCodes::LoadBalancing::Token);

    if (token_auth) {
        // Token mechanism
        const auto params = models::TokenAuth::decode(req);
        if (!params)
            lco_return params.error();

        peer.persistent = load_persistent_peer(server_manager, params->get<DictKeyCodes::LoadBalancing::Token>(), refresh_token);
    } else {
        // Regular mechanism
        const auto params = models::StandardAuth::decode(req);
        if (!params)
            lco_return params.error();

        auto p = create_persistent_peer();

        // Handle app version
        const std::string& app_id = params->get<DictKeyCodes::LoadBalancing::ApplicationId>();
        const std::string *version_ptr = params->get<DictKeyCodes::LoadBalancing::AppVersion>();
        const std::string app_version = version_ptr ? *version_ptr : "(poor attempt at emulating null app version)";

        peer.log->info("Client is using app {} (version {})", app_id, version_ptr ? *version_ptr : "(null)");
        p->app = App::get(server_manager, app_id, app_version);

        // Make sure app is available
        if (!p->app)
            lco_return{.operation_code = req.operation_code, .return_code = ErrorCodes::Auth::InvalidAuthentication, .debug_message = "Invalid app id"};

        // Get app settings
        const auto& app_settings = p->app->get_settings();

        // Make sure to not exceed max peer count of app
        if (app_settings.max_peers && p->app->get_peer_count() >= app_settings.max_peers)
            lco_return{.operation_code = req.operation_code,
                       .return_code = ErrorCodes::Throttling::MaxCcuReached,
                       .debug_message = std::format("Max CCU of {} reached", app_settings.max_peers)};

        // Extract authentication parameters
        const std::string *uid = params->get<DictKeyCodes::LoadBalancing::UserId>();
        const uint8_t auth_type = params->get<DictKeyCodes::AuthAndLobby::ClientAuthenticationType>();

        if (app_settings.auth_mode == AppSettings::AuthMode::Weak || (app_settings.auth_mode == AppSettings::AuthMode::Anonymous && auth_type == 0)) {
            // Weak authentication (all pass) or anonymous authentication (type 0 passes)
            p->user_id = get_anonymous_user_id(app_settings.custom_anonymous_uid_mode, app_settings.anonymous_uid_prefix, uid ? *uid : "");

        } else if (auth_type == 0) {
            // Type 0 (Anonymous) specified or implied, but server strictly requires valid authentication
            lco_return{.operation_code = req.operation_code,
                       .return_code = ErrorCodes::Auth::CustomAuthenticationFailed,
                       .debug_message = "Anonymous authentication is not allowed"};

        } else {
            // Strict mode with custom authentication type. A UserId is strictly mandatory here.
            if (!uid || uid->empty()) {
                lco_return{.operation_code = req.operation_code,
                           .return_code = ErrorCodes::Auth::InvalidAuthentication,
                           .debug_message = "User ID is required for custom authentication"};
            }

            // Check if the specific authentication provider is configured and allowed
            std::optional<AuthProviderSettings> auth_provider_opt;
#ifdef LUXON_SERVER_ENABLE_SETTINGS_DATABASE
            if (auto& settings_manager = server_manager.settings_manager)
                auth_provider_opt = settings_manager->get_auth_provider(app_settings.appid, auth_type);
#endif

            if (!auth_provider_opt || !auth_provider_opt->is_allowed) {
                lco_return{.operation_code = req.operation_code,
                           .return_code = ErrorCodes::Auth::CustomAuthenticationFailed,
                           .debug_message = "Authentication provider is not allowed"};
            }

#ifdef LUXON_SERVER_ENABLE_PLUGINS
            const std::string& auth_params = params->get<DictKeyCodes::AuthAndLobby::ClientAuthenticationParameters>();

            // Fetch the provider URL (or fallback to empty string) to pass to the plugin
            std::string plugin_context = auth_provider_opt->auth_url.value_or("");

            // Attempt to dispatch custom authentication via registered plugins
            if (auto plugin_res = lco_await auth_plugins::registry::call(server_manager, auth_type, *uid, auth_params, plugin_context,
                                                                         auth_provider_opt->secret, auth_provider_opt->auth_url)) {
                if (plugin_res->has_value()) {
                    p->user_id = plugin_res->value();
                } else {
                    auto err_resp = plugin_res->error();
                    err_resp.operation_code = req.operation_code;
                    lco_return err_resp;
                }
            } else {
                lco_return{.operation_code = req.operation_code,
                           .return_code = ErrorCodes::Auth::CustomAuthenticationFailed,
                           .debug_message = "Authentication type not available"};
            }
#else
            lco_return{.operation_code = req.operation_code,
                       .return_code = ErrorCodes::Auth::CustomAuthenticationFailed,
                       .debug_message = "Authentication plugins are disabled"};
#endif
        }

        peer.persistent = std::move(p);
    }

    // Check for success
    if (!peer.persistent) {
        // Handle authentication failure
        lco_return{.operation_code = req.operation_code,
                   .return_code = token_auth ? ErrorCodes::Auth::AuthenticationTokenExpired : ErrorCodes::Auth::InvalidAuthentication,
                   .debug_message = "Authentication failure: Got no persistent peer data"};
    }

    peer.log->info("Client has authenticated as: {}", peer.persistent->user_id);

    ser::OperationResponseMessage resp{.operation_code = req.operation_code};
    if (refresh_token)
        resp.parameters[DictKeyCodes::LoadBalancing::Token] = peer.persistent->token;
    lco_return resp;
}
} // namespace server
