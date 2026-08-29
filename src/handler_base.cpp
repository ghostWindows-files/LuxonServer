// Copyright (c) 2026, the Luxon Server contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "handler_base.hpp"
#include "global.hpp"
#include "peer_persistence.hpp"
#include "hookpoints.hpp"
#include "server_manager.hpp"

#include <string_view>
#include <format>
#include <charconv>
#include <commoncpp/utils.hpp>
#include <luxon/ser_interface.hpp>
#include <luxon/http_parser.hpp>
#include <luxon/visualizer.hpp>
#include <luxon/internal_codes.hpp>
#include <luxon/common_codes.hpp>
#include <magic_enum/magic_enum.hpp>
#include <tracy/Tracy.hpp>

namespace server {
namespace {
std::optional<int> fast_stoi(std::string_view sv, int base = 10) {
    int value = 0;

    // from_chars takes pointers: [data, data + size]
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), value, base);

    // Check for errors
    if (ec == std::errc::invalid_argument) {
        // No digits were found
        return std::nullopt;
    } else if (ec == std::errc::result_out_of_range) {
        // Value is too large or too small for an int
        return std::nullopt;
    }

    // Check that the entire string was consumed
    if (ptr != sv.data() + sv.size())
        return std::nullopt;

    return value;
}
} // namespace

HandlerBase::~HandlerBase() {
    if (peer_->persistent) {
        sync_persistent_peer(server_manager_, *peer_->persistent);
        store_persistent_peer(server_manager_, std::move(peer_->persistent));
    }
}

Awaitable<> HandlerBase::HandleConnect() {
    peer_->log->info("Client connected!");
    lco_return;
}
Awaitable<> HandlerBase::HandleDisconnect() {
    peer_->log->info("Client disconnected!");
    lco_return;
}

void HandlerBase::HandleSlowUpdate() {}

Awaitable<> HandlerBase::HandleENetConnectionStateChange(enet::EnetConnectionState state) {
    std::string_view state_name;
    switch (state) {
    case enet::EnetConnectionState::Disconnected:
        state_name = "disconnected";
        break;
    case enet::EnetConnectionState::Connecting:
        state_name = "connecting";
        break;
    case enet::EnetConnectionState::Connected:
        state_name = "connected";
        break;
    case enet::EnetConnectionState::Disconnecting:
        state_name = "disconnecting";
        break;
    case enet::EnetConnectionState::Stale:
        state_name = "stale";
        break;
    default:
        state_name = "in unknown state";
    }

    peer_->log->info("Client is now {}", state_name);

    lco_return;
}

Awaitable<> HandlerBase::HandleENetCommand(enet::EnetCommand&& cmd) {
    ZoneScoped;

    const auto payload = cmd.get_payload();

    // Try to parse header
    std::expected<ser::Message, ser::Error> expected_message;
    {
        ZoneScopedN("DeserializeENetCommand");
        expected_message = proto_->Deserialize(payload);
    }
    if (!expected_message) {
        // Try to parse as HTTP request
        if (auto expected_request = luxon::parse_raw_http(std::string_view{reinterpret_cast<const char *>(payload.data()), payload.size()})) {
            lco_await HandleHTTPRequest(std::move(*expected_request), cmd.header);
        } else {
            // We don't know what this is!
            peer_->log->warn("Invalid packet ({} bytes in length) received: {}", payload.size(), expected_message.error().message);
#ifdef LUXON_SERVER_ENABLE_VISUALIZER
            luxon::visualizer::helpers::print_hex_dump(payload, 2);
#endif
        }

        lco_return;
    }

    ser::Message& message = *expected_message;
    cmd.reset_payload();

    LUXON_SERVER_HOOKPOINT(HandlerBase_HandleENetCommand_OnMessage, message, cmd.header);

    if (auto *req = message.get_if<ser::InitMessage>())
        lco_return lco_await HandleInitRequest(std::move(*req), cmd.header);

    if (auto *req = message.get_if<ser::OperationRequestMessage>())
        lco_return lco_await HandleOperationRequest(std::move(*req), message.encrypted, cmd.header);

    if (auto *req = message.get_if<ser::InternalOperationRequestMessage>())
        lco_return lco_await HandleInternalOperationRequest(std::move(*req), message.encrypted, cmd.header);

    peer_->log->warn("Invalid message type {} received", message.index());

    lco_return;
}

Awaitable<> HandlerBase::HandleHTTPRequest(HttpRequest&& request, const enet::EnetCommandHeader& cmd_header) {
    ZoneScoped;

    // Check if init request
    if (request.path == "/" && request.method == "POST" && request.query_params.contains("init")) {
        // Translate to fake init request
        {
            // Handle init request by first synthesizing photon init request from it
            ser::InitMessage photon_req{};
            if (request.query_params.contains("app"))
                photon_req.app_id = request.query_params.at("app");
            if (request.query_params.contains("clientversion")) {
                // Parse client version
                const auto version_numbers = common::utils::str_split(request.query_params.at("clientversion"), '.');
                if (version_numbers.size() == 4) {
                    photon_req.version_major = fast_stoi(version_numbers[0]).value_or(0);
                    photon_req.version_minor = fast_stoi(version_numbers[1]).value_or(0);
                    photon_req.version_revision = fast_stoi(version_numbers[2]).value_or(0);
                    photon_req.version_patch = fast_stoi(version_numbers[3]).value_or(0);
                }
            }
            if (request.query_params.contains("protocol")) {
                // Parse protocol version
                constexpr std::string_view prefix = "GpBinaryV";
                const std::string& protocol_string = request.query_params.at("protocol");
                if (protocol_string.starts_with(prefix)) {
                    // Remove prefix
                    const std::string_view binary_version = std::string_view(protocol_string).substr(prefix.size());
                    if (binary_version.size() == 2) {
                        photon_req.protocol_major = binary_version[0] - '0';
                        photon_req.protocol_minor = binary_version[1] - '0';
                    }
                }
            }

            // Pass the synthesized init request to our handler
            lco_await HandleInitRequest(std::move(photon_req), cmd_header);
        }

        // Translate to fake authenticate operation request
        if (request.body.size() > 4) {
            std::string token = request.body;
            token.erase(0, 2);

            ser::OperationRequestMessage photon_req{.operation_code = OpCodes::Auth::AuthenticateOnce};
            photon_req.parameters[DictKeyCodes::LoadBalancing::Token] = token;
            lco_await HandleOperationRequest(std::move(photon_req), false, cmd_header);
        }
    } else {
        // We don't know what this HTTP request is!
        peer_->log->warn("Invalid HTTP request received");
#ifdef LUXON_SERVER_ENABLE_VISUALIZER
        luxon::visualizer::print_http_message(request, 2);
#endif
    }
}

Awaitable<> HandlerBase::HandleInitRequest(ser::InitMessage&& req, const enet::EnetCommandHeader& cmd_header) {
    ZoneScoped;

    // Try to create new protocol implementation for given version
    auto protocol = ser::IProtocol::make(req.protocol_major, req.protocol_minor);

    // Answer init request
    if (protocol) {
        proto_ = std::move(protocol);
        send(proto_->Serialize(ser::InitResponseMessage{}), enet::EnetSendOptions{cmd_header.channel_id});
        peer_->log->info("Connection init complete");
    } else {
        peer_->log->error("Connection init failed: Protocol mismatch");
        peer_->disconnect();
    }

    lco_return;
}

Awaitable<> HandlerBase::HandleOperationRequest(ser::OperationRequestMessage&& req, bool is_encrypted, const enet::EnetCommandHeader& cmd_header) {
    ZoneScoped;

    // Only answer unknown operations on channel 0
    if (cmd_header.channel_id != 0)
        lco_return;

    // Handle authentication requests that are coming through despite peer already being authenticated
    if (req.operation_code == OpCodes::Auth::Authenticate && peer_->is_authenticated()) {
        const ser::OperationResponseMessage resp{
            .operation_code = req.operation_code, .return_code = ErrorCodes::Core::OperationNotAllowedInCurrentState, .debug_message = "Already authenticated"};
        send(proto_->Serialize(resp));
        lco_return;
    }

    const ser::OperationResponseMessage resp{.operation_code = req.operation_code,
                                             .return_code = ErrorCodes::Core::OperationInvalid,
                                             .debug_message = std::format("Unsupported operation {}", req.operation_code)};
    send(proto_->Serialize(resp));
    peer_->log->warn("Client sent operation request with unknown opcode: {}", req.operation_code);
}

Awaitable<> HandlerBase::HandleInternalOperationRequest(ser::InternalOperationRequestMessage&& req, bool is_encrypted,
                                                        const enet::EnetCommandHeader& cmd_header) {
    ZoneScoped;

    if (cmd_header.channel_id != 0)
        lco_return;

    if (req.operation_code == ICodes::IOpInitEncryption) {
        ZoneScopedN("HandleInternalOperationRequest_IOpInitEncryption");

        // Answer crypto handshake
        auto expected_response = proto_->HandleInitEncryptionRequest(req);
        if (!expected_response) {
            peer_->log->error("Failed to establish encryption: {}", expected_response.error().message);
            lco_return;
        }
        send(proto_->Serialize(*expected_response));

        peer_->log->info("Established encryption");
    } else if (req.operation_code == ICodes::IOpPing) {
        ZoneScopedN("HandleInternalOperationRequest_IOpPing");

        // Answer internal pings
        ser::InternalOperationResponseMessage resp;
        resp.operation_code = ICodes::IOpPing;
        resp.return_code = ErrorCodes::Core::Ok;

        resp.parameters[ICodes::IKeyClientTimestamp] = std::move(req.parameters[ICodes::IKeyClientTimestamp]);
        resp.parameters[ICodes::IKeyServerTimestamp] = static_cast<int32_t>(peer_->enet_peer->get_server_time());

        send(proto_->Serialize(resp));
    } else if (req.operation_code == ICodes::IOpTransportProtocol) {
        ZoneScopedN("HandleInternalOperationRequest_IOpTransportProtocol");

        // Quietly process transport protocol tell
        req.parameters[ICodes::IKeyTransportProtocol].store_if(reinterpret_cast<uint8_t&>(peer_->transport_protocol));
        peer_->log->info("Got informed about transport protocol: {}", magic_enum::enum_name(peer_->transport_protocol));
    } else {
        // Answer unknown operation
        const ser::OperationResponseMessage resp{.operation_code = req.operation_code,
                                                 .return_code = ErrorCodes::Core::OperationInvalid,
                                                 .debug_message = std::format("Unsupported internal operation {}", req.operation_code)};
        send(proto_->Serialize(resp));
        peer_->log->warn("Client sent internal operation request with unknown opcode: {}", req.operation_code);
    }
}

void HandlerBase::send(const ser::ByteArray& payload, const enet::EnetSendOptions& opt) { peer_->send(payload, opt); }

void HandlerBase::send(const std::expected<ser::ByteArray, ser::Error>& expected_payload, const enet::EnetSendOptions& opt) {
    if (!expected_payload)
        peer_->log->error("Failed to serialize data: {}", expected_payload.error().message);
    else
        send(*expected_payload, opt);
}
} // namespace server
