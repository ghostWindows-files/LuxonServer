// Copyright (c) 2026, the Luxon Server contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "offset_map.hpp"

#include <vector>
#include <functional>
#include <unordered_map>

#if defined(__linux__)
#include <unistd.h>
#include <sys/epoll.h>
#elif defined(_WIN32)
#include "wepoll.h"
#else
#include <sys/select.h>
#endif

namespace server {
class SockSelector {
public:
#ifdef _WIN32
    using socket_t = SOCKET;
    using epoll_t = HANDLE;
    template <typename V> using callback_map_t = std::unordered_map<SOCKET, V>;
#else
    using socket_t = int;
    using epoll_t = int;
    template <typename V> using callback_map_t = offset_map<unsigned, V>;
#endif

    using callback_t = std::function<void(socket_t)>;

private:
#if defined(__linux__) || defined(_WIN32)
    epoll_t epoll_fd;
    std::vector<struct epoll_event> events;
#else
    fd_set read_fd_set;
    std::vector<socket_t> read_fds;
#ifndef _WIN32
    socket_t highest_fd;
#else
    constexpr static socket_t highest_fd = 0;
#endif
#endif

    callback_map_t<callback_t> callbacks;

public:
    SockSelector();
    ~SockSelector();

    // Zero-timeout means potentially forever-blocking
    bool run(unsigned timeout_ms = 0);

    bool add_read_fd(socket_t fd, callback_t&& callback);
    void remove_read_fd(socket_t fd);
};
} // namespace server
