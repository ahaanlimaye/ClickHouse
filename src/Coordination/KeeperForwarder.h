#pragma once

#include "config.h"

#if USE_NURAFT

#include <libnuraft/nuraft.hxx>
#include <Common/logger_useful.h>

#include <functional>
#include <mutex>

namespace DB
{

struct KeeperForwarder
{
    uint64_t connection_idx = 0;

};

/// Manages a single persistent RPC connection to the current Raft leader
/// and forwards client_request messages with the STREAM_FORWARDING wire flag.
///
/// The STREAM_FORWARDING flag provides connection-scoped term binding:
/// the leader captures the term on the first request and rejects (by closing
/// the connection) any request that arrives after the term changes.
/// This guarantees that all entries sent over one connection are committed
/// in order, with no gaps: the committed prefix of the forwarded sequence is
/// contiguous.
///
/// Usage:
///   1. Call getConnectionIdx() to get the current connection generation.
///   2. Call appendEntries() with serialized log entries and the connection_idx.
///      - Returns true if the entries were sent.
///      - Returns false if there's no connection (e.g. reconnection backoff,
///        unknown leader). The caller should retry later.
///   3. When the connection closes (error, term change), close_callback is
///      invoked with the connection_idx. The caller should error-out all
///      in-flight (uncommitted) entries associated with that connection_idx.
///
/// Thread safety: all public methods are synchronized internally.
/// The close_callback may be invoked from an ASIO worker thread;
/// it must not call back into KeeperForwarder.
class KeeperForwarder
{
public:
    /// close_callback is called when a forwarding connection closes.
    /// The argument is the connection_idx of the closed connection.
    /// The callback is invoked at most once per connection.
    explicit KeeperForwarder(
        nuraft::ptr<nuraft::asio_service> asio_service_,
        /// Returns the leader's RPC endpoint string (host:port), or empty
        /// string if the leader is unknown.
        std::function<std::string()> get_leader_endpoint_,
        /// Our server id, used as src in req_msg.
        int32_t my_server_id_,
        std::function<void(uint64_t /*closed_connection_idx*/)> close_callback_);

    ~KeeperForwarder();

    /// Returns the current connection generation index.
    /// Returns 0 if there is no active connection.
    /// After a connection close, this resets to 0 until the next
    /// successful appendEntries creates a new connection.
    uint64_t getConnectionIdx();

    /// Sends log entries to the leader over the forwarding connection.
    /// Creates a new connection if needed (and a leader endpoint is known).
    ///
    /// Returns false if there's no connection and one could not be created
    /// (e.g. leader unknown, reconnection backoff, or asio_service stopped).
    /// In that case, no entries were sent; the caller may retry later.
    ///
    /// Returns true if the entries were handed to the RPC layer for sending.
    /// Note: true does NOT mean the entries were committed — the caller learns
    /// about commits via the Raft commit callback.
    ///
    /// connection_idx must match the current getConnectionIdx().
    /// If it doesn't (stale caller), returns false.
    bool appendEntries(
        const std::vector<nuraft::ptr<nuraft::buffer>> & logs,
        uint64_t connection_idx);

    /// Tear down the current connection (if any) without invoking the
    /// close_callback. Used during shutdown.
    void shutdown();

private:
    /// Must be called under mutex_. Creates a new rpc_client to the leader.
    /// Returns true if the connection was created.
    bool tryConnect();

    /// Called from ASIO threads when an RPC send completes (success or error).
    void onRpcResponse(
        uint64_t connection_idx,
        nuraft::ptr<nuraft::resp_msg> & resp,
        nuraft::ptr<nuraft::rpc_exception> & err);

    /// Close the current connection and invoke close_callback.
    /// Must be called under mutex_. Releases the lock before invoking the callback.
    void closeConnection(std::unique_lock<std::mutex> & lock);

    nuraft::ptr<nuraft::asio_service> asio_service;
    std::function<std::string()> get_leader_endpoint;
    int32_t my_server_id;
    std::function<void(uint64_t)> close_callback;

    mutable std::mutex mutex;
    nuraft::ptr<nuraft::rpc_client> current_client;
    uint64_t current_connection_idx = 0;
    uint64_t next_connection_idx = 1;
    bool is_shutdown = false;

    LoggerPtr log;
};

}

#endif
