#pragma once

#include "config.h"

#if USE_NURAFT

#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <functional>
#include <unordered_set>
#include <Coordination/KeeperServer.h>
#include <Coordination/Keeper4LWInfo.h>
#include <Coordination/KeeperConnectionStats.h>
#include <Coordination/KeeperSnapshotManagerS3.h>
#include <Common/MultiVersion.h>
#include <Common/Macros.h>
#include <Poco/JSON/Object.h>

namespace DB
{

/// Process user requests via consensus and return responses.
class KeeperRequestDispatcher
{
private:
    using RequestsQueue = ConcurrentBoundedQueue<KeeperRequestForSession>;
    using ResponsesQueue = ConcurrentBoundedQueue<KeeperResponseForSession>;
    using SessionToResponseCallback = std::unordered_map<int64_t, ZooKeeperResponseCallback>;

    /// Size depends on coordination settings
    std::unique_ptr<RequestsQueue> requests_queue;
    ResponsesQueue responses_queue;

    mutable std::mutex live_sessions_mutex;
    std::unordered_set<int64_t> live_sessions;

    mutable std::mutex session_to_response_callback_mutex;
    SessionToResponseCallback session_to_response_callback;

    /// Reading and batching new requests from client handlers
    ThreadFromGlobalPool request_thread;
    /// Pushing responses to clients client handlers
    /// using session_id.
    ThreadFromGlobalPool responses_thread;

    KeeperServer * server;

    LoggerPtr log;

    KeeperContextPtr keeper_context;

    std::mutex read_request_queue_mutex;

    /// queue of read requests that can be processed after a request with specific session ID and XID is committed
    std::unordered_map<int64_t, std::unordered_map<Coordination::XID, KeeperRequestsForSessions>> read_request_queue;

    /// Thread put requests to raft
    void requestThread();
    /// Thread put responses for subscribed sessions
    void responseThread();

    /// Returns true if response was successfully sent to client, false if session doesn't exist on this node.
    bool setResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr request = nullptr);

    /// Add error responses for requests to responses queue.
    /// Clears requests.
    void addErrorResponses(const KeeperRequestsForSessions & requests_for_sessions, Coordination::Error error);

    /// Forcefully wait for result and sets errors if something when wrong.
    /// Clears both arguments
    nuraft::ptr<nuraft::buffer> forceWaitAndProcessResult(
        RaftAppendResult & result, KeeperRequestsForSessions & requests_for_sessions, bool clear_requests_on_success);

public:
    explicit KeeperRequestDispatcher(KeeperServer * server_);

    void shutdown();

    /// Put request to ClickHouse Keeper
    bool putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id, bool use_xid_64);

    /// Put local read request to ClickHouse Keeper
    bool putLocalReadRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id);

    /// Register session and subscribe for responses with callback.
    /// The callback must be safe for concurrent invocation — see ZooKeeperResponseCallback.
    void registerSession(int64_t session_id, ZooKeeperResponseCallback callback);

    /// Call if we don't need any responses for this session no more (session was expired)
    void finishSession(int64_t session_id);

    void onResponse(KeeperResponseForSession response) noexcept;
    void onCommit(const KeeperRequestForSession & request_for_session);
};

}

#endif
