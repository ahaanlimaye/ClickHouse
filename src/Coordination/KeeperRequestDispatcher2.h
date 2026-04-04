#pragma once

#include "config.h"

#if USE_NURAFT

#include <Common/CacheLine.h>
#include <Common/NonblockingBoundedQueue.h>
#include <Coordination/KeeperAppendStream.h>
#include <Coordination/KeeperServer.h>
#include <Interpreters/OpenTelemetrySpanLog.h>

extern template class NonblockingBoundedQueue<DB::KeeperRequestForSession>;
extern template class NonblockingBoundedQueue<DB::KeeperResponseForSession>;

namespace DB
{

//asdqwe consider explaining the series of tubes mental model, with a toy pipeline
///
/// KeeperRequestDispatcher2 is in charge of preventing queue size bloat in the whole pipeline.
/// Here's a story for why no queues or buffers throughout keeper can grow out of control:
///  * Requests from clients arrive in server's TCP sockets (memory usage limited by the OS) and go
///    through some ReadBuffer-s (size limited per client connection), and are parsed by KeeperTCPHandler.
///  * Request is then passed to KeeperRequestDispatcher2, which puts it in requests_queue, which
///    has a limited size (in bytes).
///  * KeeperRequestDispatcher2 then puts requests into batches and sends them on a journey through
///    the whole raft pipeline. It also keeps track of the in-flight requests and makes sure there
///    aren't too many; this is sufficient to ensure no queues or buffers explode anywhere in
///    the whole raft pipeline. So we don't have to carefully choose queue sizes etc everywhere
///    throughout nuraft.
///    (All intermediate messages inside nuraft are small, so there's no situation where our
///     moderate amount of requests translates to disproportionately large memory usage inside nuraft.)
///  * After going through nuraft, newly committed requests end up in KeeperStateMachine<Storage>::commit,
///    which produces responses and passes them to KeeperRequestDispatcher2::onResponse.
///    From onResponse they go to responses_queue, then to KeeperTCPHandler::responses (through responseThread).
///    The total size of responses_queue + all KeeperTCPHandler::responses queues is tracked by
///    KeeperRequestDispatcher2::response_bytes_in_all_queues.
///    If that total size gets too big, onResponse just sleeps, delaying the commit thread
///    (or any other thread that produces responses) and preventing allocation of further responses
///    until queues gets smaller.
///  * Finally, KeeperTCPHandler takes responses from its queue and passes them to WriteBuffer,
///    which passes them to the socket. Again the WriteBuffer memory is limited
///    (per client connection), and socket memory is limited by the OS. And we're done!
///
/// ---
///
/// (KeeperRequestDispatcher2 implementation goes all fancy on avoiding locks and grouping atomics.
///  This is mostly just for fun and for practice; a much sloppier implementation would probably be
///  equally fast because KeeperRequestDispatcher2 should't be the bottleneck.
///  One part where performance matters is commit callback; we shouldn't waste any time there
///  because the commit thread is likely a bottleneck.)
class KeeperRequestDispatcher2
{
public:
    explicit KeeperRequestDispatcher2(KeeperServer * server_);

    /// closed_all_connections is used just for an assert: if true, we expect that all
    /// onResponseDeallocated calls were made, so the tracked response queue size should be zero.
    void shutdown(bool closed_all_connections);

    /// May block for up to operation_timeout_ms if queue is full.
    bool putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id, bool use_xid_64);
    bool putLocalReadRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id);

    /// For every registerSession call there must eventually be a finishSession call
    /// (except during shutdown asdqwe
    void registerSession(int64_t session_id, ZooKeeperResponseCallback callback);
    void finishSession(int64_t session_id);

    /// May block for a short time if queue is full.
    void onResponse(KeeperResponseForSession response) noexcept;

    void onCommit(const KeeperRequestForSession & request_for_session);

    /// KeeperRequestDispatcher2 is in charge of preventing response queue(s) from growing very big and OOMing.
    /// This covers KeeperRequestDispatcher2's own responses_queue, and `responses` queues from all KeeperTCPHandler-s.
    /// (After that the response goes to WriteBuffer and the OS TCP stack, which both have their own size limits.)
    /// Must be called eventually for every ZooKeeperResponseCallback call.
    void onResponseDeallocated(const Coordination::ZooKeeperResponse & response);

private:
    struct InFlightBatch
    {
        /// If false, this is a vacant slot in the in_flight_batches array, not between head_idx and tail_idx.
        std::atomic<bool> active {};

        alignas(CH_CACHE_LINE_SIZE) size_t bytes = 0;
        std::chrono::steady_clock::time_point start_time;
        KeeperRequestsForSessions requests;
        size_t committed_requests = 0;
        /// Element <next_request_idx, read_requests> means that these read_request must be executed
        /// just after request requests[next_request_idx - 1].
        std::vector<std::pair</*next_request_idx*/ size_t, KeeperRequestsForSessions>> reads;
        size_t reads_idx = 0;

        void activate()
        {
            committed_requests = 0;
            reads_idx = 0;
            active.store(true);
        }

        void deactivate()
        {
            requests.clear();
            reads.clear();
            active.store(false);
        }
    };

    struct Session
    {
        ZooKeeperResponseCallback response_callback;
        std::atomic<bool> dead {};

        /// A flag used by request batching to detect dependencies between reads and writes.
        size_t reordering_version = 0;
    };

    KeeperServer * server;
    KeeperContextPtr keeper_context;
    LoggerPtr log;

    ThreadFromGlobalPool dispatch_thread;
    ThreadFromGlobalPool response_thread;

    /// Locked exclusively when adding or removing `sessions` entries.
    SharedMutex sessions_mutex;
    std::unordered_map<int64_t, Session> sessions;

    size_t current_reordering_version = 1;

    std::atomic<bool> shutting_down {};

    /// Lock-free SPSC queue. "Tail" is the enqueue side, "head" is the dequeue side.
    /// Queue size is tail_idx - head_idx.
    /// Queue byte size (sum of InFlightBatch::bytes for batches in the queue) is tail_bytes - head_bytes.
    /// One producer: dispatchThread.
    /// Two consumers, synchronized through request_completion_mutex.
    /// head_idx is incremented after the slot is fully vacated.
    std::vector<InFlightBatch> in_flight_batches;

    std::shared_ptr<KeeperAppendStream> stream;

    /// True if no requests sent through the current `stream` succeeded yet.
    std::atomic<bool> current_stream_is_suspect {};

    //asdqwe add byte limit (or update comment at the top claiming that such limit exists)
    NonblockingBoundedQueue<KeeperRequestForSession> requests_queue;
    std::atomic<size_t> requests_queue_bytes {};

    /// TODO: Currently responses to all requests are formed on all nodes, and go through this queue
    ///       and through responseThread and session id lookup on all nodes, and all except one node
    ///       discard the response. Consider adding server id to the request so that we can tell
    ///       early that response is not needed. It would also save time in
    ///       KeeperRequestDispatcher2::onCommit, we won't have to check most requests against the queue.
    NonblockingBoundedQueue<KeeperResponseForSession> responses_queue;
    std::atomic<size_t> response_bytes_in_all_queues {};

    /// State frequently mutated by dispatch thread.
    alignas(CH_CACHE_LINE_SIZE) std::atomic<size_t> tail_idx {};

    /// State frequently mutated by commit thread.
    alignas(CH_CACHE_LINE_SIZE) std::atomic<size_t> head_idx {};
    /// Locked in commit callback and in dropInFlightRequests.
    /// Normally there's no contention because commit callback is called from one thread, and
    /// dropInFlightRequests is very rare.
    std::mutex request_completion_mutex;

    void dispatchThread();
    void responseThread();

    void popBatch(size_t batch_idx);
    bool tryPopRequest(KeeperRequestForSession & request); // call instead of requests_queue.tryPop

    void dropInFlightRequests();

    void addErrorResponse(const KeeperRequestForSession & request_for_session, Coordination::Error error);
};

}

#endif
