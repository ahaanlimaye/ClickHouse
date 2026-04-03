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

class KeeperDispatcher2
{
public:
    KeeperDispatcher2(...);
    void enqueueRequests(KeeperRequestsForSessions);

private:
    struct InFlightBatch
    {
        /// If false, this is a vacant slot in the in_flight_batches array, not between head_idx and tail_idx.
        std::atomic<bool> active {};

        alignas(CH_CACHE_LINE_SIZE) size_t bytes = 0;
        std::chrono::steady_clock::time_point start_time;
        KeeperRequestsForSessions requests;
        size_t committed_requests = 0;
        std::vector<std::pair</*request_idx*/ size_t, KeeperRequestsForSessions>> reads;
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
    };

    SharedMutex sessions_mutex;
    std::unordered_map<int64_t, Session> sessions;

    NonblockingBoundedQueue<KeeperRequestForSession> requests_queue;
    /// TODO: Currently responses to all requests are formed on all nodes, and go through this queue
    ///       and through responseThread and session id lookup on all nodes, and all except one node
    ///       discard the response. Consider adding server id to the request so that we can tell
    ///       early that response is not needed. It would also save time in
    ///       KeeperDispatcher2::onCommit, we won't have to check most requests against the queue.
    NonblockingBoundedQueue<KeeperResponseForSession> responses_queue;

    std::atomic<bool> shutdown {};

    /// Lock-free SPSC queue. "Tail" is the enqueue side, "head" is the dequeue side.
    /// Queue size is tail_idx - head_idx.
    /// Queue byte size (sum of InFlightBatch::bytes for batches in the queue) is tail_bytes - head_bytes.
    /// (Actually there are two consumers. They synchronize through request_completion_mutex.)
    std::vector<InFlightBatch> in_flight_batches;
    alignas(CH_CACHE_LINE_SIZE) std::atomic<size_t> head_idx {};
    //asdqwe subrequest counters
    std::atomic<size_t> head_bytes {};
    alignas(CH_CACHE_LINE_SIZE) std::atomic<size_t> tail_idx {};
    std::atomic<size_t> tail_bytes {};

    /// Locked in commit callback and in dropInFlightRequests.
    /// Normally there's no contention because commit callback is called from one thread, and
    /// dropInFlightRequests is very rare.
    alignas(CH_CACHE_LINE_SIZE) std::mutex request_completion_mutex;

    alignas(CH_CACHE_LINE_SIZE) std::shared_ptr<KeeperAppendStream> stream;
    /// True if no requests sent through the current `stream` succeeded yet.
    alignas(CH_CACHE_LINE_SIZE) std::atomic<bool> current_stream_is_suspect {};

    void dispatchThread();
    void responseThread();

    void onResponse(KeeperResponseForSession response);
    void onCommit(const KeeperRequestForSession & request_for_session);

    void popBatch(size_t batch_idx);
    void dropInFlightRequests();
};

}

#endif
