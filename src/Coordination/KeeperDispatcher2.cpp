#include <Coordination/KeeperDispatcher2.h>

#if USE_NURAFT

template class NonblockingBoundedQueue<DB::KeeperRequestForSession>;
template class NonblockingBoundedQueue<DB::KeeperResponseForSession>;

namespace DB
{

void KeeperDispatcher2::dispatchThread()
{
    auto last_stuck_check_time = std::chrono::steady_clock::now();
    while (!shutdown.load())
    {
        auto now = std::chrono::steady_clock::now();

        /// If stream is broken, drop in-flight requests.
        if (stream && stream->isBroken())
        {
            /// After we lost connection to leader, we want to sleep for multiple reasons:
            ///  1. If there are requests in flight, wait in hopes that they get committed.
            ///     (E.g. during graceful leader migration.)
            ///     After the sleep we'll have to fail the remaining in-flight requests and
            ///     close their client sessions.
            ///  2. If there's no healthy leader, we can't do much and can as well wait for
            ///     leader election to complete before proceeding. This may also give more
            ///     chance for in-flight requests to get committed and removed from in_flight_batches.
            ///  3. If there's no healthy leader, we don't want to spam reconnects very quickly.
            auto sleep_start = now;
            while (true)
            {
                auto slept = std::chrono::steady_clock::now() - sleep_start;
                if (slept >= std::chrono::milliseconds(operation_timeout_ms) || shutdown.load())
                    break;
                if (server->isLeaderAlive() &&
                    (!current_stream_is_suspect.load() || slept >= ... retry delay) &&
                    (head_idx.load() == tail_idx.load() || slept >= ... in-flight wait delay))
                    break;
                std::this_thread::sleep_for(... 100ms);
            }

            dropInFlightRequests();
            stream.reset();
            continue;
        }

        if (!stream)
        {
            current_stream_is_suspect.store(true);
            stream = std::make_shared<KeeperAppendStream>(...);
        }

        /// Periodically check that we don't have stuck requests.
        /// In particular, we can get stuck if there's a bug that breaks stream guarantees
        /// (causes reordering or gaps) as our commit callback expects to see all requests in
        /// correct order.
        if (now > last_stuck_check_time + std::chrono::milliseconds(operation_timeout_ms))
        {
            last_stuck_check_time = now;
            size_t idx = head_idx.load();
            if (tail_idx.load() > idx && now > in_flight_batches[idx % in_flight_batches.size()].start_time + std::chrono::milliseconds(operation_timeout_ms))
            {
                if (server->isLeaderAlive())
                    LOG_ERROR(log, "Detected stuck or reordered requests. Dropping. This may indicate a bug.");
                stream->markAsBroken();
                continue;
            }
        }

        /// Check that we don't have too many running requests already.
        size_t batch_idx = tail_idx.load();
        if (batch_idx - head_idx.load() >= in_flight_batches.size() || tail_bytes.load() - head_bytes.load() >= ... in-flight byte limit)
        {
            /// Too many batches in flight. Busy-wait.
            ///
            /// Busy-wait is acceptable because there are very few dispatcher threads
            /// (currently one), and keeper server typically uses much fewer cpu cores than
            /// the machine has.
            ///
            /// Busy-wait is actually good because otherwise we'd have to do a futex wake
            /// syscall (e.g. through condition_variable::notify) from the commit thread.
            /// That would be bad because the commit thread is often the bottleneck of
            /// the entire keeper service, and futex wake is sometimes slow.
            ///
            /// This sleep needs to be shorter than the time it takes to drain the whole
            /// in_flight_batches queue, otherwise we'll be sitting idle after the queue went
            /// from full to empty during one sleep.
            ///
            /// (Maybe we have to pace requests more carefully here.
            ///  I'm not sure what's the best way to think about this. Here's one:
            ///
            ///  Consider a simplified toy system where each request has to go through
            ///  a sequence of 5 stages each having maximum throughput of 10 requests/s
            ///  (e.g. limited by single-core cpu speed), connected by large queues, and
            ///  requests take 1 second to move between stages (e.g. network latency).
            ///  Total latency is 5 seconds.
            ///  Maximum total throughput is 10 requests/s, achieved if every stage has
            ///  nonempty input queue at all times.
            ///  Suppose we are careful about pacing: we start 1 new request every 100ms.
            ///  This gives us maximum throughput, with 50 requests in flight at all times.
            ///  Suppose we are less careful and start requests asap, with a limit of
            ///  100 in-flight requests. We send the first 100 requests at once.
            ///  5 seconds later we start getting one request completion every 100ms,
            ///  starting one new request every 100ms. And I guess it just keeps going like
            ///  that, smooth 1 request every 100ms, full throughput. So we're fine, and
            ///  careful pacing is not needed?
            ///  Or maybe there are conditions where this breaks and we instead converge to
            ///  issuing bursts of 100 requests every 5 seconds, with corresponding
            ///  throughput of 20 requests/s?)
            std::this_thread::sleep_for(... 100us);
            continue;
        }

        /// Check that we have any requests to execute.
        KeeperRequestForSession request;
        if (!requests_queue.tryPop(request))
        {
            /// No requests to process. Busy-wait here too.
            /// TODO: Perhaps we should replace this with a futex wait to improve throughput on
            ///       latency-bound workloads. E.g. one client doing blocking requests in a loop.
            std::this_thread::sleep_for(... 100us);
            continue;
        }

        /// Pick a batch of requests.

        KeeperRequestsForSessions requests;
        std::vector<std::pair<size_t, KeeperRequestsForSessions>> reads;
        ... form batch from request and queue, reorder reads (explain that we could go further and replace one queue with per-session queues and two priority queues, but that is hopefully not needed), send first read through raft to attach others to it, exec here if can;

        /// Add information about the batch to the queue of in-flight requests.

        auto & batch = in_flight_batches[batch_idx % in_flight_batches.size()];
        batch.bytes = ... calc;
        batch.start_time = std::chrono::steady_clock::now();
        batch.requests = std::move(requests);
        batch.reads = std::move(reads);
        batch.activate();

        tail_bytes.fetch_add(batch.bytes);
        tail_idx.store(batch_idx + 1);

        /// Finally send the requests to leader.

        stream->putRequestBatch(batch.requests);
    }
}

void KeeperDispatcher2::popBatch(size_t batch_idx)
{
    auto & batch = in_flight_batches[batch_idx % in_flight_batches.size()];
    size_t bytes = batch.bytes;
    batch.deactivate();
    head_idx.store(batch_idx + 1);
    head_bytes.fetch_add(bytes);
}

void KeeperDispatcher2::dropInFlightRequests()
{
    std::lock_guard stream_lock(request_completion_mutex);
    while (head_idx.load() < tail_idx.load())
    {
        size_t batch_idx = head_idx.load();
        auto & batch = in_flight_batches[batch_idx % in_flight_batches.size()];
        batch.requests.erase(batch.requests.begin(), batch.requests.begin() + batch.committed_requests);
        ... error out batch.requests;
        for (size_t i = batch.reads_idx; i < batch.reads.size(); ++i)
        {
            ... error out batch.reads[i].second;
        }
        popBatch(batch_idx);
    }
}

void KeeperDispatcher2::onCommit(const KeeperRequestForSession & request_for_session)
{
    std::lock_guard stream_lock(request_completion_mutex);

    auto is_same_request = [&](size_t batch_idx, size_t request_idx)
    {
        auto & batch = in_flight_batches[batch_idx % in_flight_batches.size()];
        if (!batch.active.load())
            return false;
        const auto & req = batch.requests.at(request_idx);
        return req.session_id == request_for_session.session_id &&
                req.request->xid == request_for_session.request->xid;
    };

    /// We expect requests to be committed in order with no gaps.
    /// So we only have to check if the newly committed request is the first one in our queue.
    size_t batch_idx = head_idx.load();
    auto & batch = in_flight_batches[batch_idx % in_flight_batches.size()];
    if (!batch.active.load())
        return; // no in-flight batches
    const auto & req = batch.requests.at(batch.committed_requests);
    if (req.session_id != request_for_session.session_id ||
        req.request->xid != request_for_session.request->xid)
        return;

    current_stream_is_suspect.store(false); // a request succeeded, the stream is working

    if (batch.reads_idx < batch.reads.size() && batch.reads[batch.reads_idx].first == batch.committed_requests)
    {
        auto reads = std::move(batch.reads[batch.reads_idx].second);
        batch.reads_idx += 1;
        ... execute reads;
    }

    batch.committed_requests += 1;

    if (batch.committed_requests == batch.requests.size())
        popBatch(batch_idx);
}

}

#endif
