#include <Coordination/KeeperForwarder.h>

#include "config.h"

#if USE_NURAFT

#include <libnuraft/log_val_type.hxx>
#include <libnuraft/msg_type.hxx>
#include <libnuraft/req_msg.hxx>

namespace DB
{

KeeperForwarder::KeeperForwarder(
    nuraft::ptr<nuraft::asio_service> asio_service_,
    std::function<std::string()> get_leader_endpoint_,
    int32_t my_server_id_,
    std::function<void(uint64_t)> close_callback_)
    : asio_service(std::move(asio_service_))
    , get_leader_endpoint(std::move(get_leader_endpoint_))
    , my_server_id(my_server_id_)
    , close_callback(std::move(close_callback_))
    , log(getLogger("KeeperForwarder"))
{
}

KeeperForwarder::~KeeperForwarder()
{
    shutdown();
}

uint64_t KeeperForwarder::getConnectionIdx()
{
    std::lock_guard lock(mutex);
    return current_connection_idx;
}

bool KeeperForwarder::appendEntries(
    const std::vector<nuraft::ptr<nuraft::buffer>> & logs,
    uint64_t connection_idx)
{
    if (logs.empty())
        return true;

    std::unique_lock lock(mutex);

    if (is_shutdown)
        return false;

    /// No active connection — try to create one.
    if (!current_client)
    {
        if (!tryConnect())
            return false;
    }

    /// Caller has a stale connection_idx (from before a reconnect).
    if (connection_idx != current_connection_idx)
        return false;

    /// Build a client_request req_msg, same structure as raft_server::append_entries_ext.
    nuraft::ptr<nuraft::req_msg> req = nuraft::cs_new<nuraft::req_msg>(
        static_cast<nuraft::ulong>(0),
        nuraft::msg_type::client_request,
        my_server_id,
        0, /// dst: 0—server will fill it in
        static_cast<nuraft::ulong>(0),
        static_cast<nuraft::ulong>(0),
        static_cast<nuraft::ulong>(0));

    req->set_extra_flags(nuraft::req_msg::STREAM_FORWARDING_REQUEST);

    for (const auto & buf : logs)
    {
        buf->pos(0);
        nuraft::ptr<nuraft::log_entry> entry =
            nuraft::cs_new<nuraft::log_entry>(0, buf, nuraft::log_val_type::app_log);
        req->log_entries().push_back(entry);
    }

    uint64_t conn_idx = current_connection_idx;
    nuraft::ptr<nuraft::rpc_client> client = current_client;

    /// Release the lock before sending — the ASIO send may call back synchronously
    /// in some edge cases.
    lock.unlock();

    nuraft::rpc_handler handler =
        [this, conn_idx](
            nuraft::ptr<nuraft::resp_msg> & resp,
            nuraft::ptr<nuraft::rpc_exception> & err)
        {
            onRpcResponse(conn_idx, resp, err);
        };

    client->send(req, handler);
    return true;
}

void KeeperForwarder::shutdown()
{
    std::unique_lock lock(mutex);
    if (is_shutdown)
        return;
    is_shutdown = true;
    current_client.reset();
    current_connection_idx = 0;
}

bool KeeperForwarder::tryConnect()
{
    /// Caller must hold mutex_.
    std::string endpoint = get_leader_endpoint();
    if (endpoint.empty())
    {
        LOG_TRACE(log, "No leader endpoint known, cannot connect");
        return false;
    }

    LOG_INFO(log, "Connecting to leader at {}", endpoint);

    nuraft::ptr<nuraft::rpc_client> client = asio_service->create_client(endpoint);
    if (!client)
    {
        LOG_WARNING(log, "Failed to create RPC client to {}", endpoint);
        return false;
    }

    current_client = std::move(client);
    current_connection_idx = next_connection_idx++;
    LOG_INFO(log, "Forwarding connection {} established to {}", current_connection_idx, endpoint);
    return true;
}

void KeeperForwarder::onRpcResponse(
    uint64_t connection_idx,
    nuraft::ptr<nuraft::resp_msg> & resp,
    nuraft::ptr<nuraft::rpc_exception> & err)
{
    /// We don't care about successful responses — the commit callback is how
    /// the caller learns about committed entries. We only care about errors.
    /// An error means the connection is broken or the leader rejected us.
    ///
    /// The STREAM_FORWARDING server-side behavior is:
    ///   - On non-accepted response → close connection (no response sent).
    ///   - So from the client side, we see a read-error / abandoned client.
    ///
    /// We also get errors for plain network issues (timeout, disconnect).

    if (!err && resp)
    {
        /// Successful send. In STREAM_FORWARDING mode the server doesn't send
        /// meaningful responses (it may not send any at all for rejected requests),
        /// so just ignore.
        return;
    }

    std::unique_lock lock(mutex);

    /// Stale callback from a previous connection — ignore.
    if (connection_idx != current_connection_idx)
        return;

    if (err)
        LOG_WARNING(log, "Forwarding connection {} error: {}", connection_idx, err->what());
    else
        LOG_WARNING(log, "Forwarding connection {} received null response", connection_idx);

    closeConnection(lock);
}

void KeeperForwarder::closeConnection(std::unique_lock<std::mutex> & lock)
{
    /// Caller must hold mutex_.
    uint64_t closed_idx = current_connection_idx;
    current_client.reset();
    current_connection_idx = 0;

    /// Release lock before invoking user callback to avoid deadlock.
    lock.unlock();

    if (close_callback)
    {
        LOG_INFO(log, "Invoking close callback for connection {}", closed_idx);
        close_callback(closed_idx);
    }
}

}

#endif
