#include <Coordination/KeeperAppendStream.h>

#if USE_NURAFT

#include <Coordination/KeeperServer.h>
#include <Coordination/CoordinationSettings.h>

#include <libnuraft/log_val_type.hxx>
#include <libnuraft/msg_type.hxx>
#include <libnuraft/req_msg.hxx>

namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsMilliseconds operation_timeout_ms;
}

KeeperAppendStream::KeeperAppendStream(KeeperServer * server_) : server(server_) {}

std::future<bool> KeeperAppendStream::putRequestBatch(const KeeperRequestsForSessions & requests_for_sessions)
{
    std::promise<bool> promise;
    auto future = promise.get_future();

    if (isBroken())
    {
        promise.set_value(false);
        return future;
    }

    auto fail = [&]() -> std::future<bool>
    {
        is_broken->store(true);
        promise.set_value(false);
        return std::move(future);
    };

    if (!client && !term)
    {
        int32_t leader_id = server->raft_instance->get_leader();
        if (leader_id == -1)
            return fail();
        int32_t my_id = server->raft_instance->get_id();

        if (leader_id == my_id)
        {
            term = server->raft_instance->get_term();
        }
        else
        {
            auto c_conf = server->raft_instance->get_config();
            auto srv_conf = c_conf->get_server(leader_id);
            client = server->asio_service->create_client(srv_conf->get_endpoint());
            if (!client)
                return fail();
        }
    }

    std::vector<nuraft::ptr<nuraft::buffer>> entries;
    entries.reserve(requests_for_sessions.size());
    for (const auto & request_for_session : requests_for_sessions)
        entries.push_back(IKeeperStateMachine::getZooKeeperLogEntry(request_for_session));

    if (term)
    {
        nuraft::raft_server::req_ext_params params;
        params.expected_term_ = term.value();
        auto res = server->raft_instance->append_entries_ext(entries, params);
        if (!res || !res->get_accepted())
            return fail();
        /// We pretend that this is async, but actually `res` is always ready here and doesn't need
        /// a callback, when async replication is enabled.
        res->when_ready([promise = std::move(promise), is_broken = is_broken](nuraft::ptr<nuraft::buffer> &, nuraft::ptr<std::exception> & err) mutable
        {
            if (err != nullptr)
                is_broken->store(true);
            promise.set_value(err == nullptr);
        });
    }
    else
    {
        client->send(
            req,
            [promise = std::move(promise), is_broken = is_broken](nuraft::ptr<nuraft::resp_msg> &, nuraft::ptr<nuraft::rpc_exception> & err) mutable
            {
                if (err != nullptr)
                    is_broken->store(true);
                promise.set_value(err == nullptr);
            },
            server->keeper_context->getCoordinationSettings()[CoordinationSetting::operation_timeout_ms].totalMilliseconds());
    }

    return future;
}

}

#endif
