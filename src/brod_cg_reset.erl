%%%
%%%   Copyright (c) 2017 Klarna AB
%%%
%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%

%%%=============================================================================
%%% @doc This is a utility module to help reset committed offsets in kafak.
%%% @end
%%%=============================================================================

-module(brod_cg_reset).

-behaviour(gen_server).
-behaviour(brod_group_member).

-export([ run/3
        ]).

-export([ start_link/4
        , stop/1
        , sync/1
        ]).

%% callbacks for brod_group_coordinator
-export([ get_committed_offsets/2
        , assignments_received/4
        , assignments_revoked/1
        , assign_partitions/3
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-include("brod_int.hrl").

-type topic() :: brod:topic().
-type partition() :: brod:partition().
-type member_id() :: brod:group_member_id().
-type offsets() :: [{brod:topic(), [{brod:partition(), brod:offset()}]}].
-type per_group_input() :: {brod:group_id(), Retention :: integer(), offsets()}.

-record(state,
        { client             :: brod:client()
        , groupId            :: brod:group_id()
        , memberId           :: ?undef | member_id()
        , generationId       :: ?undef | brod:group_generation_id()
        , coordinator        :: pid()
        , offsets            :: ?undef | offsets()
        , is_elected = false :: boolean()
        , pending_sync       :: ?undef | {pid(), reference()}
        , is_done = false    :: boolean()
        }).

%%%_* APIs =====================================================================

%% @doc Force commit offsets.
-spec run([brod:endpoint()], brod:client_config(), [per_group_input()]) ->
        ok | no_return().
run(BootstrapHosts, SockOpts, Groups) ->
  ClientId = ?MODULE,
  ok = brod:start_client(BootstrapHosts, ClientId, SockOpts),
  lists:foreach(
    fun({GroupId, Retention, Offsets}) ->
      {ok, Pid} = start_link(ClientId, GroupId, Retention, Offsets),
      ok = sync(Pid),
      ok = stop(Pid)
    end, Groups).

%% @doc Start (link) a group subscriber.
%% Client:
%%   Client ID (or pid, but not recommended) of the brod client.
%% GroupId:
%%   Consumer group ID which should be unique per kafka cluster
%% Offsets:
%%   The desired offsets to set to.
%% @end
-spec start_link(brod:client(), brod:group_id(), integer(), offsets()) ->
        {ok, pid()} | {error, any()}.
start_link(Client, GroupId, Retention, Offsets) ->
  Args = {Client, GroupId, Retention, Offsets},
  gen_server:start_link(?MODULE, Args, []).

%% @doc Stop the process.
-spec stop(pid()) -> ok.
stop(Pid) ->
  Mref = erlang:monitor(process, Pid),
  ok = gen_server:cast(Pid, stop),
  receive
    {'DOWN', Mref, process, Pid, _Reason} ->
      ok
  end.

%% @doc Make a call to the resetter process, the call will be blocked
%% until offset reset is done.
%% @end
-spec sync(pid()) -> ok.
sync(Pid) ->
  gen_server:call(Pid, sync, infinity).

%%%_* APIs for group coordinator ===============================================

%% @doc Called by group coordinator when there is new assignemnt received.
-spec assignments_received(pid(), member_id(), integer(),
                           brod:received_assignments()) -> ok.
assignments_received(Pid, MemberId, GenerationId, TopicAssignments) ->
  gen_server:cast(Pid, {new_assignments, MemberId,
                        GenerationId, TopicAssignments}).

%% @doc Called by group coordinator before re-joinning the consumer group.
-spec assignments_revoked(pid()) -> ok.
assignments_revoked(Pid) ->
  gen_server:call(Pid, unsubscribe_all_partitions, infinity).

%% @doc This function is called only when `partition_assignment_strategy'
%% is set for `callback_implemented' in group config.
%% @end
-spec assign_partitions(pid(), [brod:group_member()],
                        [{brod:topic(), brod:partition()}]) ->
        [{member_id(), [brod:partition_assignment()]}].
assign_partitions(Pid, Members, TopicPartitionList) ->
  Call = {assign_partitions, Members, TopicPartitionList},
  gen_server:call(Pid, Call, infinity).

%% @doc Called by group coordinator when initializing the assignments
%% for subscriber.
%% NOTE: this function is called only when it is DISABLED to commit offsets
%%       to kafka.
%% @end
-spec get_committed_offsets(pid(), [{brod:topic(), brod:partition()}]) ->
        {ok, [{{brod:topic(), brod:partition()}, brod:offset()}]}.
get_committed_offsets(_Pid, _TopicPartitions) ->
  {ok, []}.

%%%_* gen_server callbacks =====================================================

init({Client, GroupId, Retention, Offsets}) ->
  ok = brod_utils:assert_client(Client),
  ok = brod_utils:assert_group_id(GroupId),
  Topics = lists:usort([T || {T, _P} <- Offsets]),
  %% use callback_implemented strategy so I know I am elected leader
  %% when this assign_partitions is called.
  Config = [ {partition_assignment_strategy, callback_implemented}
           , {offset_retention_seconds, Retention}
           ],
  {ok, Pid} = brod_group_coordinator:start_link(Client, GroupId, Topics,
                                                Config, ?MODULE, self()),
  State = #state{ client      = Client
                , groupId     = GroupId
                , coordinator = Pid
                , offsets     = Offsets
                },
  {ok, State}.

handle_info(Info, State) ->
  log(State, info, "discarded message:~p", [Info]),
  {noreply, State}.

handle_call(sync, From, State0) ->
  State1 = State0#state{pending_sync = From},
  State = maybe_reply_sync(State1),
  {noreply, State};
handle_call({assign_partitions, Members, TopicPartitions}, _From,
            #state{coordinator = CoordinatorPid} = State) ->
  log(State, info, "Assigning all topic partitions to self", []),
  Result = assign_all_to_self(CoordinatorPid, Members, TopicPartitions),
  {reply, Result, State#state{is_elected = true}};
handle_call(unsubscribe_all_partitions, _From, #state{} = State) ->
  {reply, ok, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

handle_cast({new_assignments, _MemberId, GenerationId, Assignments},
            #state{ is_elected = IsLeader
                  , offsets = OffsetsToCommit
                  , coordinator = Pid
                  } = State) ->
  %% Assert that I am the leader of the group,
  %% so that other members won't interfere
  case IsLeader of
    true ->
      ok;
    false ->
      log(State, error, "not elected, there are probably "
                        "other active group members in the group", []),
      erlang:exit(not_elected)
  end,
  Groupped =
    brod_utils:group_per_key(
      fun(#brod_received_assignment{ topic        = Topic
                                   , partition    = Partition
                                   , begin_offset = Offset
                                   }) ->
          {Topic, {Partition, Offset}}
      end, Assignments),
  log(State, info, "current offsets:\n~p\n", [Groupped]),
  %% Stage all offsets in coordinator process
  lists:foreach(
    fun({Topic, PartitionOffsetList}) ->
        lists:foreach(
          fun({Partition, Offset}) ->
              brod_group_coordinator:ack(Pid, GenerationId,
                                         Topic, Partition, Offset)
          end, PartitionOffsetList)
    end, OffsetsToCommit),
  %% Now force it to commit
  case brod_group_coordinator:commit_offsets(Pid) of
    ok -> ok;
    {error, Reason} ->
      log(State, error, "failed to commit, reason:\n~p", [Reason]),
      erlang:exit(commit_failed)
  end,
  {noreply, set_done(State)};
handle_cast(stop, State) ->
  {stop, normal, State};
handle_cast(_Cast, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, #state{}) ->
  ok.

%%%_* Internal Functions =======================================================

set_done(State) ->
  maybe_reply_sync(State#state{is_done = true}).

maybe_reply_sync(#state{is_done = false} = State) ->
  State;
maybe_reply_sync(#state{pending_sync = ?undef} = State) ->
  State;
maybe_reply_sync(#state{pending_sync = From} = State) ->
  gen_server:reply(From, ok),
  State#state{pending_sync = ?undef}.

%% @private Usually, people would stop all other members before
%% using `brod_cg_rest' to reset committed offsets.
%% i.e. we should usually have only one member in the list.
%% Nonetheless, here we still make sure other members (if any)
%% do not get anything assigned. so they hopefully won't commit anything
%% during or right after we reset offsets.
%% @end
-spec assign_all_to_self(pid(), [brod:group_member()],
                         [{topic(), partition()}]) ->
        [{member_id(), [brod:partition_assignment()]}].
assign_all_to_self(CoordinatorPid, Members, TopicPartitions) ->
  MyMemberId = find_leader(CoordinatorPid, Members),
  Groupped = brod_utils:group_per_key(TopicPartitions),
  [{MyMemberId, Groupped}].

%% @private Leader's member ID is kept in `brod_group_coordinator' looping
%% state, but not exposed to callback module. Here we make use of the leader's
%% pid in `user_data' to locate it.
%% @end
find_leader(CoordinatorPid, [H | T]) ->
  {MemberId, #kafka_group_member_metadata{user_data = UD}} = H,
  try
    L = binary_to_term(UD),
    {_, CoordinatorPid} = lists:keyfind(<<"member_coordinator">>, 1, L),
    MemberId
  catch
    _ : _ ->
      find_leader(CoordinatorPid, T)
  end.

log(#state{groupId  = GroupId}, Level, Fmt, Args) ->
  brod_utils:log(Level,
                 "Group member (~s,coor=~p):\n" ++ Fmt,
                 [self(), GroupId | Args]).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
