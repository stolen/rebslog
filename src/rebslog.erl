-module(rebslog).

-export([start/1, start_link/1, stop/1]).
-export([record/3, record_file/2, replay/3, replay_file/2, finish/1]).

-record(rebslog_state, {
        encoder = undefined         :: fun((binary(), in|out) -> {binary(), fun()}),
        device = undefined          :: file:io_device(),
        closer = fun(_) -> ok end   :: fun((file:io_device()) -> ok),
        acceptor = undefined        :: pid(),
        default_direction = out     :: in | out
    }).

% Utility to convert any term to atom
atomize(Term) ->
    Printed = io_lib:print(Term),
    Flattened = lists:flatten(Printed),
    erlang:list_to_atom(Flattened).

% Startup functions
start(NoID) when NoID == undefined; NoID == none ->
    {ok, spawn(fun idle_loop/0)};
start(ID) ->
    RegName = atomize(ID),
    undefined = whereis(RegName),
    Pid = spawn(fun idle_loop/0),
    register(RegName, Pid),
    {ok, Pid}.

start_link(NoID) when NoID == undefined; NoID == none ->
    {ok, spawn_link(fun idle_loop/0)};
start_link(ID) ->
    RegName = atomize(ID),
    undefined = whereis(RegName),
    Pid = spawn_link(fun idle_loop/0),
    register(RegName, Pid),
    {ok, Pid}.


% Idle loop
idle_loop() ->
    idle_loop(#rebslog_state{}).
idle_loop(State) ->
    receive
        {record, Device, Closer} ->
            ok = (State#rebslog_state.closer)(State#rebslog_state.device),
            record_loop(State#rebslog_state{
                    encoder = rebs_codec:encoder(),
                    device = Device,
                    closer = Closer});
        {open_record, Opener, Closer} ->
            ok = (State#rebslog_state.closer)(State#rebslog_state.device),
            record_loop(State#rebslog_state{
                    encoder = rebs_codec:encoder(),
                    device = Opener(),
                    closer = Closer});
        {replay, Device, Closer, Acceptor} ->
            ok = (State#rebslog_state.closer)(State#rebslog_state.device),
            replay_loop(State#rebslog_state{
                    device = Device,
                    closer = Closer,
                    acceptor = Acceptor});
        finish ->
            idle_loop(State);
        stop ->
            ok
    end.

record_loop(State) ->
    receive
        % Direction :: in | out
        {log, Direction, Data} ->
            NewState = log_data(State, Direction, Data),
            record_loop(NewState);
        {log, Data} ->
            NewState = log_data(State, default, Data),
            record_loop(NewState);
        finish ->
            ok = (State#rebslog_state.closer)(State#rebslog_state.device),
            idle_loop(State);
        stop ->
            ok
    end.

% Utility: log data to device
log_data(State = #rebslog_state{default_direction = DDir}, default, Data) ->
    log_data(State, DDir, Data);

log_data(State, Direction, Data) ->
    #rebslog_state{
        device = Device,
        encoder = Encoder } = State,
    {Chunk, NextEncoder} = Encoder(Data, Direction),
    ok = file:write(Device, Chunk),

    State#rebslog_state{encoder = NextEncoder}.


replay_loop(_State) ->
    replay = not_implemented.

% API
finish(Pid) ->
    Pid ! finish.

stop(Pid) ->
    Pid ! stop.

record(Pid, Device, Closer) ->
    Pid ! {record, Device, Closer}.

record_file(Pid, FileName) ->
    Opener = fun() -> {ok, File} = file:open(FileName, [append, raw]), File end,
    Closer = fun(D) -> file:close(D) end,
    Pid ! {open_record, Opener, Closer}.

replay(_, _, _) ->
    {error, not_implemented}.
replay_file(_, _) ->
    {error, not_implemented}.
