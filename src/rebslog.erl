%% Rebslog - replayable binary stream log
%
% Usage:
%       start(Name) to start daemon
%       record_file(Daemon, File) to start recording
%       replay_file(Daemon, File, Acceptor) to start realtime replay
%       finish(Daemon) to stop
%
%       fold_file(Fun, Acc0, File) to iterate over entries in file
%               Fun = fun(Time, Message, AccIn) -> AccOut
%               Time = erlang:now()
%               File = filename:name()
%
% generic unpack example:
%   rebslog:fold_file(fun(Time, Msg, Acc) ->
%           io:format("~w -> ~w~n", [Time, Msg]),
%           Acc + 1
%       end, 0, "/home/stolen/input.rebs")
%

-module(rebslog).

-export([start/1, start_link/1, stop/1]).
-export([record/3, record_file/2, replay/4, replay_file/3, finish/1, fold_file/3]).
-export([log/2, log_in/2, log_out/2]).

-define(READ_CHUNK_SIZE, 2048).
-record(rebslog_state, {
        encoder = undefined         :: fun((binary(), in|out) -> {binary(), fun()}),
        decoder = undefined         :: fun((binary()) -> {[term()], fun()}),
        device = undefined          :: file:io_device(),
        closer = fun(_) -> ok end   :: fun((file:io_device()) -> ok),
        acceptor = undefined        :: pid(),
        default_direction = out     :: in | out
    }).

-record(fold_state, {
        input,
        decoder,
        lastabs,
        lastsec,
        lastms,
        function,
        acc
    }).

-record(rtworker_state, {
        reader = undefined          :: pid(),
        acceptor = undefined        :: pid(),
        curtime = undefined         :: integer(),   % Virtual UNIX time
        nexttime = undefined        :: integer(),   % Virtual UNIX time of previous or next timestamp
        last_abs = undefined        :: integer(),   % last absolute timestamp
        nextmsgs = []               :: [term()],    % Messages on next timestamp
        buffer = undefined          :: queue()      % Buffer for upcoming events
    }).

% Utility to convert any term to atom
atomize(Atom) when is_atom(Atom) ->
    Atom;
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
            replay_init(State#rebslog_state{
                    decoder = rebs_codec:decoder(),
                    device = Device,
                    closer = Closer,
                    acceptor = Acceptor});
        {open_replay, Opener, Closer, Acceptor} ->
            ok = (State#rebslog_state.closer)(State#rebslog_state.device),
            replay_init(State#rebslog_state{
                    decoder = rebs_codec:decoder(),
                    device = Opener(),
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
            NewState = log_data(State, term, Data),
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

% Fold over entries in specified file
fold_file(Function, Acc0, File) ->
    {ok, F} = file:open(File, [read, raw, binary]),
    Decoder = rebs_codec:decoder(),
    State = #fold_state {
        input = F,
        decoder = Decoder,
        function = Function,
        acc = Acc0
    },
    FinalState = do_fold_file(State),
    ok = file:close(F),

    FinalState#fold_state.acc.

do_fold_file(State = #fold_state{input=File}) ->
    case file:read(File, 1024*1024) of
        eof ->
            State;
        {ok, Data} ->
            PFState = fold_process_chunk(State, Data),
            do_fold_file(PFState)
    end.


fold_process_chunk(State = #fold_state {decoder = Decoder}, Data) ->
    {Packets, NextDec} = Decoder(Data),
    PFState = fold_process_packets(State, Packets),

    PFState#fold_state {decoder = NextDec}.

fold_process_packets(State, []) ->
    State;
fold_process_packets(State, [Packet | MorePackets]) ->
    PPState = fold_packet(State, Packet),
    fold_process_packets(PPState, MorePackets).

fold_packet(State, {timestamp, absolute, Time}) ->
    State#fold_state{
        lastabs = Time,
        lastsec = Time,
        lastms = 0 };

fold_packet(State = #fold_state{lastabs = Time}, {timestamp, relative, Diff}) ->
    State#fold_state{
        lastsec = Time + Diff,
        lastms = 0 };

fold_packet(State, {MilliSecDiff, term, Message}) ->
    #fold_state {
        function = Function,
        acc = Acc,
        lastsec = Sec,
        lastms = MilliSec } = State,

    CurSec = Sec + ((MilliSec + MilliSecDiff) div 1000),
    CurMilliSec = (MilliSec + MilliSecDiff) rem 1000,

    CurNow = {CurSec div 1000000, CurSec rem 1000000, CurMilliSec * 1000},

    NextAcc = Function(CurNow, Message, Acc),

    State#fold_state {
        acc = NextAcc,
        lastsec = CurSec,
        lastms = CurMilliSec }.

% Replay routine. Initialization
replay_init(State = #rebslog_state{acceptor = Acceptor}) ->
    WState = #rtworker_state{
        acceptor = Acceptor,
        reader = self() },
    RTWorker = spawn_link(fun() -> replay_worker(WState) end),
    replay_loop(State, RTWorker).

% Replay loop
replay_loop(State, RTWorker) ->
    #rebslog_state{
        device = Device,
        closer = Closer,
        decoder = Decoder } = State,
    receive
        continue ->
            case file:read(Device, ?READ_CHUNK_SIZE) of
                eof ->
                    RTWorker ! eof,
                    Closer(Device),
                    idle_loop(#rebslog_state{});
                {ok, Chunk} ->
                    {Packets, NextDecoder} = Decoder(Chunk),
                    RTWorker ! {packets, Packets},
                    replay_loop(State#rebslog_state{decoder = NextDecoder}, RTWorker)
            end;
        finish ->
            RTWorker ! eof,
            Closer(Device),
            idle_loop(#rebslog_state{});
        stop ->
            RTWorker ! eof,
            ok = Closer(Device)
    end.

% Replay worker. This is realtime function. It manages timers and sends actual messages.
replay_worker(State = #rtworker_state{buffer = undefined}) ->
    Reader = State#rtworker_state.reader,
    % Initialization: create queue, start reference timer
    {_BadPackets, {timestamp, absolute, Time0}, Queue0} = rtworker_get_interval(Reader, queue:new(), []),
    {ok, _} = timer:send_interval(1000, fire),
    replay_worker(State#rtworker_state{
            buffer = Queue0,
            nexttime = Time0,
            last_abs = Time0,
            curtime = Time0 - 1
        });

replay_worker(State) ->
    #rtworker_state{
        reader = Reader,
        acceptor = Acceptor,
        buffer = Queue,
        nexttime = FireTime,
        last_abs = PrevAbs,
        curtime = CurTime } = State,

    {Packets, NextTimestamp, NextQueue} = rtworker_get_interval(Reader, Queue, []),

    receive_fire(FireTime - CurTime),
    schedule_packets(Packets, Acceptor),

    NextState = case NextTimestamp of
        {timestamp, absolute, Value} ->
            State#rtworker_state{
                nexttime = Value,
                last_abs = Value,
                curtime = FireTime,
                buffer = NextQueue };
        {timestamp, relative, Value} ->
            State#rtworker_state{
                nexttime = PrevAbs + Value,
                curtime = FireTime,
                buffer = NextQueue };
        undefined ->
            %io:format("replay_worker: terminating~n"),
            % EOF -> shutdown worker
            Reader ! finish,
            ok
    end,

    replay_worker(NextState).



% Read and replenish queue until timestamp packet is met or something bad happens
rtworker_get_interval(Reader, Queue, RevInterval) ->
    case queue:out(Queue) of
        {empty, _} ->
            % Empty queue: Request more packets
            Reader ! continue,
            receive
                {packets, Packets} ->
                    % Yahoo! Got packets. Now recurse
                    NewQueue = queue:from_list(Packets),
                    rtworker_get_interval(Reader, NewQueue, RevInterval);
                eof ->
                    % End of stream. OK too
                    {lists:reverse(RevInterval), undefined, Queue};
                fire ->
                    % Suddenly we have to fire bunch of packets.
                    % We need to handle this wisely
                    {lists:reverse(RevInterval), incomplete}
            end;
        {{value, QHead}, NewQueue} ->
            % Got head of queue. Now let's look what's inside it
            case QHead of
                {timestamp, _, _} ->
                    % Next timestamp packet. Wish every run end like this
                    {lists:reverse(RevInterval), QHead, NewQueue};
                DataPacket ->
                    % OK, put packet in front of previous ones and recurse.
                    rtworker_get_interval(Reader, NewQueue, [DataPacket|RevInterval])
            end
    end.

% Block until Count fire messages received
receive_fire(0) ->
    ok;
receive_fire(Count) ->
    receive
        fire ->
            receive_fire(Count - 1)
    end.

% Set timers to deliver data packets
schedule_packets(Packets, Acceptor) ->
    lists:foldl(fun({MilliSecDiff, Dir, Data}, Offset) ->
                NextOffset = Offset + MilliSecDiff,
                erlang:send_after(NextOffset, Acceptor, {replay, Dir, Data}),
                NextOffset
        end, 0, Packets).


% API
finish(Pid) ->
    Pid ! finish.

stop(Pid) ->
    Pid ! stop.

record(Pid, Device, Closer) ->
    Pid ! {record, Device, Closer}.

record_file(Pid, FileName) ->
    Opener = fun() -> {ok, File} = file:open(FileName, [write, raw]), File end,
    Closer = fun(D) -> file:close(D) end,
    Pid ! {open_record, Opener, Closer}.

replay(Pid, Device, Closer, Acceptor) ->
    Pid ! {replay, Device, Closer, Acceptor}.
replay_file(Pid, FileName, Acceptor) ->
    Opener = fun() -> {ok, File} = file:open(FileName, [read, raw, binary]), File end,
    Closer = fun(D) -> file:close(D) end,
    Pid ! {open_replay, Opener, Closer, Acceptor}.

log_in(Pid, Data) ->
    Pid ! {log, in, Data}.

log_out(Pid, Data) ->
    Pid ! {log, out, Data}.

log(Pid, Term) ->
    Pid ! {log, Term}.
