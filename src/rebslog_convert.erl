-module(rebslog_convert).
-author({"Danil Zagoskin", z@gosk.in}).

-export([filter_file/3, extract_reasons/3]).

-record(state, {
        input,
        output,
        filter,
        lastsec,
        lastms,
        decoder
    }).

extract_reasons(InFile, OutFile, Reasons) ->
    filter_file(InFile, OutFile, reason_extractor(Reasons)).

-spec filter_file(InFile :: filename:name(), OutFile :: filename:name(),
    FilterFun :: fun((Time::erlang:now(), Reason::any(), Message::any()) -> Output :: iolist() | ignore)) -> ok | {error, reason}.

filter_file(InFile, OutFile, FilterFun) ->
    {ok, InF} = file:open(InFile, [read, binary, raw]),
    {ok, OutF} = file:open(OutFile, [write, raw]),

    read_filter_write(InF, OutF, FilterFun),

    file:close(InF),
    file:close(OutF).


read_filter_write(In, Out, FilterFun) ->
    State = #state{
        input = In,
        output = Out,
        filter = FilterFun,
        decoder = rebs_codec:decoder() },
    read_filter_write(State).

read_filter_write(State = #state{input=In}) ->
    case file:read(In, 1024*1024) of
        eof ->
            ok;
        {ok, Data} ->
            NewState = process_chunk(State, Data),
            read_filter_write(NewState)
    end.

process_chunk(State = #state {decoder = Decoder}, Data) ->
    {Packets, NextDec} = Decoder(Data),
    NextState = process_packets(State, Packets),

    NextState#state {decoder = NextDec}.

process_packets(State, [Packet | MorePackets]) ->
    PPState = process_packet(State, Packet),
    process_packets(PPState, MorePackets).


process_packet(State, {timestamp, absolute, Time}) ->
    State#state{
        lastsec = Time,
        lastms = 0 };

process_packet(State = #state{lastsec = Time}, {timestamp, relative, Diff}) ->
    State#state{
        lastsec = Time + Diff,
        lastms = 0 };

process_packet(State, {MilliSecDiff, term, {Reason, Message}}) ->
    #state {
        output = Out,
        filter = FilterFun,
        lastsec = Sec,
        lastms = MilliSec } = State,

    CurSec = Sec + ((MilliSec + MilliSecDiff) div 1000),
    CurMilliSec = (MilliSec + MilliSecDiff) rem 1000,

    CurNow = {CurSec div 1000000, CurSec rem 1000000, CurMilliSec * 1000},

    case FilterFun(CurNow, Reason, Message) of
        ignore ->
            ok;
        Output ->
            ok = file:write(Out, Output)
    end,

    State#state {
        lastsec = CurSec,
        lastms = CurMilliSec }.


reason_extractor(Reasons) ->
    lists:foldl(fun(Reason, AccFun) ->
                fun(MTime, MReason, MMessage) when MReason == Reason ->
                        format_message(MTime, MReason, MMessage);
                    (MTime, MReason, MMessage) ->
                        AccFun(MTime, MReason, MMessage)
                end end, fun(_,_,_) -> ignore end, Reasons).

format_message(CurNow, Reason, Message) ->
    {_, Time} = calendar:now_to_datetime(CurNow),
    {Hour, Min, Sec} = Time,
    {_, _, MicroSec} = CurNow,
    FSec = Sec + MicroSec/1000000,

    Timestamp = io_lib:format("~2.10.0B:~2.10.0B:~6.3.0f", [Hour, Min, FSec]),
    
    [Timestamp, " <", Reason, "> ", Message].

