-module(rebs_codec).
-export([encoder/0, encoder/3]).
-export([decoder/0, decoder/2]).
-compile(inline).
-include("Rebslog-format.hrl").

-record(encoder_state, {
        last_Ms = undefined,
        last_abs_s = undefined,
        next_abs_s = undefined,
        last_s = undefined,
        last_ms = undefined
    }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% ASN.1 adapter functions.
%
% They should be inlined and used for generating
% right tuples without using ugly quoted atoms
% in logic code

% Call encode from ASN.1-generated module
binary_encode(Type, Packet) ->
    'Rebslog-format':encode(Type, Packet).

timestamp_bin_packet(Timestamp) ->
    binary_encode('Rebs-packet', {timestamp, Timestamp}).

% Generate UNIX time record for absolute timestamp
abs_ts_bin_packet({Megasec, Sec, _Microsec}) ->
    timestamp_bin_packet({absolute, Megasec * 1000000 + Sec}).

% Generate relative timestamp (seconds since last Absolute-timestamp)
rel_ts_bin_packet(Diff_s) ->
    timestamp_bin_packet({relative, Diff_s}).

% Generate binary data packet
data_bin_packet(MilliSecDiff, in, Data) when is_binary(Data) ->
    binary_encode('Rebs-packet', {'data-in', #'Data-packet'{
            timestamp = MilliSecDiff,
            data = {0, Data} }});
data_bin_packet(MilliSecDiff, out, Data) when is_binary(Data) ->
    binary_encode('Rebs-packet', {'data-out', #'Data-packet'{
            timestamp = MilliSecDiff,
            data = {0, Data} }});
data_bin_packet(MilliSecDiff, term, Data) ->
    binary_encode('Rebs-packet', {'term', #'Data-packet'{
            timestamp = MilliSecDiff,
            data = {0, term_to_binary(Data)} }});

data_bin_packet(MilliSecDiff, Direction, Data) when is_list(Data) ->
    data_bin_packet(MilliSecDiff, Direction, iolist_to_binary(Data)).



encoder() ->
    make_encoder(#encoder_state{}).

make_encoder(State) ->
    fun (Data, Direction) ->
            ?MODULE:encoder(State, Data, Direction)
    end.

encoder(State, Data, Direction) ->
    Now = erlang:now(),

    {TS_State, TimestampBin} = timestamp_if_needed(State, Now),
    %io:format("TimestampBin = ~p~n", [TimestampBin]),
    {NewState, DataPacketBin} = gen_bin_data_packet(TS_State, Data, Direction, Now),
    %io:format("DataPacketBin = ~p~n", [DataPacketBin]),

    %{<<TimestampBin/binary, DataPacketBin/binary>>, make_encoder(NewState)}.
    {[TimestampBin, DataPacketBin], make_encoder(NewState)}.

% Return timestamp packet binary or empty binary if not needed
timestamp_if_needed(State = #encoder_state{
                        last_Ms = LastMegaSec, 
                        next_abs_s = NextAbsSec}, 
                    Now = {MegaSec, Sec, _}) when   LastMegaSec =/= MegaSec; 
                                                    Sec >= NextAbsSec ->
    % Empty state or old absolute timestamp: initialize or update
    {ok, TSBin} = abs_ts_bin_packet(Now),
    NextState = State#encoder_state{
        last_Ms = MegaSec,
        last_abs_s = Sec,
        next_abs_s = Sec + 250, % This is needed to not allow relative seconds exceed one octet
        last_s = Sec,
        last_ms = 0},
    {NextState, TSBin};

timestamp_if_needed(State = #encoder_state{last_abs_s = LastAbsSec, last_s = LastSec}, _Now = {_, Sec, _}) 
            when LastSec =/= Sec ->
    % Fresh absolute timestamp but outdated relative timestamp
    {ok, TSBin} = rel_ts_bin_packet(Sec - LastAbsSec),
    NextState = State#encoder_state{
        last_s = Sec,
        last_ms = 0},
    {NextState, TSBin};

timestamp_if_needed(State, _Now) ->
    % LastMegaSec == MegaSec, LastSec == Sec
    % No timestamp packet needed
    {State, <<>>}.

% Generate data packet
gen_bin_data_packet(State, Data, Direction, _Now = {_, _, MicroSec}) ->
    MilliSec = MicroSec div 1000,
    MilliSecDiff = MilliSec - State#encoder_state.last_ms,

    {ok, DataPktBin} = data_bin_packet(MilliSecDiff, Direction, Data),
    NextState = State#encoder_state{last_ms = MilliSec},

    {NextState, DataPktBin}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%  Decoder


decoder() ->
    make_decoder(<<>>).

make_decoder(BinHead) ->
    fun(BinTail) ->
            ?MODULE:decoder(BinHead, BinTail)
    end.

decoder(BinHead, BinTail) ->
    Buffer = <<BinHead/binary, BinTail/binary>>,
    {Packets, NewBuffer} = decode_all_packets(Buffer, []),
    {Packets, make_decoder(NewBuffer)}.

decode_all_packets(Buffer, RevPackets) ->
    case 'Rebslog-format':decode('Rebs-packet', Buffer) of
        {ok, Packet, Tail} ->
            decode_all_packets(Tail, [filter_packet(Packet)|RevPackets]);
        {error,{asn1,{{badmatch,_}, _}}} ->
            {lists:reverse(RevPackets), Buffer}
    end.

filter_packet({'data-in',{_,MilliSecDiff,{0,Data}}}) ->
    {MilliSecDiff, in, Data};
filter_packet({'data-out',{_,MilliSecDiff,{0,Data}}}) ->
    {MilliSecDiff, out, Data};
filter_packet({'term',{_,MilliSecDiff,{0,Data}}}) ->
    {MilliSecDiff, term, binary_to_term(Data)};
filter_packet({timestamp,{Type,Value}}) ->
    {timestamp, Type, Value}.

