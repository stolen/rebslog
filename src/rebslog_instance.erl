-module(rebslog_instance, [Pid]).

-export([record/2, record_file/1, replay/3, replay_file/2, stop/0, finish/0]).
-export([log/1, log_in/1, log_out/1]).

finish()    -> rebslog:finish(Pid).
stop()      -> rebslog:stop(Pid).

record(Device, Closer) -> rebslog:record(Pid, Device, Closer).
record_file(FileName)  -> rebslog:record_file(Pid, FileName).

replay(Device, Closer, Acceptor) -> rebslog:replay(Pid, Device, Closer, Acceptor).
replay_file(FileName, Acceptor)  -> rebslog:replay_file(Pid, FileName, Acceptor).

log(Term)     -> rebslog:log(Pid, Term).
log_in(Data)  -> rebslog:log_in(Pid, Data).
log_out(Data) -> rebslog:log_out(Pid, Data).
