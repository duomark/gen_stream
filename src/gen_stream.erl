%%
%% %CopyrightBegin%
%%
%% Copyright DuoMark International, Inc.  2010-2012. All Rights Reserved.
%%
%% The contents of this file are subject to the New BSD license,
%% you may not use this file except in compliance with the License.
%% You should have received a copy of the License along with this
%% software. If not, it can be retrieved online at
%%  http://www.github.com/duomark/gen_stream/LICENSE.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% %CopyrightEnd%
%%
-module(gen_stream).
-behaviour(gen_server).

-include_lib("kernel/include/file.hrl").


%%% ---------------------------------------------------
%%%
%%% The gen_stream server delivers a serial interface
%%% to a data stream, whether it is based on a binary,
%%% a file or a generated data series.
%%%
%%% A user callback module is employed only when the
%%% data series is synthetic.  All other cases operate
%%% directly on the data without a user module.
%%%
%%% If the Parent process terminates the Module:terminate/2
%%% function is called.
%%%
%%% The callback module, if used, should export:
%%%
%%%   init(Args)
%%%     ==> {ok, State}
%%%         {ok, State, Timeout}
%%%         ignore
%%%         {stop, Reason}
%%%
%%%   stream_size(Args)
%%%    ==> {stream_size, Size}
%%%
%%%   inc_progress(Args, AlreadySeen, ThisChunk)
%%%     ==> NewAlreadySeen
%%%
%%%   extract_block(State)
%%%    ==> {noreply, State}
%%%        {noreply, State, Timeout}
%%%        {stop, Reason, State}
%%%              Reason = normal | shutdown | Term terminate(State) is called
%%%
%%%   extract_final_block(State)
%%%    ==> {noreply, State}
%%%        {noreply, State, Timeout}
%%%        {stop, Reason, State}
%%%              Reason = normal | shutdown | Term, terminate(State) is called
%%%
%%%   terminate(Reason, State) Let the user module clean up
%%%        always called when server terminates
%%%    ==> ok
%%%
%%%   code_change()
%%%    ==> ok
%%%
%%% ---------------------------------------------------


%% API
-export([start/1, start/2, start_link/1, start_link/2, stop/1,
         next_block/1, stream_size/1, stream_pos/1, pct_complete/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% spawned stream funs
-export([buffer_stream/3]).

%%%=========================================================================
%%%  API
%%%=========================================================================

%% gen_stream Behaviour definition
-export([behaviour_info/1]).

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].

behaviour_info(callbacks) ->
    [
     {init, 1},
     {stream_size, 1},
     {inc_progress, 2},
     {extract_block, 5},
     {extract_final_block, 5},
     {terminate, 2},
     {code_change, 3}
    ];
behaviour_info(_Other) ->
    undefined.

%%-----------------------------------------------------------------

-define(DEFAULT_NUM_PROCS, 1).     %% 1 process.
-define(DEFAULT_NUM_CHUNKS, 1).    %% 1 chunk buffer per process.
-define(DEFAULT_CIRCULAR, false).  %% One pass data stream.
-define(DEFAULT_CHUNK_SIZE, 8*1024). %% Default 8K chunks.
-define(DEFAULT_BLOCK_FACTOR, 1).  %% 1 chunk per block returned.

-record(gstr_opts, {
          num_procs =    ?DEFAULT_NUM_PROCS    :: pos_integer(),
          num_chunks =   ?DEFAULT_NUM_CHUNKS   :: pos_integer(),
          chunk_size =   ?DEFAULT_CHUNK_SIZE   :: pos_integer(),
          block_factor = ?DEFAULT_BLOCK_FACTOR :: pos_integer(),
          is_circular =  ?DEFAULT_CIRCULAR     :: boolean(),
          x_mfa :: {atom(), atom()},
          x_fun :: function()
         }).

-type from() :: {pid(), term()}.
-type server_ref() ::
        atom() | {atom(), node()} | {global, atom()} | pid().

-type stream_type() :: {'binary', binary()} | {'file', string()}
                     | {'behaviour', atom(), any()} | 'undefined'.

-record(gstr_state, {
          stream_type :: stream_type(),
          options = #gstr_opts{},
          orig_procs = [] :: list(pid()),
          procs = end_of_stream :: tuple(pid()) | 'end_of_stream',
          mod_state :: any(),   %% State of user-defined module
          active_proc = 1 :: non_neg_integer(),
          source_size = 0 :: non_neg_integer() | atom(),
          report_size :: non_neg_integer() | atom(),
          consumed = 0 :: non_neg_integer() | any()
         }).

-record(gstr_args, {
          stream_type   :: stream_type(),
          source_size   :: any(),
          num_procs     :: pos_integer(),
          num_chunks    :: pos_integer(),
          chunk_size    :: pos_integer(),
          block_factor  :: pos_integer(),
          block_size    :: pos_integer(),
          skip_size     :: pos_integer(),
          is_circular   :: boolean(),
          x_function    :: function() | {atom(), atom()},
          mod_state     :: any()
         }).


%%%  -----------------------------------------------------------------
%%% Starts a generic stream.
%%% start(Options)
%%% start(Name, Options)
%%% start_link(Options)
%%% start_link(Name, Options) where:
%%%    Name ::= {local, atom()} | {global, atom()}
%%%    Options ::= [{stream_type, {binary, Bin}
%%%                               | {file, FileName}
%%%                               | {behaviour, Mod, ModArgs}}
%%%                 | {num_procs, ProcCount}
%%%                 | {chunks_per_proc, ChunkCount}
%%%                 | {chunk_size, ChunkSize}
%%%                 | {timeout, Timeout}
%%%                 | {debug, [Flag]}]
%%%      Mod  ::= atom(), callback module implementing the 'real' server
%%%      ModArgs ::= any(), supplied as Mod:init(ModArgs)
%%%      Flag ::= trace | log | {logfile, File} | statistics | debug
%%%          (debug == log && statistics)
%%% Returns: {ok, Pid} |
%%%          {error, {already_started, Pid}} |
%%%          {error, Reason}
%%% -----------------------------------------------------------------
-spec start([{_,_}]) -> {ok, pid()} | {error, any()}.

start(Options) ->
    case init_options(Options) of
        {#gstr_state{} = State, SysOpts} ->
            gen_server:start(?MODULE, State, SysOpts);
        {Error, _SysOpts} ->
            {error, Error}
    end.


-spec start({local, atom()} | {global, atom()}, [{_,_}])
           -> {ok, pid()} | {error, any()}.

start(ServerName, Options) ->
    case init_options(Options) of
        {#gstr_state{} = State, SysOpts} ->
            gen_server:start(ServerName, ?MODULE, State, SysOpts);
        {Error, _SysOpts} ->
            {error, Error}
    end.


-spec start_link([{_,_}]) -> {ok, pid()} | {error, any()}.

start_link(Options) ->
    case init_options(Options) of
        {#gstr_state{} = State, SysOpts} ->
            gen_server:start_link(?MODULE, State, SysOpts);
        {Error, _SysOpts} ->
            {error, Error}
    end.


-spec start_link({local, atom()} | {global, atom()}, [{_,_}])
                -> {ok, pid()} | {error, any()}.

start_link(ServerName, Options) ->
    case init_options(Options) of
        {#gstr_state{} = State, SysOpts} ->
            gen_server:start_link(ServerName,?MODULE,State,SysOpts);
        {Error, _SysOptions} ->
            {error, Error}
    end.

%% -----------------------------------------------------------------
%% Stops a generic stream
%% -----------------------------------------------------------------
-spec stop(server_ref()) -> stopped.

stop(Server) ->
    gen_server:call(Server, stop).

%% -----------------------------------------------------------------
%% Get the next block of data from a stream
%% -----------------------------------------------------------------
-spec next_block(server_ref())
                -> {next_block, any() | end_of_stream}.

next_block(Server) ->
    gen_server:call(Server, next_block).

%% --------------------------------------------------------------------
%% Get the total size of the stream data. It may be an atom when the
%% stream 'is_circular' or if a behaviour module chooses to set
%% the stream size to 'infinite', 'unknown' or some other value.
%% --------------------------------------------------------------------
-spec stream_size(server_ref())
                 -> {stream_size, non_neg_integer() | atom()}.

stream_size(Server) ->
    gen_server:call(Server, stream_size).

%% --------------------------------------------------------------------
%% Get the current read position in the stream. It is the number
%% of bytes already processed from the beginning of the stream.
%% Circular streams continue incrementing with each block delivered.
%% --------------------------------------------------------------------
-spec stream_pos(server_ref())
                -> {stream_pos, integer() | atom()}.

stream_pos(Server) ->
    gen_server:call(Server, stream_pos).

%% --------------------------------------------------------------------
%% Report the percent complete from 0 to 100.  If the stream is
%% circular, report {pct_complete, is_circular}.
%% --------------------------------------------------------------------
-spec pct_complete(pid() | {local, atom()} | {global, atom()})
                  -> {pct_complete, pid(), integer() | atom()}.

pct_complete(Server) ->
    gen_server:call(Server, pct_complete).

%%%========================================================================
%%% gen_server callback functions
%%%========================================================================

%% --------------------------------------------------------------------
%% Initialize the gen_server state using the gen_stream options.
%% Any processes needed to generate the data stream are started.
%% --------------------------------------------------------------------
-spec init(#gstr_state{}) -> ignore | {stop, term()}
                                 | {ok, #gstr_state{consumed::0}}.

init(#gstr_state{mod_state=ignore}) ->
    ignore;
init(#gstr_state{mod_state={stop, Reason}}) ->
    {stop, Reason};
init(#gstr_state{mod_state=ModState} = State) ->
    Procs = launch_procs(setup_proc_args(State)),
    NewState = State#gstr_state{orig_procs=tuple_to_list(Procs),
                                procs=Procs},
    case ModState of

        %% Only behaviour streams should have a mod_state...
        {ok, ActualModState} ->
            {ok, NewState#gstr_state{mod_state=ActualModState}};
        {ok, ActualModState, hibernate} ->
            {ok, NewState#gstr_state{mod_state=ActualModState},
             hibernate};
        {ok, ActualModState, Timeout} ->
            {ok, NewState#gstr_state{mod_state=ActualModState},
             Timeout};

        %% All others will be undefined.
        undefined -> {ok, NewState}
    end.

setup_proc_args(#gstr_state{stream_type=ST, source_size=SS, mod_state=MS,
                            options=#gstr_opts{
                              num_procs=NP, num_chunks=NC, chunk_size=CS,
                              block_factor=BF, is_circular=IC, x_mfa=MFA,
                              x_fun=Fun}}) ->
    Function = case {MFA, Fun} of
                   {undefined, undefined} -> undefined;
                   {Mfa, undefined} -> Mfa;
                   {undefined, Fun} -> Fun
               end,
    #gstr_args{stream_type=ST, source_size=SS, num_procs=NP,
               num_chunks=NC, chunk_size=CS, block_factor=BF,
               block_size=CS*BF, skip_size=NP*CS*BF,
               is_circular=IC, x_function=Function, mod_state=MS}.


%% --------------------------------------------------------------------
%% Handle all next_block requests
%% Return a binary or end_of_stream when source is binary or file.
%% Behaviours are free to return any term, but must compute progress.
%% --------------------------------------------------------------------
-spec handle_call(atom(), from(), #gstr_state{})
                 -> {reply, {atom(), any()}, #gstr_state{}}
                        | {stop, normal, stopped, #gstr_state{}}.

handle_call(next_block, _From,
            #gstr_state{procs=end_of_stream} = State) ->
    {reply, {next_block, end_of_stream}, State};

handle_call(next_block, _From,
            #gstr_state{stream_type=Src, procs=Buffers,
                        active_proc=Active, consumed=Seen} = State) ->

    %% Synchronous wait for next chunk
    %% Crash on timeout or bad reply
    element(Active, Buffers) ! {next_block, self()},
    receive
        {next_block, end_of_stream} = EOS ->
            stop_procs(Buffers),
            {reply, EOS,
             State#gstr_state{active_proc=0, procs=end_of_stream}};

        {next_block, {RawBlock, RealBlock}} ->
            NextProc = case Active < tuple_size(Buffers) of
                           true ->  Active+1;
                           false -> 1
                       end,
            NewSeen = update_progress(Src, Seen, RawBlock),
            {reply, {next_block, RealBlock},
             State#gstr_state{active_proc=NextProc, consumed=NewSeen}}
    end;


%% --------------------------------------------------------------------
%% Handle all size, position and percent complete requests
%% Size can be any term, but pct_complete only works if it is integer.
%% --------------------------------------------------------------------
handle_call(stream_size, _From, #gstr_state{report_size=Size} = State) ->
    {reply, {stream_size, Size}, State};

handle_call(stream_pos, _From, #gstr_state{consumed=Seen} = State) ->
    {reply, {stream_pos, Seen}, State};

handle_call(pct_complete, _From,
            #gstr_state{options=#gstr_opts{is_circular=true}} = State) ->
    {reply, {pct_complete, is_circular}, State};

handle_call(pct_complete, _From,
            #gstr_state{procs=end_of_stream} = State) ->
    {reply, {pct_complete, 100}, State};

handle_call(pct_complete, _From,
            #gstr_state{source_size=Size, consumed=Seen} = State)
  when is_integer(Size) ->
    {reply, {pct_complete, (Seen * 100) div Size}, State};

%% If Size is not an integer, return its value every time...
%% For example: {pct_complete, infinite}
handle_call(pct_complete, _From, #gstr_state{source_size=Size} = State) ->
    {reply, {pct_complete, Size}, State};

handle_call(stop, _From, #gstr_state{procs=Procs} = State) ->
    stop_procs(Procs),
    {stop, normal, stopped, State};

handle_call(Request, _From, State) ->
    {reply, {unknown_request, Request}, State}.


%% --------------------------------------------------------------------
%% Cast and Info calls are not expected
%% --------------------------------------------------------------------
-spec handle_cast(any(), #gstr_state{}) -> {noreply, #gstr_state{}}.

handle_cast(_Msg, State) ->
    {noreply, State}.


-spec handle_info(any(), #gstr_state{}) -> {noreply, #gstr_state{}}.

handle_info(_Info, State) ->
    {noreply, State}.

%% --------------------------------------------------------------------
%% No shutdown cleanup is necessary, the linked processes will die.
%% Behaviour modules get a chance to cleanup.
%% --------------------------------------------------------------------
-spec terminate(any(), #gstr_state{}) -> ok.

terminate(Reason, #gstr_state{stream_type={behaviour, Module, _ModArgs},
                              mod_state=ModState, procs=Procs}) ->
    Module:terminate(Reason, ModState),
    stop_procs(Procs),
    ok;
terminate(_Reason, #gstr_state{stream_type={file, _Any}, procs=Procs}) ->
    stop_procs(Procs),
    ok;
terminate(_Reason, _State) ->
    ok.

stop_procs(end_of_stream) -> ok;
stop_procs(Procs)
  when is_tuple(Procs) ->
    [Pid ! {stop} || Pid <- tuple_to_list(Procs)].


%% --------------------------------------------------------------------
%% Code change is forwarded from gen_server.
%% --------------------------------------------------------------------
-spec code_change(atom(), #gstr_state{}, any())
                 -> {ok, #gstr_state{}}.

code_change(OldVsn,
            #gstr_state{stream_type={behaviour, Module, _ModArgs},
                        mod_state=ModState} = State, Extra) ->
    NewModState = Module:code_change(OldVsn, ModState, Extra),
    {ok, State#gstr_state{mod_state=NewModState}};
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%========================================================================
%% Internal functions
%%%========================================================================

%% --------------------------------------------------------------------
%% gen_stream options are separated from gen_server options and
%% then overlaid on a default initialized gen_stream options record.
%% Both are returned so that the user can specify gen_server options
%% and have them pass through to the underlying process to expose
%% control over tracing, debugging and other system features.
%% --------------------------------------------------------------------
init_options(Options) ->
    case proplists:split(Options, stream_options()) of
        {[[] | _], ExtraOpts} ->
            {{stream_type, missing}, ExtraOpts};
        {[StreamProps | StripeProps], SysOpts} ->
            case get_stream_type(StreamProps) of
                {error, Problem} ->
                    {Problem, SysOpts};
                StreamType ->
                    case init_options(tl(stream_options()),
                                      StripeProps, #gstr_opts{}) of
                        #gstr_opts{} = OptsRec ->
                            {make_opts_state(StreamType, OptsRec), SysOpts};
                        Error ->
                            {Error, SysOpts}
                    end
            end
    end.

get_stream_type(StreamProps) ->
    StreamType = proplists:get_value(hd(stream_options()), StreamProps),
    case valid_stream_type(StreamType) of
        false -> {error, {invalid_stream_type, StreamType}};
        true ->  StreamType
    end.


init_options([], [], #gstr_opts{x_mfa=MFA, x_fun=Fun} = Opts) ->
    case {MFA, Fun} of

        %% Success bindings...
        {undefined, undefined} -> Opts;
        {{M,F}, undefined} when is_atom(M), is_atom(F) -> Opts;
        {undefined, F} when is_function(F,1) -> Opts;

        %% Errors...
        {MFA, undefined} -> {x_mfa, {not_module_plus_function_pair, MFA}};
        {undefined, Fun} -> {x_fun, {not_fun_of_arity_1, Fun}};
        _Other -> {choose_only_one_option, {x_mfa, x_fun}}
    end;

init_options([Key | More], [PropList | Rest], #gstr_opts{} = Opts) ->
    NewOpts = case proplists:get_value(Key, PropList) of
                  undefined -> Opts;
                  Value -> set_gstr_opt(Opts, Key, Value)
              end,
    init_options(More, Rest, NewOpts).

valid_stream_type({binary, Bin}) when is_binary(Bin) -> true;
valid_stream_type({file, FileName}) when is_list(FileName) -> true;
valid_stream_type({behaviour, Module, _ModInitArgs})
  when is_atom(Module) ->
    true;
valid_stream_type(_Other) -> false.

stream_options() ->
    [stream_type, num_procs, num_chunks,
     chunk_size, block_factor, is_circular, x_mfa, x_fun].

%% Passing an invalid option type causes a crash.
set_gstr_opt(#gstr_opts{} = Opts, num_procs, Value)
  when is_integer(Value) ->
    Opts#gstr_opts{num_procs=Value};
set_gstr_opt(#gstr_opts{} = Opts, num_chunks, Value)
  when is_integer(Value) ->
    Opts#gstr_opts{num_chunks=Value};
set_gstr_opt(#gstr_opts{} = Opts, chunk_size, Value)
  when is_integer(Value) ->
    Opts#gstr_opts{chunk_size=Value};
set_gstr_opt(#gstr_opts{} = Opts, block_factor, Value)
  when is_integer(Value) ->
    Opts#gstr_opts{block_factor=Value};
set_gstr_opt(#gstr_opts{} = Opts, is_circular, Value)
  when is_boolean(Value) ->
    Opts#gstr_opts{is_circular=Value};
set_gstr_opt(#gstr_opts{} = Opts, x_mfa, Value) ->
    Opts#gstr_opts{x_mfa=Value};
set_gstr_opt(#gstr_opts{} = Opts, x_fun, Value) ->
    Opts#gstr_opts{x_fun=Value}.

make_opts_state(StreamType, #gstr_opts{is_circular=true} = OptsRec) ->
    case source_size(StreamType) of
        Size when is_integer(Size) ->
            make_gstr_state(StreamType, OptsRec, Size, true);
        NonInteger ->
            {error, {stream_cannot_be_circular,
                     {size_specified, NonInteger}}}
    end;
make_opts_state(StreamType, #gstr_opts{} = OptsRec) ->
    Size = source_size(StreamType),
    make_gstr_state(StreamType, OptsRec, Size, false).

make_gstr_state(StreamType, OptsRec, Size, IsCircular) ->
    CircularSize = if IsCircular -> is_circular; true -> Size end,
    #gstr_state{stream_type=StreamType, options=OptsRec,
                procs=end_of_stream, source_size=Size,
                mod_state=make_mod_state(StreamType),
                report_size=CircularSize}.

make_mod_state({behaviour, Mod, ModArgs}) -> Mod:init(ModArgs);
make_mod_state(_Other) -> undefined.

source_size({binary, Bin}) when is_binary(Bin) ->
    byte_size(Bin);
source_size({file, FileName}) when is_list(FileName) ->
    {ok, FileInfo} = file:read_file_info(FileName),
    FileInfo#file_info.size;
source_size({behaviour, Module, ModArgs}) when is_atom(Module) ->
    Module:stream_size(ModArgs).


%% --------------------------------------------------------------------
%% Progress update is automatic or requires Module:inc_progress/2.
%% --------------------------------------------------------------------
update_progress({behaviour, Module, _ModInitArgs}, Seen, Chunk) ->
    Module:inc_progress(Seen, Chunk);
update_progress(_Other, Seen, Chunk) when is_binary(Chunk) ->
    Seen + byte_size(Chunk).

%% --------------------------------------------------------------------
%% There can be more than one process to deliver binary blocks
%% Each process contains num_chunks buffers to allow pre-fetching.
%% The requests are distributed round-robin to the processes so that
%% two consecutive next_block/2 requests will be with two separate
%% processes.
%% --------------------------------------------------------------------
launch_procs(#gstr_args{num_procs=NumProcs} = Args) ->
    ProcNums = lists:seq(0, NumProcs-1),
    Procs = [proc_lib:spawn_link(?MODULE, buffer_stream, VArgs)
             || VArgs <- make_args(Args, ProcNums)],
    list_to_tuple(Procs).

start_pos(false, Num, BlockSize, _SrcSize) -> Num * BlockSize;
start_pos(true, Num, BlockSize, SrcSize) -> Num * BlockSize rem SrcSize.

make_args(#gstr_args{stream_type={binary, Bin}, source_size=SrcSize,
                     block_size=BlockSize, is_circular=IsCircular}
          = Args, ProcNums) ->
    [[Args, start_pos(IsCircular, Num, BlockSize, SrcSize), Bin]
     || Num <- ProcNums];
make_args(#gstr_args{stream_type={file, _Name} = File,
                     source_size=SrcSize, block_size=BlockSize,
                     is_circular=IsCircular} = Args, ProcNums) ->
    [[Args, start_pos(IsCircular, Num, BlockSize, SrcSize), File]
     || Num <- ProcNums];
make_args(#gstr_args{stream_type={behaviour, Module, _InitArgs},
                     source_size=SrcSize, block_size=BlockSize,
                     is_circular=IsCircular, mod_state=ModState}
          = Args, ProcNums) ->
    [[Args, start_pos(IsCircular, Num, BlockSize, SrcSize),
      {mod, Module, ModState}] || Num <- ProcNums].


%% --------------------------------------------------------------------
%% Worker processes and pre-fetch buffered stream block chunking logic:
%%   buffer_stream is the main loop inside each buffering process
%%   buffer_reply (_or_fetch) replies with a block from a buffer
%%   fetch_block excises a block from the stream source
%% --------------------------------------------------------------------

%% Open the file if this process reads one, then jump to normal proc.
buffer_stream(#gstr_args{} = Args, Pos, {file, FileName}) ->
    {ok, IoDevice} = file:open(FileName, [read, binary, raw,
                                          {read_ahead,0}]),
    buffer_stream(Args, Pos, IoDevice);

%% With 1 chunk buffer, prefetch chunk block and wait for a request.
buffer_stream(#gstr_args{num_procs=1, x_function=Xfn} = Args,
              Pos, BlockSrc) ->
    {Reply, NewPos} = fetch_block(Args, Pos, BlockSrc),
    send_reply(Xfn, Reply, BlockSrc),
    buffer_stream(Args, NewPos, BlockSrc);

%% Multiple buffers works the same but rotates the queue and always
%% maintains a full set of buffers.
buffer_stream(#gstr_args{} = Args, Pos, BlockSrc) ->
    buffer_stream(Args, Pos, BlockSrc, queue:new()).

buffer_stream(#gstr_args{num_chunks=NumBuffers, x_function=Xfn} = Args,
              Pos, BlockSrc, Buffers) ->
    case queue:len(Buffers) of
        FullCount when FullCount >= NumBuffers ->
            %% Buffer is full, so just get the chunk and reply.
            {{value, Reply}, PoppedBuffers} = queue:out(Buffers),
            buffer_reply(Reply, BlockSrc),
            buffer_stream(Args, Pos, BlockSrc, PoppedBuffers);
        Count ->
            if
                %% Buffer is empty, fetch a chunk first...
                Count =:= 0  ->
                    {Block, NextPos} =
                        fetch_block(Args, Pos, BlockSrc),
                    Reply = make_reply(Xfn, Block),
                    NewBuffers = queue:in(Reply, Buffers);

                %% Otherwise, just use next available.
                true ->
                    NextPos = Pos,
                    NewBuffers = Buffers
            end,

            %% Buffer is not full, schedule a new fetch soon...
            self() ! {fetch_block},

            %% Fetch or reply to a client request.
            {LastPos, LastBuffers} =
                buffer_reply_or_fetch(Args, NextPos,
                                      BlockSrc, NewBuffers),
            buffer_stream(Args, LastPos, BlockSrc, LastBuffers)
    end.

send_reply(_, end_of_stream, Bin) ->
    buffer_reply(end_of_stream, Bin);
send_reply(undefined, Reply, Bin) ->
    buffer_reply({Reply, Reply}, Bin);
send_reply({Mod, Fun}, Reply, Bin) ->
    buffer_reply({Reply, Mod:Fun(Reply)}, Bin);
send_reply(Fun, Reply, Bin) ->
    buffer_reply({Reply, Fun(Reply)}, Bin).

make_reply(_, end_of_stream)  -> end_of_stream;
make_reply(undefined, Reply)  -> {Reply, Reply};
make_reply({Mod, Fun}, Reply) -> {Reply, Mod:Fun(Reply)};
make_reply(Fun, Reply)        -> {Reply, Fun(Reply)}.

buffer_reply(ReplyPair, StreamSource) ->
    receive
        {next_block, From} -> From ! {next_block, ReplyPair};
        {stop} -> quit_proc(StreamSource)
                  %% A {fetch_block} message may arrive, so don't unload queue.
    end.

buffer_reply_or_fetch(#gstr_args{x_function=Xfn} = Args,
                      Pos, BlockSrc, Buffers) ->
    receive
        {next_block, From} ->
            {{value, ReplyPair}, NewBuffers} = queue:out(Buffers),
            From ! {next_block, ReplyPair},
            {Pos, NewBuffers};
        {fetch_block} ->
            {Block, NextPos} = fetch_block(Args, Pos, BlockSrc),
            Reply = make_reply(Xfn, Block),
            NewBuffers = queue:in(Reply, Buffers),
            {NextPos, NewBuffers};
        {stop} -> quit_proc(BlockSrc);

        %% In case a future hack breaks the message interface,
        %% and to avoid message overflow, warn and dump extra messages.
        UnexpectedMsg ->
            WarnFmt = "~w binary buffer received unexpected "
                "message ~w~n",
            error_logger:warning_msg(WarnFmt, [buffer_reply_or_fetch,
                                               UnexpectedMsg]),
            buffer_reply_or_fetch(Args, Pos, BlockSrc, Buffers)
    end.

quit_proc({mod, Module, ModState}) when is_atom(Module) ->
    Module:terminate(normal, ModState),
    exit(normal);
quit_proc(Bin) when is_binary(Bin) ->
    exit(normal);
quit_proc(IoDevice) ->
    file:close(IoDevice),
    exit(normal).


%% Signal end_of_stream for a non-circular stream...
fetch_block(#gstr_args{source_size=SrcSize, is_circular=false},
            Pos, _BlockSrc)
  when SrcSize =< Pos ->
    {end_of_stream, Pos};

%% Get last block from a non-circular stream...
fetch_block(#gstr_args{source_size=SrcSize, block_size=BlockSize,
                       is_circular=false} = Args, Pos, BlockSrc)
  when SrcSize < Pos + BlockSize ->
    {extract_final_block(BlockSrc, Pos, SrcSize, Args), SrcSize};

%% Replicate data if BlockSize bigger than entire stream...
fetch_block(#gstr_args{source_size=SrcSize, block_size=BlockSize,
                       skip_size=SkipSize, is_circular=true} = Args,
            Pos, BlockSrc)
  when SrcSize < BlockSize ->
    FullCopies = BlockSize div SrcSize,
    {Head, Dups} =
        case Pos of
            0 ->
                {extract_block(BlockSrc, 0, 0, Args), FullCopies};
            Pos when Pos >= SrcSize ->
                {extract_block(BlockSrc, 0, 0, Args), FullCopies};
            Pos ->
                Head1 = extract_final_block(BlockSrc, Pos, SrcSize, Args),
                ReducedCopies = (BlockSize - byte_size(Head1)) div SrcSize,
                {Head1, ReducedCopies}
        end,
    Remains = BlockSize - SrcSize * Dups - byte_size(Head),
    Remainder = extract_block(BlockSrc, 0, Remains, Args),
    Repeat = extract_block(BlockSrc, 0, SrcSize, Args),
    {list_to_binary([Head, lists:duplicate(Dups, Repeat), Remainder]),
     (Pos + SkipSize) rem SrcSize};

%% Get block wraparound in a circular stream...
fetch_block(#gstr_args{source_size=SrcSize, block_size=BlockSize,
                       skip_size=SkipSize, is_circular=true} = Args,
            Pos, BlockSrc)
  when SrcSize =< Pos ->
    {extract_block(BlockSrc, 0, BlockSize, Args), SkipSize rem SrcSize};
fetch_block(#gstr_args{source_size=SrcSize, block_size=BlockSize,
                       skip_size=SkipSize, is_circular=true} = Args,
            Pos, BlockSrc)
  when SrcSize < Pos + BlockSize ->
    HeadSize = SrcSize - Pos,
    TailSize = BlockSize - HeadSize,
    Head = extract_final_block(BlockSrc, Pos, SrcSize, Args),
    Tail = extract_block(BlockSrc, 0, TailSize, Args),
    {<<Head/binary, Tail/binary>>, (Pos + SkipSize) rem SrcSize};

%% Normal block at beginning or in middle of stream.
fetch_block(#gstr_args{source_size=SrcSize, block_size=BlockSize,
                       skip_size=SkipSize, is_circular=IsCircular} = Args,
            Pos, BlockSrc) ->
    NewPos = case IsCircular of
                 false -> Pos + SkipSize;
                 true ->  (Pos + SkipSize) rem SrcSize
             end,
    {extract_block(BlockSrc, Pos, BlockSize, Args), NewPos}.


%% --------------------------------------------------------------------
%% Extracting stream blocks for each type of stream
%% --------------------------------------------------------------------

extract_block({mod, Module, ModState}, Pos, NumBytes,
              #gstr_args{chunk_size=CS, block_factor=BF}) ->
    Module:extract_block(ModState, Pos, NumBytes, CS, BF);
extract_block(Bin, Pos, NumBytes, _Args)
  when is_binary(Bin) ->
    <<_Skip:Pos/binary, Block:NumBytes/binary, _Rest/binary>> = Bin,
    Block;
extract_block(_IoDevice, _Pos, 0, _Args) ->
    <<>>;
extract_block(IoDevice, Pos, NumBytes, _Args) ->
    {ok, Block} = file:pread(IoDevice, Pos, NumBytes),
    Block.

extract_final_block({mod, Module, ModState}, Pos, StreamSize,
                    #gstr_args{chunk_size=CS, block_factor=BF}) ->
    Module:extract_final_block(ModState, Pos, StreamSize-Pos, CS, BF);
extract_final_block(Bin, Pos, _BinSize, _Args)
  when is_binary(Bin) ->
    <<_Skip:Pos/binary, Rest/binary>> = Bin,
    Rest;
extract_final_block(IoDevice, Pos, FileSize, _Args) ->
    case FileSize - Pos of
        0 -> <<>>;
        NumBytes ->
            {ok, Rest} = file:pread(IoDevice, Pos, NumBytes),
            Rest
    end.
