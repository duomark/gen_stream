%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2010. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% %CopyrightEnd%
%%
-module(gen_stream_SUITE).

-include("test_server.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("kernel/include/inet.hrl").

-export([init_per_testcase/2, fin_per_testcase/2]).

-export([all/1]).
-export([
	 start_bin/1, start_link_bin/1, bad_calls/1,
	 stream_info_bin/1, stream_info_file/1, stream_info_module/1,
	 num_procs_bin/1, num_procs_file/1, num_procs_module/1,
	 num_buffers_bin/1, num_buffers_file/1, num_buffers_module/1,
	 block_buffers_bin/1, block_buffers_file/1, block_buffers_module/1,
	 circular_mult_bin/1, circular_non_mult_bin/1,
	 circular_replicated_bin/1,
	 circular_mult_file/1, circular_non_mult_file/1,
	 circular_replicated_file/1,
	 circular_mult_module/1, circular_non_mult_module/1,
	 circular_replicated_module/1
	]).

-export([reverse/1]).

-define(datadir, "./gen_stream_SUITE_data").
-define(datadir(Conf), ?config(data_dir, Conf)).

all(suite) ->
    [start_bin, start_link_bin, bad_calls,
     stream_info_bin, stream_info_file, stream_info_module,
     num_procs_bin, num_procs_file, num_procs_module,
     num_buffers_bin, num_buffers_file, num_buffers_module,
     block_buffers_bin, block_buffers_file, block_buffers_module,
     circular_mult_bin, circular_non_mult_bin, circular_replicated_bin,
     circular_mult_file, circular_non_mult_file, circular_replicated_file,
     circular_mult_module, circular_non_mult_module, circular_replicated_module
    ].


-define(default_timeout, ?t:minutes(1)).

init_per_testcase(_Case, Config) ->
    ?line Dog = ?t:timetrap(?default_timeout),
    [{watchdog, Dog} | Config].
fin_per_testcase(_Case, Config) ->
    Dog = ?config(watchdog, Config),
    test_server:timetrap_cancel(Dog),
    ok.


%%--------------------------------------
%% Start and stop a gen_stream.
%%--------------------------------------
start_bin(suite) -> [];
start_bin(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    %% Binary data stream...
    BinExample = <<"short stream">>,
    BinExampleSize = byte_size(BinExample),
    SimpleBinOpts = [{stream_type, {binary, BinExample}}],
    ?line {ok, Pid0b} = gen_stream:start(SimpleBinOpts),
    WorkerProcs0b = get_linked_procs(Pid0b),  %% All are started when 1 replies.
    ?line {stream_size, BinExampleSize} = gen_stream:stream_size(Pid0b),
    ?line stopped = gen_stream:stop(Pid0b),
    ?line busy_wait_for_process_to_end(Pid0b, 600),
    ?line check_procs_dead(WorkerProcs0b),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid0b)),

    %% File stream...
    FileExample = test_file_path(Config, "alpha.txt"),
    FileExampleSize = file_size(FileExample),
    SimpleFileOpts = [{stream_type, {file, FileExample}}],
    ?line {ok, Pid0f} = gen_stream:start(SimpleFileOpts),
    WorkerProcs0f = get_linked_procs(Pid0f),
    ?line {stream_size, FileExampleSize} = gen_stream:stream_size(Pid0f),
    ?line stopped = gen_stream:stop(Pid0f),
    ?line busy_wait_for_process_to_end(Pid0f, 600),
    ?line check_procs_dead(WorkerProcs0f),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid0f)),

    %% Behaviour stream...
    BehaviourOpts = [{stream_type, {behaviour, gen_stream_odd_nums, []}}],
    ?line {ok, Pid0m} = gen_stream:start(BehaviourOpts),
    WorkerProcs0m = get_linked_procs(Pid0m),
    ?line {stream_size, infinite} = gen_stream:stream_size(Pid0m),
    ?line stopped = gen_stream:stop(Pid0m),
    ?line busy_wait_for_process_to_end(Pid0m, 600),
    ?line check_procs_dead(WorkerProcs0m),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid0m)),

    %% Invalid stream_type...
    ?line {error, {stream_type, missing}} = gen_stream:start([]),
    ?line {error, {stream_type, missing}} =
	gen_stream:start([{timeout, 100}]),
    ?line {error, {stream_type, missing}} =
	gen_stream:start([{num_procs, 5}, {timeout, 100},
			      {num_chunks, 12}, {chunk_size, 30}]),
    ?line {error, {invalid_stream_type, bad}} =
	gen_stream:start([{stream_type, bad}]),

    %% Circular streams...
    SmallCircular = [{chunk_size, BinExampleSize - 3},{is_circular, true}],
    ?line {ok, Pid1sc} = gen_stream:start(SimpleBinOpts ++ SmallCircular),
    WorkerProcs1sc = get_linked_procs(Pid1sc),
    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid1sc),
    ?line stopped = gen_stream:stop(Pid1sc),
    ?line busy_wait_for_process_to_end(Pid1sc, 600),
    ?line check_procs_dead(WorkerProcs1sc),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid1sc)),

    LargeCircular = [{chunk_size, 2*BinExampleSize + 3},{is_circular, true}],
    ?line {ok, Pid1lc} = gen_stream:start(SimpleBinOpts ++ LargeCircular),
    WorkerProcs1lc = get_linked_procs(Pid1lc),
    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid1lc),
    ?line stopped = gen_stream:stop(Pid1lc),
    ?line busy_wait_for_process_to_end(Pid1lc, 600),
    ?line check_procs_dead(WorkerProcs1lc),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid1lc)),

    %% Validate x_mfa, x_fun options...
    ?line {error, {x_mfa, {not_module_plus_function_pair, 4}}} =
	gen_stream:start(SimpleBinOpts ++ [{x_mfa, 4}]),
    ?line {error, {x_mfa, {not_module_plus_function_pair, {?MODULE, 3}}}} =
	gen_stream:start(SimpleBinOpts ++ [{x_mfa, {?MODULE, 3}}]),
    ?line {ok, PidMfa} =
	gen_stream:start(SimpleBinOpts ++ [{x_mfa, {?MODULE, reverse}}]),
    WorkerProcsMfa = get_linked_procs(PidMfa),
    ?line {stream_size, BinExampleSize} = gen_stream:stream_size(PidMfa),
    ?line stopped = gen_stream:stop(PidMfa),
    ?line busy_wait_for_process_to_end(PidMfa, 600),
    ?line check_procs_dead(WorkerProcsMfa),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(PidMfa)),

    ?line {error, {x_fun, {not_fun_of_arity_1, _Fun1}}} =
	gen_stream:start(SimpleBinOpts ++ [{x_fun, fun() -> 4 end}]),
    ?line {error, {x_fun, {not_fun_of_arity_1, _Fun2}}} =
	gen_stream:start(SimpleBinOpts ++ [{x_fun, fun(X,Y) -> X*Y end}]),
    ?line {ok, PidFun} =
	gen_stream:start(SimpleBinOpts ++ [{x_fun, fun(X) -> X end}]),
    WorkerProcsFun = get_linked_procs(PidFun),
    ?line {stream_size, BinExampleSize} = gen_stream:stream_size(PidFun),
    ?line stopped = gen_stream:stop(PidFun),
    ?line busy_wait_for_process_to_end(PidFun, 600),
    ?line check_procs_dead(WorkerProcsFun),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(PidFun)),

    ?line {error, {choose_only_one_option, {x_mfa, x_fun}}} =
	gen_stream:start(SimpleBinOpts ++ [{x_mfa, {?MODULE, stop}},
					   {x_fun, fun(X) -> 2*X end}]),

    %% local register
    LocalName = {local, my_test_name},
    ?line {ok, Pid2} = gen_stream:start(LocalName, SimpleBinOpts),
    ?line {stream_size, BinExampleSize} =
	gen_stream:stream_size(my_test_name),
    ?line {error, {already_started, Pid2}} =
	gen_stream:start(LocalName, SimpleBinOpts),

    ?line stopped = gen_stream:stop(my_test_name),
    ?line busy_wait_for_process_to_end(Pid2, 600),
    ?line {'EXIT', {noproc,_}} =
	(catch gen_stream:stream_size(my_test_name)),

    %% global register
    GlobalName = {global, my_test_name},
    ?line {ok, Pid3} = gen_stream:start(GlobalName, SimpleBinOpts),
    ?line {stream_size, BinExampleSize} =
	gen_stream:stream_size(GlobalName),
    ?line {error, {already_started, Pid3}} =
	gen_stream:start(GlobalName, SimpleBinOpts),

    ?line stopped = gen_stream:stop(GlobalName),
    ?line busy_wait_for_process_to_end(Pid3, 600),
    ?line {'EXIT', {noproc,_}} =
	(catch gen_stream:stream_size(GlobalName)),

    test_server:messages_get(),

    %% Must wait for all error messages before going to next test.
    %% (otherwise it interferes too much with real time characteristics).
    case os:type() of
	vxworks ->
	    receive after 5000 -> ok end;
	_ ->
	    ok
    end,
    process_flag(trap_exit, OldFl),
    ok.


start_link_bin(suite) -> [];
start_link_bin(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    %% Binary data stream...
    BinExample = <<"binary data">>,
    BinExampleSize = byte_size(BinExample),
    SimpleBinOpts = [{stream_type, {binary, BinExample}}],
    ?line {ok, Pid1} = gen_stream:start_link(SimpleBinOpts),
    ?line {stream_size, BinExampleSize} = gen_stream:stream_size(Pid1),
    ?line stopped = gen_stream:stop(Pid1),
    ?line ok = wait_for_link_death(Pid1),

    %% File stream...
    FileExample = test_file_path(Config, "alpha.txt"),
    FileExampleSize = file_size(FileExample),
    SimpleFileOpts = [{stream_type, {file, FileExample}}],
    ?line {ok, Pid1f} = gen_stream:start_link(SimpleFileOpts),
    ?line {stream_size, FileExampleSize} = gen_stream:stream_size(Pid1f),
    ?line stopped = gen_stream:stop(Pid1f),
    ?line ok = wait_for_link_death(Pid1f),

    %% Behaviour stream...
    BehaviourOpts = [{stream_type, {behaviour, gen_stream_odd_nums, []}}],
    ?line {ok, Pid1m} = gen_stream:start_link(BehaviourOpts),
    ?line {stream_size, infinite} = gen_stream:stream_size(Pid1m),
    ?line stopped = gen_stream:stop(Pid1m),
    ?line ok = wait_for_link_death(Pid1m),

    %% Invalid stream_type...
    ?line {error, {stream_type, missing}} = gen_stream:start_link([]),
    ?line {error, {stream_type, missing}} =
	gen_stream:start_link([{timeout, 100}]),
    ?line {error, {stream_type, missing}} =
	gen_stream:start_link([{num_procs, 5}, {timeout, 100},
				   {num_chunks, 12}, {chunk_size, 30}]),
    ?line {error, {invalid_stream_type, bad}} =
	gen_stream:start_link([{stream_type, bad}]),

    %% Circular streams...
    SmallCircular = [{chunk_size, BinExampleSize - 3},{is_circular, true}],
    ?line {ok, Pid1sc} =
	gen_stream:start_link(SimpleBinOpts ++ SmallCircular),
    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid1sc),
    ?line stopped = gen_stream:stop(Pid1sc),
    ?line ok = wait_for_link_death(Pid1sc),

    LargeCircular = [{chunk_size, 2*BinExampleSize + 3},{is_circular, true}],
    ?line {ok, Pid1lc} =
	gen_stream:start_link(SimpleBinOpts ++ LargeCircular),
    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid1lc),
    ?line stopped = gen_stream:stop(Pid1lc),
    ?line ok = wait_for_link_death(Pid1lc),

    %% Validate x_mfa, x_fun options...
    ?line {error, {x_mfa, {not_module_plus_function_pair, 4}}} =
	gen_stream:start_link(SimpleBinOpts ++ [{x_mfa, 4}]),
    ?line {error, {x_mfa, {not_module_plus_function_pair, {?MODULE, 3}}}} =
	gen_stream:start_link(SimpleBinOpts ++ [{x_mfa, {?MODULE, 3}}]),
    ?line {ok, PidMfa} =
	gen_stream:start_link(SimpleBinOpts ++ [{x_mfa, {?MODULE, reverse}}]),
    ?line {stream_size, BinExampleSize} = gen_stream:stream_size(PidMfa),
    ?line stopped = gen_stream:stop(PidMfa),
    ?line busy_wait_for_process_to_end(PidMfa, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(PidMfa)),

    ?line {error, {x_fun, {not_fun_of_arity_1, _Fun1}}} =
	gen_stream:start_link(SimpleBinOpts ++ [{x_fun, fun() -> 4 end}]),
    ?line {error, {x_fun, {not_fun_of_arity_1, _Fun2}}} =
	gen_stream:start_link(SimpleBinOpts ++ [{x_fun, fun(X,Y) -> X*Y end}]),
    ?line {ok, PidFun} =
	gen_stream:start_link(SimpleBinOpts ++ [{x_fun, fun(X) -> X end}]),
    ?line {stream_size, BinExampleSize} = gen_stream:stream_size(PidFun),
    ?line stopped = gen_stream:stop(PidFun),
    ?line busy_wait_for_process_to_end(PidFun, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(PidFun)),

    ?line {error, {choose_only_one_option, {x_mfa, x_fun}}} =
	gen_stream:start_link(SimpleBinOpts ++ [{x_mfa, {?MODULE, stop}},
						{x_fun, fun(X) -> 2*X end}]),

    %% local register
    LocalName = {local, my_test_name},
    ?line {ok, Pid2} = gen_stream:start_link(LocalName, SimpleBinOpts),
    ?line {stream_size, BinExampleSize} =
	gen_stream:stream_size(my_test_name),
    ?line {error, {already_started, Pid2}} =
	gen_stream:start_link(LocalName, SimpleBinOpts),

    ?line stopped = gen_stream:stop(my_test_name),
    ?line ok = wait_for_link_death(Pid2),

    %% global register
    GlobalName = {global, my_test_name},
    ?line {ok, Pid3} = gen_stream:start_link(GlobalName, SimpleBinOpts),
    ?line {stream_size, BinExampleSize} =
	gen_stream:stream_size(GlobalName),
    ?line {error, {already_started, Pid3}} =
	gen_stream:start_link(GlobalName, SimpleBinOpts),

    ?line stopped = gen_stream:stop(GlobalName),
    ?line ok = wait_for_link_death(Pid3),

    test_server:messages_get(),

    %% Must wait for all error messages before going to next test.
    %% (otherwise it interferes too much with real time characteristics).
    case os:type() of
	vxworks ->
	    receive after 5000 -> ok end;
	_ ->
	    ok
    end,
    process_flag(trap_exit, OldFl),
    ok.


%%--------------------------------------
%% Check handling of bad interface calls
%%--------------------------------------
bad_calls(suite) -> [];
bad_calls(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    %% Binary data stream...
    BinExample = <<"Unused data">>,
    SimpleBinOpts = [{stream_type, {binary, BinExample}}],
    ?line {ok, Pid1} = gen_stream:start(SimpleBinOpts),

    ?line {unknown_request, foo} = gen_server:call(Pid1, foo),
    ?line ok = gen_server:cast(Pid1, bar),
    ?line baz = Pid1 ! baz,

    ?line stopped = gen_stream:stop(Pid1),
    ?line busy_wait_for_process_to_end(Pid1, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid1)),

    process_flag(trap_exit, OldFl),
    ok.

%%--------------------------------------
%% Check the stream attributes
%%--------------------------------------
stream_info_bin(suite) -> [];
stream_info_bin(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    %% Binary data stream...
    BinExample = <<"1234567890123456">>,
    BinExampleSize = byte_size(BinExample),
    SimpleBinOpts = [{stream_type, {binary, BinExample}}, {chunk_size, 4}],
    ?line {ok, Pid1} = gen_stream:start(SimpleBinOpts),

    ?line {stream_size, BinExampleSize} = gen_stream:stream_size(Pid1),
    ?line {stream_pos, 0} = gen_stream:stream_pos(Pid1),
    ?line {pct_complete, 0} = gen_stream:pct_complete(Pid1),

    ?line info_chunk(Pid1, BinExampleSize, <<"1234">>,     4,  25),
    ?line info_chunk(Pid1, BinExampleSize, <<"5678">>,     8,  50),
    ?line info_chunk(Pid1, BinExampleSize, <<"9012">>,    12,  75),
    ?line info_chunk(Pid1, BinExampleSize, <<"3456">>,    16, 100),
    ?line info_chunk(Pid1, BinExampleSize, end_of_stream, 16, 100),
    ?line info_chunk(Pid1, BinExampleSize, end_of_stream, 16, 100),

    ?line stopped = gen_stream:stop(Pid1),
    ?line busy_wait_for_process_to_end(Pid1, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid1)),

    %% Now a circular version...
    CircularOpts = SimpleBinOpts ++ [{is_circular, true}],
    ?line {ok, Pid2} = gen_stream:start(CircularOpts),

    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid2),
    ?line {stream_pos, 0} = gen_stream:stream_pos(Pid2),
    ?line {pct_complete, is_circular} = gen_stream:pct_complete(Pid2),

    ?line info_chunk(Pid2, is_circular, <<"1234">>,  4, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"5678">>,  8, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"9012">>, 12, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"3456">>, 16, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"1234">>, 20, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"5678">>, 24, is_circular),

    ?line stopped = gen_stream:stop(Pid2),
    ?line busy_wait_for_process_to_end(Pid2, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid2)),

    process_flag(trap_exit, OldFl),
    ok.

stream_info_file(suite) -> [];
stream_info_file(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    %% Binary data stream...
    FileExample = test_file_path(Config, "alpha.txt"),
    FileExampleSize = file_size(FileExample),
    SimpleFileOpts = [{stream_type, {file, FileExample}}, {chunk_size,4}],
    ?line {ok, Pid1} = gen_stream:start(SimpleFileOpts),

    ?line {stream_size, FileExampleSize} = gen_stream:stream_size(Pid1),
    ?line {stream_pos, 0} = gen_stream:stream_pos(Pid1),
    ?line {pct_complete, 0} = gen_stream:pct_complete(Pid1),

    ?line info_chunk(Pid1, FileExampleSize, <<"abcd">>,     4,  10),
    ?line info_chunk(Pid1, FileExampleSize, <<"efgh">>,     8,  21),
    ?line info_chunk(Pid1, FileExampleSize, <<"ijkl">>,    12,  31),
    ?line info_chunk(Pid1, FileExampleSize, <<"mnop">>,    16,  42),
    ?line info_chunk(Pid1, FileExampleSize, <<"qrst">>,    20,  52),
    ?line info_chunk(Pid1, FileExampleSize, <<"uvwx">>,    24,  63),
    ?line info_chunk(Pid1, FileExampleSize, <<"yz\n1">>,   28,  73),
    ?line info_chunk(Pid1, FileExampleSize, <<"2345">>,    32,  84),
    ?line info_chunk(Pid1, FileExampleSize, <<"6789">>,    36,  94),
    ?line info_chunk(Pid1, FileExampleSize, <<"0\n">>,     38, 100),
    ?line info_chunk(Pid1, FileExampleSize, end_of_stream, 38, 100),
    ?line info_chunk(Pid1, FileExampleSize, end_of_stream, 38, 100),

    ?line stopped = gen_stream:stop(Pid1),
    ?line busy_wait_for_process_to_end(Pid1, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid1)),

    %% Now a circular version...
    CircularOpts = SimpleFileOpts ++ [{is_circular, true}],
    ?line {ok, Pid2} = gen_stream:start(CircularOpts),

    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid2),
    ?line {stream_pos, 0} = gen_stream:stream_pos(Pid2),
    ?line {pct_complete, is_circular} = gen_stream:pct_complete(Pid2),

    ?line info_chunk(Pid2, is_circular, <<"abcd">>,     4, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"efgh">>,     8, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"ijkl">>,    12, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"mnop">>,    16, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"qrst">>,    20, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"uvwx">>,    24, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"yz\n1">>,   28, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"2345">>,    32, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"6789">>,    36, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"0\nab">>,   40, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"cdef">>,    44, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"ghij">>,    48, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"klmn">>,    52, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"opqr">>,    56, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"stuv">>,    60, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"wxyz">>,    64, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"\n123">>,   68, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"4567">>,    72, is_circular),
    ?line info_chunk(Pid2, is_circular, <<"890\n">>,   76, is_circular),

    ?line stopped = gen_stream:stop(Pid2),
    ?line busy_wait_for_process_to_end(Pid2, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid2)),

    process_flag(trap_exit, OldFl),
    ok.

stream_info_module(suite) -> [];
stream_info_module(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    %% Fixed length data stream...
    ModSize = 20,
    FixedModOpts = [{stream_type, {behaviour, gen_stream_odd_nums,
				   [{stream_size, ModSize}]}},
		    {chunk_size, 2*4}],
    ?line {ok, Pid1} = gen_stream:start(FixedModOpts),

    ?line {stream_size, ModSize} = gen_stream:stream_size(Pid1),
    ?line {stream_pos, 0} = gen_stream:stream_pos(Pid1),
    ?line {pct_complete, 0} = gen_stream:pct_complete(Pid1),

    ?line info_chunk(Pid1, ModSize, odd_bin([1,3]),  8,  40),
    ?line info_chunk(Pid1, ModSize, odd_bin([5,7]), 16,  80),
    ?line info_chunk(Pid1, ModSize, odd_bin([9]),   20, 100),
    ?line info_chunk(Pid1, ModSize, end_of_stream,  20, 100),
    ?line info_chunk(Pid1, ModSize, end_of_stream,  20, 100),

    ?line stopped = gen_stream:stop(Pid1),
    ?line busy_wait_for_process_to_end(Pid1, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid1)),

    %% Infinite stream...
    InfiniteModOpts = [{stream_type, {behaviour, gen_stream_odd_nums, [{}]}}, {chunk_size, 12}],
    ?line {ok, Pid2} = gen_stream:start(InfiniteModOpts),

    ?line {stream_size, infinite} = gen_stream:stream_size(Pid2),
    ?line {stream_pos, 0} = gen_stream:stream_pos(Pid2),
    ?line {pct_complete, infinite} = gen_stream:pct_complete(Pid2),

    ?line info_chunk(Pid2, infinite, odd_bin([1,3,5]),    12, infinite),
    ?line info_chunk(Pid2, infinite, odd_bin([7,9,11]),   24, infinite),
    ?line info_chunk(Pid2, infinite, odd_bin([13,15,17]), 36, infinite),
    ?line info_chunk(Pid2, infinite, odd_bin([19,21,23]), 48, infinite),
    ?line info_chunk(Pid2, infinite, odd_bin([25,27,29]), 60, infinite),

    ?line stopped = gen_stream:stop(Pid2),
    ?line busy_wait_for_process_to_end(Pid2, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid2)),

    %% Now a circular version...
    CircularOpts = FixedModOpts ++ [{is_circular, true}],
    ?line {ok, Pid3} = gen_stream:start(CircularOpts),

    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid3),
    ?line {stream_pos, 0} = gen_stream:stream_pos(Pid3),
    ?line {pct_complete, is_circular} = gen_stream:pct_complete(Pid3),

    ?line info_chunk(Pid3, is_circular, odd_bin([1,3]),  8, is_circular),
    ?line info_chunk(Pid3, is_circular, odd_bin([5,7]), 16, is_circular),
    ?line info_chunk(Pid3, is_circular, odd_bin([9,1]), 24, is_circular),
    ?line info_chunk(Pid3, is_circular, odd_bin([3,5]), 32, is_circular),
    ?line info_chunk(Pid3, is_circular, odd_bin([7,9]), 40, is_circular),
    ?line info_chunk(Pid3, is_circular, odd_bin([1,3]), 48, is_circular),

    ?line stopped = gen_stream:stop(Pid3),
    ?line busy_wait_for_process_to_end(Pid3, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid3)),

    process_flag(trap_exit, OldFl),
    ok.


%%--------------------------------------
%% Test procs, buffers and block factor
%%--------------------------------------
num_procs_bin(suite) -> [];
num_procs_bin(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    %% Binary data stream...
    BinExample = <<"1234567890123456">>,
    BinExampleSize = byte_size(BinExample),
    BinOpts = [{stream_type, {binary, BinExample}}, {chunk_size, 4}],

    %% Testing the same values for 1-4 procs...
    ?line [proc_info(N, BinExampleSize, BinOpts ++ [{num_procs, N}], undefined)
	   || N <- [1,2,3,4]],
    ?line [proc_info(N, BinExampleSize, BinOpts ++ [{num_procs, N}],
		    {?MODULE, reverse}) || N <- [1,2,3,4]],
    ?line [proc_info(N, BinExampleSize, BinOpts ++ [{num_procs, N}],
		    fun(Bin) -> {byte_size(Bin), Bin} end)
	   || N <- [1,2,3,4]],
    ?line [proc_info(N, BinExampleSize, BinOpts ++ [{num_procs, N}],
		    fun reverse/1) || N <- [1,2,3,4]],

    process_flag(trap_exit, OldFl),
    ok.


num_procs_file(suite) -> [];
num_procs_file(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    %% File data stream...
    FileExample = test_file_path(Config, "alpha.txt"),
    FileExampleSize = file_size(FileExample),
    FileOpts = [{stream_type, {file, FileExample}}, {chunk_size,4}],

    %% Testing the same values for 1-4 procs...
    ?line [proc_file_info(N, FileExampleSize, FileOpts ++ [{num_procs, N}],
			  undefined) || N <- [1,2,3,4]],
    ?line [proc_file_info(N, FileExampleSize, FileOpts ++ [{num_procs, N}],
			  {?MODULE, reverse}) || N <- [1,2,3,4]],
    ?line [proc_file_info(N, FileExampleSize, FileOpts ++ [{num_procs, N}],
			  fun(Bin) -> {byte_size(Bin), Bin} end)
	   || N <- [1,2,3,4]],
    ?line [proc_file_info(N, FileExampleSize, FileOpts ++ [{num_procs, N}],
			  fun reverse/1) || N <- [1,2,3,4]],

    process_flag(trap_exit, OldFl),
    ok.


num_procs_module(suite) -> [];
num_procs_module(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    %% Behaviour data stream...
    ModExampleSize = 64,
    ModOpts = [{stream_type, {behaviour, gen_stream_odd_nums,
			      [{stream_size, ModExampleSize}]}},
	       {chunk_size, 4*4}],

    %% Testing the same values for 1-4 procs...
    ?line [proc_mod_info(N, ModExampleSize, ModOpts ++ [{num_procs, N}],
			 undefined) || N <- [1,2,3,4]],
    ?line [proc_mod_info(N, ModExampleSize, ModOpts ++ [{num_procs, N}],
			 {?MODULE, reverse}) || N <- [1,2,3,4]],
    ?line [proc_mod_info(N, ModExampleSize, ModOpts ++ [{num_procs, N}],
			 fun(Bin) -> {byte_size(Bin), Bin} end)
	   || N <- [1,2,3,4]],
    ?line [proc_mod_info(N, ModExampleSize, ModOpts ++ [{num_procs, N}],
			 fun reverse/1) || N <- [1,2,3,4]],

    process_flag(trap_exit, OldFl),
    ok.


num_buffers_bin(suite) -> [];
num_buffers_bin(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    %% Binary data stream...
    BinExample = list_to_binary(lists:duplicate(6,<<"1234567890">>)),
    BinExampleSize = byte_size(BinExample),
    BinOpts = [{stream_type, {binary,BinExample}}, {chunk_size,5}],

    %% Testing the same values for 1-3 procs + 2-4 buffers...
    ?line [buffer_info(N, BinExampleSize,
		       BinOpts ++ [{num_procs, N}, {num_chunks, N+1}],
		       undefined) || N <- [1,2,3]],
    ?line [buffer_info(N, BinExampleSize,
		       BinOpts ++ [{num_procs, N}, {num_chunks, N+1}],
		       {?MODULE, reverse}) || N <- [1,2,3]],
    ?line [buffer_info(N, BinExampleSize,
		       BinOpts ++ [{num_procs, N}, {num_chunks, N+1}],
		       fun(Bin) -> {byte_size(Bin), Bin} end)
	   || N <- [1,2,3]],
    ?line [buffer_info(N, BinExampleSize,
		       BinOpts ++ [{num_procs, N}, {num_chunks, N+1}],
		       fun reverse/1) || N <- [1,2,3]],

    process_flag(trap_exit, OldFl),
    ok.


num_buffers_file(suite) -> [];
num_buffers_file(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    %% File data stream...
    FileExample = test_file_path(Config, "alpha.txt"),
    FileExampleSize = file_size(FileExample),
    FileOpts = [{stream_type, {file, FileExample}}, {chunk_size,5}],

    %% Testing the same values for 1-3 procs + 2-4 buffers...
    ?line [buffer_file_info(N, FileExampleSize,
			    FileOpts ++ [{num_procs,N},{num_chunks,N+1}],
			   undefined) || N <- [1,2,3]],
    ?line [buffer_file_info(N, FileExampleSize,
			    FileOpts ++ [{num_procs,N},{num_chunks,N+1}],
			    {?MODULE, reverse}) || N <- [1,2,3]],
    ?line [buffer_file_info(N, FileExampleSize,
			    FileOpts ++ [{num_procs,N},{num_chunks,N+1}],
			    fun(Bin) -> {byte_size(Bin), Bin} end)
	   || N <- [1,2,3]],
    ?line [buffer_file_info(N, FileExampleSize,
			    FileOpts ++ [{num_procs,N},{num_chunks,N+1}],
			    fun reverse/1) || N <- [1,2,3]],

    process_flag(trap_exit, OldFl),
    ok.


num_buffers_module(suite) -> [];
num_buffers_module(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    %% Binary data stream...
    ModExampleSize = 240,
    ModOpts = [{stream_type, {behaviour, gen_stream_odd_nums,
			      [{stream_size, ModExampleSize}]}},
	       {chunk_size, 5*4}],

    %% Testing the same values for 1-3 procs + 2-4 buffers...
    ?line [buffer_mod_info(N, ModExampleSize,
		       ModOpts ++ [{num_procs, N}, {num_chunks, N+1}],
		       undefined) || N <- [1,2,3]],
    ?line [buffer_mod_info(N, ModExampleSize,
		       ModOpts ++ [{num_procs, N}, {num_chunks, N+1}],
		       {?MODULE, reverse}) || N <- [1,2,3]],
    ?line [buffer_mod_info(N, ModExampleSize,
		       ModOpts ++ [{num_procs, N}, {num_chunks, N+1}],
		       fun(Bin) -> {byte_size(Bin), Bin} end)
	   || N <- [1,2,3]],
    ?line [buffer_mod_info(N, ModExampleSize,
		       ModOpts ++ [{num_procs, N}, {num_chunks, N+1}],
		       fun reverse/1) || N <- [1,2,3]],

    process_flag(trap_exit, OldFl),
    ok.


block_buffers_bin(suite) -> [];
block_buffers_bin(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    %% Binary data stream...
    BinExample = list_to_binary(lists:duplicate(6,<<"1234567890">>)),
    BinExampleSize = byte_size(BinExample),
    BinOpts = [{stream_type, {binary,BinExample}}, {chunk_size,5}],

    %% Testing the same values for 1-3 procs + 2-4 buffers...
    ?line [block_buffer_info(N, 5*B, BinExampleSize, BinExample,
			     BinOpts ++ [{num_procs,N},{num_chunks,N+1},
					 {block_factor,B}], undefined)
	   || {N,B} <- [{1,2},{2,5},{3,3}]],
    ?line [block_buffer_info(N, 5*B, BinExampleSize, BinExample,
			     BinOpts ++ [{num_procs,N},{num_chunks,N+1},
					 {block_factor,B}],
			    {?MODULE, reverse})
	   || {N,B} <- [{1,2},{2,5},{3,3}]],
    ?line [block_buffer_info(N, 5*B, BinExampleSize, BinExample,
			     BinOpts ++ [{num_procs,N},{num_chunks,N+1},
					 {block_factor,B}],
			     fun(Bin) -> {size(Bin), Bin} end)
	   || {N,B} <- [{1,2},{2,5},{3,3}]],
    ?line [block_buffer_info(N, 5*B, BinExampleSize, BinExample,
			     BinOpts ++ [{num_procs,N},{num_chunks,N+1},
					 {block_factor,B}],
			     fun reverse/1)
	   || {N,B} <- [{1,2},{2,5},{3,3}]],

    BinOdd = list_to_binary(lists:duplicate(7,<<"12345678901">>)),
    block_buffer_info(3, 4*3, byte_size(BinOdd), BinOdd,
		      [{stream_type, {binary, BinOdd}}, {chunk_size, 4},
		       {num_procs,3},{num_chunks,4},{block_factor,3}],
		     undefined),
    block_buffer_info(3, 4*3, byte_size(BinOdd), BinOdd,
		      [{stream_type, {binary, BinOdd}}, {chunk_size, 4},
		       {num_procs,3},{num_chunks,4},{block_factor,3}],
		     {?MODULE, reverse}),
    block_buffer_info(3, 4*3, byte_size(BinOdd), BinOdd,
		      [{stream_type, {binary, BinOdd}}, {chunk_size, 4},
		       {num_procs,3},{num_chunks,4},{block_factor,3}],
		     fun(Bin) -> {size(Bin), Bin} end),
    block_buffer_info(3, 4*3, byte_size(BinOdd), BinOdd,
		      [{stream_type, {binary, BinOdd}}, {chunk_size, 4},
		       {num_procs,3},{num_chunks,4},{block_factor,3}],
		     fun reverse/1),

    process_flag(trap_exit, OldFl),
    ok.


block_buffers_file(suite) -> [];
block_buffers_file(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    %% File data stream...
    FilePath = test_file_path(Config, "alpha.txt"),
    {ok, FileExample} = file:read_file(FilePath),
    FileExampleSize = file_size(FilePath),
    FileOpts = [{stream_type, {file, FilePath}}, {chunk_size,5}],

    %% Testing the same values for 1-3 procs + 2-4 buffers...
    ?line [block_buffer_info(N, 5*B, FileExampleSize, FileExample,
			     FileOpts ++ [{num_procs,N},{num_chunks,N+1},
					  {block_factor,B}], undefined)
	   || {N,B} <- [{1,2},{2,5},{3,3}]],
    ?line [block_buffer_info(N, 5*B, FileExampleSize, FileExample,
			     FileOpts ++ [{num_procs,N},{num_chunks,N+1},
					  {block_factor,B}],
			     {?MODULE, reverse})
	   || {N,B} <- [{1,2},{2,5},{3,3}]],
    ?line [block_buffer_info(N, 5*B, FileExampleSize, FileExample,
			     FileOpts ++ [{num_procs,N},{num_chunks,N+1},
					  {block_factor,B}],
			     fun(Bin) -> {byte_size(Bin), Bin} end)
	   || {N,B} <- [{1,2},{2,5},{3,3}]],
    ?line [block_buffer_info(N, 5*B, FileExampleSize, FileExample,
			     FileOpts ++ [{num_procs,N},{num_chunks,N+1},
					  {block_factor,B}],
			     fun reverse/1)
	   || {N,B} <- [{1,2},{2,5},{3,3}]],

    process_flag(trap_exit, OldFl),
    ok.

block_buffers_module(suite) -> [];
block_buffers_module(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    %% Behaviour data stream...
    ModBin = odd_bin([N || N <- lists:seq(1,120), N rem 2 =:= 1]),
    ModSize = 240,
    ModOpts = [{stream_type, {behaviour,gen_stream_odd_nums,[{stream_size,ModSize}]}}, {chunk_size,20}],

    %% Testing the same values for 1-3 procs + 2-4 buffers...
    ?line [block_buffer_info(N, 20*B, ModSize, ModBin,
			     ModOpts ++ [{num_procs,N},{num_chunks,N+1},
					 {block_factor,B}], undefined)
	   || {N,B} <- [{1,2},{2,5},{3,3}]],
    ?line [block_buffer_info(N, 20*B, ModSize, ModBin,
			     ModOpts ++ [{num_procs,N},{num_chunks,N+1},
					 {block_factor,B}],
			    {?MODULE, reverse})
	   || {N,B} <- [{1,2},{2,5},{3,3}]],
    ?line [block_buffer_info(N, 20*B, ModSize, ModBin,
			     ModOpts ++ [{num_procs,N},{num_chunks,N+1},
					 {block_factor,B}],
			     fun(Bin) -> {size(Bin), Bin} end)
	   || {N,B} <- [{1,2},{2,5},{3,3}]],
    ?line [block_buffer_info(N, 20*B, ModSize, ModBin,
			     ModOpts ++ [{num_procs,N},{num_chunks,N+1},
					 {block_factor,B}],
			     fun reverse/1)
	   || {N,B} <- [{1,2},{2,5},{3,3}]],

    BinOdd = odd_bin([N || N <- lists:seq(1, 77*2), N rem 2 =:= 1]),
    ModOddSize = 77 * 4,
    block_buffer_info(3, 16*3, ModOddSize, BinOdd,
		      [{stream_type, {behaviour, gen_stream_odd_nums, [{stream_size,ModOddSize}]}}, {chunk_size, 16},
		       {num_procs,3},{num_chunks,4},{block_factor,3}],
		      undefined),
    block_buffer_info(3, 16*3, ModOddSize, BinOdd,
		      [{stream_type, {behaviour, gen_stream_odd_nums, [{stream_size,ModOddSize}]}}, {chunk_size, 16},
		       {num_procs,3},{num_chunks,4},{block_factor,3}],
		      {?MODULE, reverse}),
    block_buffer_info(3, 16*3, ModOddSize, BinOdd,
		      [{stream_type, {behaviour, gen_stream_odd_nums, [{stream_size,ModOddSize}]}}, {chunk_size, 16},
		       {num_procs,3},{num_chunks,4},{block_factor,3}],
		      fun(Bin) -> {size(Bin), Bin} end),
    block_buffer_info(3, 16*3, ModOddSize, BinOdd,
		      [{stream_type, {behaviour, gen_stream_odd_nums, [{stream_size,ModOddSize}]}}, {chunk_size, 16},
		       {num_procs,3},{num_chunks,4},{block_factor,3}],
		      fun reverse/1),

    process_flag(trap_exit, OldFl),
    ok.


%%--------------------------------------
%% Test circular data stream
%%--------------------------------------

%% Circular with Binary multiple of chunk_size...
circular_mult_bin(suite) -> [];
circular_mult_bin(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    BinSmall6 = <<"123456789012345678">>,
    BinOpts3a = [{stream_type, {binary,BinSmall6}}, {chunk_size,3},
		 {is_circular, true}],
    BinOpts3b = [{stream_type, {binary,BinSmall6}}, {chunk_size,3},
		 {num_procs,2}, {is_circular, true}],
    BinOpts3c = [{stream_type, {binary,BinSmall6}}, {chunk_size,1},
		 {num_procs,2}, {block_factor,3}, {is_circular, true}],

    [circ3_info(Opts, undefined)
     || Opts <- [BinOpts3a, BinOpts3b, BinOpts3c]],
    [circ3_info(Opts, {?MODULE, reverse})
     || Opts <- [BinOpts3a, BinOpts3b, BinOpts3c]],
    [circ3_info(Opts, fun(Bin) -> {byte_size(Bin), Bin} end)
     || Opts <- [BinOpts3a, BinOpts3b, BinOpts3c]],
    [circ3_info(Opts, fun reverse/1)
     || Opts <- [BinOpts3a, BinOpts3b, BinOpts3c]],

    process_flag(trap_exit, OldFl),
    ok.

%% Circular with non-multiple chunk_size...
circular_non_mult_bin(suite) -> [];
circular_non_mult_bin(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    BinSmall5 = <<"12345">>,
    BinOpts4a = [{stream_type, {binary,BinSmall5}}, {chunk_size,4},
		 {is_circular, true}],
    BinOpts4b = [{stream_type, {binary,BinSmall5}}, {chunk_size,4},
		 {num_procs,2}, {is_circular, true}],
    BinOpts4c = [{stream_type, {binary,BinSmall5}}, {chunk_size,2},
		 {num_procs,3}, {block_factor,2}, {is_circular, true}],

    [circ4_info(Opts, undefined)
     || Opts <- [BinOpts4a, BinOpts4b, BinOpts4c]],
    [circ4_info(Opts, {?MODULE, reverse})
     || Opts <- [BinOpts4a, BinOpts4b, BinOpts4c]],
    [circ4_info(Opts, fun(Bin) -> {byte_size(Bin), Bin} end)
     || Opts <- [BinOpts4a, BinOpts4b, BinOpts4c]],
    [circ4_info(Opts, fun reverse/1)
     || Opts <- [BinOpts4a, BinOpts4b, BinOpts4c]],

    process_flag(trap_exit, OldFl),
    ok.

%% Circular with chunk_size larger than Binary...
circular_replicated_bin(suite) -> [];
circular_replicated_bin(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    BinSmall5 = <<"12345">>,
    BinOpts8a = [{stream_type, {binary,BinSmall5}}, {chunk_size,8},
		 {is_circular, true}],
    BinOpts8b = [{stream_type, {binary,BinSmall5}}, {chunk_size,2},
		 {num_procs,2}, {block_factor,4}, {is_circular, true}],
    BinOpts8c = [{stream_type, {binary,BinSmall5}}, {chunk_size,4},
		 {num_procs,3}, {block_factor,2}, {is_circular, true}],

    [circ8_info(Opts, undefined)
     || Opts <- [BinOpts8a, BinOpts8b, BinOpts8c]],
    [circ8_info(Opts, {?MODULE, reverse})
     || Opts <- [BinOpts8a, BinOpts8b, BinOpts8c]],
    [circ8_info(Opts, fun(Bin) -> {byte_size(Bin), Bin} end)
     || Opts <- [BinOpts8a, BinOpts8b, BinOpts8c]],
    [circ8_info(Opts, fun reverse/1)
     || Opts <- [BinOpts8a, BinOpts8b, BinOpts8c]],

    process_flag(trap_exit, OldFl),
    ok.

%%--------------------------------------
%% Test file streams.
%%--------------------------------------

%% Circular with File multiple of chunk_size...
circular_mult_file(suite) -> [];
circular_mult_file(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    FilePath = test_file_path(Config, "alpha.txt"),
    FileOpts19a = [{stream_type, {file, FilePath}}, {chunk_size, 19},
		   {is_circular, true}],
    FileOpts19b = [{stream_type, {file, FilePath}}, {chunk_size,19},
		   {num_procs,2}, {is_circular, true}],
    FileOpts19c = [{stream_type, {file, FilePath}}, {chunk_size,1},
		   {num_procs,2}, {block_factor,19}, {is_circular, true}],

    [circ_f19_info(Opts, undefined)
     || Opts <- [FileOpts19a, FileOpts19b, FileOpts19c]],
    [circ_f19_info(Opts, {?MODULE, reverse})
     || Opts <- [FileOpts19a, FileOpts19b, FileOpts19c]],
    [circ_f19_info(Opts, fun(Bin) -> {byte_size(Bin), Bin} end)
     || Opts <- [FileOpts19a, FileOpts19b, FileOpts19c]],
    [circ_f19_info(Opts, fun reverse/1)
     || Opts <- [FileOpts19a, FileOpts19b, FileOpts19c]],

    process_flag(trap_exit, OldFl),
    ok.

%% Circular with non-multiple chunk_size...
circular_non_mult_file(suite) -> [];
circular_non_mult_file(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    FilePath = test_file_path(Config, "alpha.txt"),
    BinOpts14a = [{stream_type, {file, FilePath}}, {chunk_size,14},
		  {is_circular, true}],
    BinOpts14b = [{stream_type, {file, FilePath}}, {chunk_size,14},
		  {num_procs,2}, {is_circular, true}],
    BinOpts14c = [{stream_type, {file, FilePath}}, {chunk_size,2},
		  {num_procs,3}, {block_factor,7}, {is_circular, true}],

    [circ_f14_info(Opts, undefined)
     || Opts <- [BinOpts14a, BinOpts14b, BinOpts14c]],
    [circ_f14_info(Opts, {?MODULE, reverse})
     || Opts <- [BinOpts14a, BinOpts14b, BinOpts14c]],
    [circ_f14_info(Opts, fun(Bin) -> {byte_size(Bin), Bin} end)
     || Opts <- [BinOpts14a, BinOpts14b, BinOpts14c]],
    [circ_f14_info(Opts, fun reverse/1)
     || Opts <- [BinOpts14a, BinOpts14b, BinOpts14c]],

    process_flag(trap_exit, OldFl),
    ok.

%% Circular with chunk_size larger than Binary...
circular_replicated_file(suite) -> [];
circular_replicated_file(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    FilePath = test_file_path(Config, "alpha.txt"),
    BinOpts60a = [{stream_type, {file, FilePath}}, {chunk_size,60},
		  {is_circular, true}],
    BinOpts60b = [{stream_type, {file, FilePath}}, {chunk_size,15},
		  {num_procs,2}, {block_factor,4}, {is_circular, true}],
    BinOpts60c = [{stream_type, {file, FilePath}}, {chunk_size,6},
		  {num_procs,3}, {block_factor,10}, {is_circular, true}],

    [circ_f60_info(Opts, undefined)
     || Opts <- [BinOpts60a, BinOpts60b, BinOpts60c]],
    [circ_f60_info(Opts, {?MODULE, reverse})
     || Opts <- [BinOpts60a, BinOpts60b, BinOpts60c]],
    [circ_f60_info(Opts, fun(Bin) -> {byte_size(Bin), Bin} end)
     || Opts <- [BinOpts60a, BinOpts60b, BinOpts60c]],
    [circ_f60_info(Opts, fun reverse/1)
     || Opts <- [BinOpts60a, BinOpts60b, BinOpts60c]],

    process_flag(trap_exit, OldFl),
    ok.


%%--------------------------------------
%% Test behaviour streams.
%%--------------------------------------

%% Circular with Module multiple of chunk_size...
circular_mult_module(suite) -> [];
circular_mult_module(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    BinSize = 18*4,
    BinOpts3a = [{stream_type, {behaviour, gen_stream_odd_nums,
				[{stream_size, BinSize}]}},
		 {chunk_size,3*4}, {is_circular, true}],
    BinOpts3b = [{stream_type, {behaviour, gen_stream_odd_nums,
				[{stream_size, BinSize}]}},
		 {chunk_size,3*4}, {num_procs,2}, {is_circular, true}],
    BinOpts3c = [{stream_type, {behaviour, gen_stream_odd_nums,
				[{stream_size, BinSize}]}},
		 {chunk_size,1*4}, {num_procs,2}, {block_factor,3},
		 {is_circular, true}],

    [circ3m_info(Opts, undefined)
     || Opts <- [BinOpts3a, BinOpts3b, BinOpts3c]],
    [circ3m_info(Opts, {?MODULE, reverse})
     || Opts <- [BinOpts3a, BinOpts3b, BinOpts3c]],
    [circ3m_info(Opts, fun(Bin) -> {byte_size(Bin), Bin} end)
     || Opts <- [BinOpts3a, BinOpts3b, BinOpts3c]],
    [circ3m_info(Opts, fun reverse/1)
     || Opts <- [BinOpts3a, BinOpts3b, BinOpts3c]],

    process_flag(trap_exit, OldFl),
    ok.

%% Circular with non-multiple chunk_size...
circular_non_mult_module(suite) -> [];
circular_non_mult_module(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    BinSize = 5*4,
    BinOpts4a = [{stream_type, {behaviour, gen_stream_odd_nums,
				[{stream_size, BinSize}]}},
		 {chunk_size,4*4}, {is_circular, true}],
    BinOpts4b = [{stream_type, {behaviour, gen_stream_odd_nums,
			       [{stream_size, BinSize}]}},
		 {chunk_size,4*4}, {num_procs,2}, {is_circular, true}],
    BinOpts4c = [{stream_type, {behaviour, gen_stream_odd_nums,
			       [{stream_size, BinSize}]}},
		 {chunk_size,2*4}, {num_procs,3}, {block_factor,2},
		 {is_circular, true}],

    [circ4m_info(Opts, undefined)
     || Opts <- [BinOpts4a, BinOpts4b, BinOpts4c]],
    [circ4m_info(Opts, {?MODULE, reverse})
     || Opts <- [BinOpts4a, BinOpts4b, BinOpts4c]],
    [circ4m_info(Opts, fun(Bin) -> {byte_size(Bin), Bin} end)
     || Opts <- [BinOpts4a, BinOpts4b, BinOpts4c]],
    [circ4m_info(Opts, fun reverse/1)
     || Opts <- [BinOpts4a, BinOpts4b, BinOpts4c]],

    process_flag(trap_exit, OldFl),
    ok.

%% Circular with chunk_size larger than Binary...
circular_replicated_module(suite) -> [];
circular_replicated_module(Config) when is_list(Config) ->
    OldFl = process_flag(trap_exit, true),
    ?line shell_bug_if_procs_leaked(),

    BinSize = 5*4,
    BinOpts8a = [{stream_type, {behaviour, gen_stream_odd_nums,
			       [{stream_size, BinSize}]}},
		 {chunk_size,8*4}, {is_circular, true}],
    BinOpts8b = [{stream_type, {behaviour, gen_stream_odd_nums,
			       [{stream_size, BinSize}]}},
		 {chunk_size,2*4}, {num_procs,2}, {block_factor,4},
		 {is_circular, true}],
    BinOpts8c = [{stream_type, {behaviour, gen_stream_odd_nums,
			       [{stream_size, BinSize}]}},
		 {chunk_size,4*4}, {num_procs,3}, {block_factor,2},
		 {is_circular, true}],

    [circ8m_info(Opts, undefined)
     || Opts <- [BinOpts8a, BinOpts8b, BinOpts8c]],
    [circ8m_info(Opts, {?MODULE, reverse})
     || Opts <- [BinOpts8a, BinOpts8b, BinOpts8c]],
    [circ8m_info(Opts, fun(Bin) -> {byte_size(Bin), Bin} end)
     || Opts <- [BinOpts8a, BinOpts8b, BinOpts8c]],
    [circ8m_info(Opts, fun reverse/1)
     || Opts <- [BinOpts8a, BinOpts8b, BinOpts8c]],

    process_flag(trap_exit, OldFl),
    ?line shell_bug_if_procs_leaked(),
    ok.


%%%--------------------------------------------------------
%% Internal wait for pid to end funs
%%%--------------------------------------------------------
busy_wait_for_process_to_end(_Pid, 0) ->
    still_alive;
busy_wait_for_process_to_end(Pid, N) ->
    case erlang:is_process_alive(Pid) of
	true ->
	    receive
	    after 100 ->
		    ok
	    end,
	    busy_wait_for_process_to_end(Pid, N-1);
	_ ->
	    ok
    end.

wait_for_link_death(Pid) ->
    receive
	{'EXIT', Pid, normal} ->
	    ok
    after 5000 ->
	    test_server:fail(not_stopped)
    end.

%%%--------------------------------------------------------
%% Internal test API functions
%%%--------------------------------------------------------
create_opts(NormalOpts, undefined) ->
    NormalOpts;
create_opts(NormalOpts, {_Mod, _Fun} = Xfn) ->
    NormalOpts ++ [{x_mfa, Xfn}];
create_opts(NormalOpts, Xfn) ->
    NormalOpts ++ [{x_fun, Xfn}].

info_chunk(Pid, ExpSize, ExpBin, ExpPos, ExpPct) ->
    info_chunk(Pid, ExpSize, ExpBin, ExpPos, ExpPct, undefined).

info_chunk(Pid, ExpSize, end_of_stream, ExpPos, ExpPct, _Xfn) ->
    {next_block, end_of_stream} = gen_stream:next_block(Pid),
    info_stream(Pid, ExpSize, ExpPos, ExpPct);
info_chunk(Pid, ExpSize, ExpBin, ExpPos, ExpPct, Xfn) ->
    ExpResult = case Xfn of
		    undefined ->  {next_block, ExpBin};
		    {Mod, Fun} -> {next_block, Mod:Fun(ExpBin)};
		    Fun ->        {next_block, Fun(ExpBin)}
		end,
    ExpResult = gen_stream:next_block(Pid),
    info_stream(Pid, ExpSize, ExpPos, ExpPct).

info_stream(Pid, ExpSize, ExpPos, ExpPct) ->
    {stream_size, ExpSize} = gen_stream:stream_size(Pid),
    {stream_pos, ExpPos} =   gen_stream:stream_pos(Pid),
    {pct_complete, ExpPct} = gen_stream:pct_complete(Pid).

proc_info(NumProcs, BinSize, BinOpts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(BinOpts, Xfn)),
    ?line {stream_size, BinSize} = gen_stream:stream_size(Pid),
    ?line {stream_pos, 0} = gen_stream:stream_pos(Pid),
    ?line {pct_complete, 0} = gen_stream:pct_complete(Pid),

    ?line info_chunk(Pid, BinSize, <<"1234">>,     4,  25, Xfn),
    WorkerProcs = get_linked_procs(Pid),
    ?line NumProcs = length(WorkerProcs),
    ?line info_chunk(Pid, BinSize, <<"5678">>,     8,  50, Xfn),
    ?line info_chunk(Pid, BinSize, <<"9012">>,    12,  75, Xfn),
    ?line info_chunk(Pid, BinSize, <<"3456">>,    16, 100, Xfn),
    ?line info_chunk(Pid, BinSize, end_of_stream, 16, 100, Xfn),
    ?line info_chunk(Pid, BinSize, end_of_stream, 16, 100, Xfn),

    ?line check_procs_dead(WorkerProcs),
    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).

proc_file_info(NumProcs, FileSize, FileOpts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(FileOpts, Xfn)),
    ?line {stream_size, FileSize} = gen_stream:stream_size(Pid),
    ?line {stream_pos, 0} = gen_stream:stream_pos(Pid),
    ?line {pct_complete, 0} = gen_stream:pct_complete(Pid),

    ?line info_chunk(Pid, FileSize, <<"abcd">>,     4,  10, Xfn),
    WorkerProcs = get_linked_procs(Pid),
    ?line NumProcs = length(WorkerProcs),
    ?line info_chunk(Pid, FileSize, <<"efgh">>,     8,  21, Xfn),
    ?line info_chunk(Pid, FileSize, <<"ijkl">>,    12,  31, Xfn),
    ?line info_chunk(Pid, FileSize, <<"mnop">>,    16,  42, Xfn),
    ?line info_chunk(Pid, FileSize, <<"qrst">>,    20,  52, Xfn),
    ?line info_chunk(Pid, FileSize, <<"uvwx">>,    24,  63, Xfn),
    ?line info_chunk(Pid, FileSize, <<"yz\n1">>,   28,  73, Xfn),
    ?line info_chunk(Pid, FileSize, <<"2345">>,    32,  84, Xfn),
    ?line info_chunk(Pid, FileSize, <<"6789">>,    36,  94, Xfn),
    ?line info_chunk(Pid, FileSize, <<"0\n">>,     38, 100, Xfn),
    ?line info_chunk(Pid, FileSize, end_of_stream, 38, 100, Xfn),
    ?line info_chunk(Pid, FileSize, end_of_stream, 38, 100, Xfn),

    ?line check_procs_dead(WorkerProcs),
    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).

proc_mod_info(NumProcs, ModSize, ModOpts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(ModOpts, Xfn)),
    ?line {stream_size, ModSize} = gen_stream:stream_size(Pid),
    ?line {stream_pos, 0} = gen_stream:stream_pos(Pid),
    ?line {pct_complete, 0} = gen_stream:pct_complete(Pid),

    ?line info_chunk(Pid, ModSize, odd_bin([1,3,5,7]),     16,  25, Xfn),
    WorkerProcs = get_linked_procs(Pid),
    ?line NumProcs = length(WorkerProcs),
    ?line info_chunk(Pid, ModSize, odd_bin([9,11,13,15]),  32,  50, Xfn),
    ?line info_chunk(Pid, ModSize, odd_bin([17,19,21,23]), 48,  75, Xfn),
    ?line info_chunk(Pid, ModSize, odd_bin([25,27,29,31]), 64, 100, Xfn),
    ?line info_chunk(Pid, ModSize, end_of_stream, 64, 100, Xfn),
    ?line info_chunk(Pid, ModSize, end_of_stream, 64, 100, Xfn),

    ?line check_procs_dead(WorkerProcs),
    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).

buffer_info(NumProcs, BinSize, BinOpts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(BinOpts, Xfn)),
    ?line {stream_size, BinSize} = gen_stream:stream_size(Pid),
    ?line {stream_pos, 0} = gen_stream:stream_pos(Pid),
    ?line {pct_complete, 0} = gen_stream:pct_complete(Pid),

    ?line info_chunk(Pid, BinSize, <<"12345">>,     5,   8, Xfn),
    WorkerProcs = get_linked_procs(Pid),
    ?line NumProcs = length(WorkerProcs),
    ?line info_chunk(Pid, BinSize, <<"67890">>,    10,  16, Xfn),
    ?line info_chunk(Pid, BinSize, <<"12345">>,    15,  25, Xfn),
    ?line info_chunk(Pid, BinSize, <<"67890">>,    20,  33, Xfn),
    ?line info_chunk(Pid, BinSize, <<"12345">>,    25,  41, Xfn),
    ?line info_chunk(Pid, BinSize, <<"67890">>,    30,  50, Xfn),
    ?line info_chunk(Pid, BinSize, <<"12345">>,    35,  58, Xfn),
    ?line info_chunk(Pid, BinSize, <<"67890">>,    40,  66, Xfn),
    ?line info_chunk(Pid, BinSize, <<"12345">>,    45,  75, Xfn),
    ?line info_chunk(Pid, BinSize, <<"67890">>,    50,  83, Xfn),
    ?line info_chunk(Pid, BinSize, <<"12345">>,    55,  91, Xfn),
    ?line info_chunk(Pid, BinSize, <<"67890">>,    60, 100, Xfn),
    ?line info_chunk(Pid, BinSize, end_of_stream,  60, 100, Xfn),
    ?line info_chunk(Pid, BinSize, end_of_stream,  60, 100, Xfn),

    ?line check_procs_dead(WorkerProcs),
    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).

buffer_file_info(NumProcs, FileSize, FileOpts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(FileOpts, Xfn)),
    ?line {stream_size, FileSize} = gen_stream:stream_size(Pid),
    ?line {stream_pos, 0} = gen_stream:stream_pos(Pid),
    ?line {pct_complete, 0} = gen_stream:pct_complete(Pid),

    ?line info_chunk(Pid, FileSize, <<"abcde">>,    5,  13, Xfn),
    WorkerProcs = get_linked_procs(Pid),
    ?line NumProcs = length(WorkerProcs),
    ?line info_chunk(Pid, FileSize, <<"fghij">>,   10,  26, Xfn),
    ?line info_chunk(Pid, FileSize, <<"klmno">>,   15,  39, Xfn),
    ?line info_chunk(Pid, FileSize, <<"pqrst">>,   20,  52, Xfn),
    ?line info_chunk(Pid, FileSize, <<"uvwxy">>,   25,  65, Xfn),
    ?line info_chunk(Pid, FileSize, <<"z\n123">>,  30,  78, Xfn),
    ?line info_chunk(Pid, FileSize, <<"45678">>,   35,  92, Xfn),
    ?line info_chunk(Pid, FileSize, <<"90\n">>,    38, 100, Xfn),
    ?line info_chunk(Pid, FileSize, end_of_stream, 38, 100, Xfn),
    ?line info_chunk(Pid, FileSize, end_of_stream, 38, 100, Xfn),

    ?line check_procs_dead(WorkerProcs),
    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).

buffer_mod_info(NumProcs, ModSize, ModOpts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(ModOpts, Xfn)),
    ?line {stream_size, ModSize} = gen_stream:stream_size(Pid),
    ?line {stream_pos, 0} = gen_stream:stream_pos(Pid),
    ?line {pct_complete, 0} = gen_stream:pct_complete(Pid),

    ?line info_chunk(Pid, ModSize, odd_bin([1,3,5,7,9]),      20,   8, Xfn),
    WorkerProcs = get_linked_procs(Pid),
    ?line NumProcs = length(WorkerProcs),
    ?line info_chunk(Pid, ModSize, odd_bin([11,13,15,17,19]), 40,  16, Xfn),
    ?line info_chunk(Pid, ModSize, odd_bin([21,23,25,27,29]), 60,  25, Xfn),
    ?line info_chunk(Pid, ModSize, odd_bin([31,33,35,37,39]), 80,  33, Xfn),
    ?line info_chunk(Pid, ModSize, odd_bin([41,43,45,47,49]), 100,  41, Xfn),
    ?line info_chunk(Pid, ModSize, odd_bin([51,53,55,57,59]), 120,  50, Xfn),
    ?line info_chunk(Pid, ModSize, odd_bin([61,63,65,67,69]), 140,  58, Xfn),
    ?line info_chunk(Pid, ModSize, odd_bin([71,73,75,77,79]), 160,  66, Xfn),
    ?line info_chunk(Pid, ModSize, odd_bin([81,83,85,87,89]), 180,  75, Xfn),
    ?line info_chunk(Pid, ModSize, odd_bin([91,93,95,97,99]), 200,  83, Xfn),
    ?line info_chunk(Pid, ModSize, odd_bin([101,103,105,107,109]), 220,  91, Xfn),
    ?line info_chunk(Pid, ModSize, odd_bin([111,113,115,117,119]), 240, 100, Xfn),
    ?line info_chunk(Pid, ModSize, end_of_stream, 240, 100, Xfn),
    ?line info_chunk(Pid, ModSize, end_of_stream, 240, 100, Xfn),

    ?line check_procs_dead(WorkerProcs),
    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).

block_buffer_info(NumProcs, ChunkBlockSize, BinSize, Bin, BinOpts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(BinOpts, Xfn)),
    ?line {stream_size, BinSize} = gen_stream:stream_size(Pid),
    ?line {stream_pos, 0} = gen_stream:stream_pos(Pid),
    ?line {pct_complete, 0} = gen_stream:pct_complete(Pid),
    WorkerProcs = get_linked_procs(Pid),
    ?line NumProcs = length(WorkerProcs),

    FullBlocks = BinSize div ChunkBlockSize,
    BlockBinSize = FullBlocks * ChunkBlockSize,
    ?line <<BlockBin:BlockBinSize/binary, Rest/binary>> = Bin,
    ?line ExpBlocks = [ <<B/binary>>
			    || <<B:ChunkBlockSize/binary>> <= BlockBin ],
    ?line ExpData = make_block_triplet(ExpBlocks, [], 0, BinSize),
    ?line [info_chunk(Pid, BinSize, ExpChunk, ExpPos, ExpPct, Xfn)
	   || {ExpChunk, ExpPos, ExpPct} <- ExpData],
    case Rest of
	<<>> -> ok;
	Tail ->
	    ?line info_chunk(Pid, BinSize, Tail, BinSize, 100, Xfn)
    end,
    ?line info_chunk(Pid, BinSize, end_of_stream, BinSize, 100, Xfn),
    ?line info_chunk(Pid, BinSize, end_of_stream, BinSize, 100, Xfn),

    ?line check_procs_dead(WorkerProcs),
    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).

make_block_triplet([], Triplets, _Pos, _TotSize) ->
    lists:reverse(Triplets);
make_block_triplet([Bin | More], Triplets, Pos, TotSize) ->
    NewPos = Pos + byte_size(Bin),
    NewTriplet = {Bin, NewPos, NewPos * 100 div TotSize},
    make_block_triplet(More, [NewTriplet | Triplets], NewPos, TotSize).

circ3_info(Opts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(Opts, Xfn)),
    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid),
    ?line {pct_complete, is_circular} = gen_stream:pct_complete(Pid),
    ?line circular_info(Pid,  0, <<"123">>, Xfn),
    ?line circular_info(Pid,  3, <<"456">>, Xfn),
    ?line circular_info(Pid,  6, <<"789">>, Xfn),
    ?line circular_info(Pid,  9, <<"012">>, Xfn),
    ?line circular_info(Pid, 12, <<"345">>, Xfn),
    ?line circular_info(Pid, 15, <<"678">>, Xfn),
    ?line circular_info(Pid, 18, <<"123">>, Xfn),
    ?line circular_info(Pid, 21, <<"456">>, Xfn),

    case proplists:get_value(num_procs, Opts) of
	undefined -> ok;
	NumProcs ->
	    WorkerProcs = get_linked_procs(Pid),
	    ?line NumProcs = length(WorkerProcs)
    end,

    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).

circ3m_info(Opts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(Opts, Xfn)),
    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid),
    ?line {pct_complete, is_circular} = gen_stream:pct_complete(Pid),
    ?line circular_info(Pid,  0*4, odd_bin([1,3,5]), Xfn),
    ?line circular_info(Pid,  3*4, odd_bin([7,9,11]), Xfn),
    ?line circular_info(Pid,  6*4, odd_bin([13,15,17]), Xfn),
    ?line circular_info(Pid,  9*4, odd_bin([19,21,23]), Xfn),
    ?line circular_info(Pid, 12*4, odd_bin([25,27,29]), Xfn),
    ?line circular_info(Pid, 15*4, odd_bin([31,33,35]), Xfn),
    ?line circular_info(Pid, 18*4, odd_bin([1,3,5]), Xfn),
    ?line circular_info(Pid, 21*4, odd_bin([7,9,11]), Xfn),

    case proplists:get_value(num_procs, Opts) of
	undefined -> ok;
	NumProcs ->
	    WorkerProcs = get_linked_procs(Pid),
	    ?line NumProcs = length(WorkerProcs)
    end,

    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).

circ4_info(Opts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(Opts, Xfn)),
    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid),
    ?line {pct_complete, is_circular} = gen_stream:pct_complete(Pid),
    ?line circular_info(Pid,  0, <<"1234">>, Xfn),
    ?line circular_info(Pid,  4, <<"5123">>, Xfn),
    ?line circular_info(Pid,  8, <<"4512">>, Xfn),
    ?line circular_info(Pid, 12, <<"3451">>, Xfn),
    ?line circular_info(Pid, 16, <<"2345">>, Xfn),
    ?line circular_info(Pid, 20, <<"1234">>, Xfn),
    ?line circular_info(Pid, 24, <<"5123">>, Xfn),
    ?line circular_info(Pid, 28, <<"4512">>, Xfn),
    ?line circular_info(Pid, 32, <<"3451">>, Xfn),
    ?line circular_info(Pid, 36, <<"2345">>, Xfn),
    ?line circular_info(Pid, 40, <<"1234">>, Xfn),

    case proplists:get_value(num_procs, Opts) of
	undefined -> ok;
	NumProcs ->
	    WorkerProcs = get_linked_procs(Pid),
	    ?line NumProcs = length(WorkerProcs)
    end,

    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).

circ4m_info(Opts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(Opts, Xfn)),
    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid),
    ?line {pct_complete, is_circular} = gen_stream:pct_complete(Pid),
    ?line circular_info(Pid,  0*4, odd_bin([1,3,5,7]), Xfn),
    ?line circular_info(Pid,  4*4, odd_bin([9,1,3,5]), Xfn),
    ?line circular_info(Pid,  8*4, odd_bin([7,9,1,3]), Xfn),
    ?line circular_info(Pid, 12*4, odd_bin([5,7,9,1]), Xfn),
    ?line circular_info(Pid, 16*4, odd_bin([3,5,7,9]), Xfn),
    ?line circular_info(Pid, 20*4, odd_bin([1,3,5,7]), Xfn),
    ?line circular_info(Pid, 24*4, odd_bin([9,1,3,5]), Xfn),
    ?line circular_info(Pid, 28*4, odd_bin([7,9,1,3]), Xfn),
    ?line circular_info(Pid, 32*4, odd_bin([5,7,9,1]), Xfn),
    ?line circular_info(Pid, 36*4, odd_bin([3,5,7,9]), Xfn),
    ?line circular_info(Pid, 40*4, odd_bin([1,3,5,7]), Xfn),

    case proplists:get_value(num_procs, Opts) of
	undefined -> ok;
	NumProcs ->
	    WorkerProcs = get_linked_procs(Pid),
	    ?line NumProcs = length(WorkerProcs)
    end,

    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).

circ8_info(Opts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(Opts, Xfn)),
    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid),
    ?line {pct_complete, is_circular} = gen_stream:pct_complete(Pid),
    ?line circular_info(Pid,  0, <<"12345123">>, Xfn),
    ?line circular_info(Pid,  8, <<"45123451">>, Xfn),
    ?line circular_info(Pid, 16, <<"23451234">>, Xfn),
    ?line circular_info(Pid, 24, <<"51234512">>, Xfn),
    ?line circular_info(Pid, 32, <<"34512345">>, Xfn),
    ?line circular_info(Pid, 40, <<"12345123">>, Xfn),
    ?line circular_info(Pid, 48, <<"45123451">>, Xfn),
    ?line circular_info(Pid, 56, <<"23451234">>, Xfn),
    ?line circular_info(Pid, 64, <<"51234512">>, Xfn),
    ?line circular_info(Pid, 72, <<"34512345">>, Xfn),
    ?line circular_info(Pid, 80, <<"12345123">>, Xfn),

    case proplists:get_value(num_procs, Opts) of
	undefined -> ok;
	NumProcs ->
	    WorkerProcs = get_linked_procs(Pid),
	    ?line NumProcs = length(WorkerProcs)
    end,

    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).

circ8m_info(Opts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(Opts, Xfn)),
    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid),
    ?line {pct_complete, is_circular} = gen_stream:pct_complete(Pid),
    ?line circular_info(Pid,  0*4, odd_bin([1,3,5,7,9,1,3,5]), Xfn),
    ?line circular_info(Pid,  8*4, odd_bin([7,9,1,3,5,7,9,1]), Xfn),
    ?line circular_info(Pid, 16*4, odd_bin([3,5,7,9,1,3,5,7]), Xfn),
    ?line circular_info(Pid, 24*4, odd_bin([9,1,3,5,7,9,1,3]), Xfn),
    ?line circular_info(Pid, 32*4, odd_bin([5,7,9,1,3,5,7,9]), Xfn),
    ?line circular_info(Pid, 40*4, odd_bin([1,3,5,7,9,1,3,5]), Xfn),
    ?line circular_info(Pid, 48*4, odd_bin([7,9,1,3,5,7,9,1]), Xfn),
    ?line circular_info(Pid, 56*4, odd_bin([3,5,7,9,1,3,5,7]), Xfn),
    ?line circular_info(Pid, 64*4, odd_bin([9,1,3,5,7,9,1,3]), Xfn),
    ?line circular_info(Pid, 72*4, odd_bin([5,7,9,1,3,5,7,9]), Xfn),
    ?line circular_info(Pid, 80*4, odd_bin([1,3,5,7,9,1,3,5]), Xfn),

    case proplists:get_value(num_procs, Opts) of
	undefined -> ok;
	NumProcs ->
	    WorkerProcs = get_linked_procs(Pid),
	    ?line NumProcs = length(WorkerProcs)
    end,

    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).

circ_f14_info(Opts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(Opts, Xfn)),
    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid),
    ?line {pct_complete, is_circular} = gen_stream:pct_complete(Pid),
    ?line circular_info(Pid,   0, <<"abcdefghijklmn">>, Xfn),
    ?line circular_info(Pid,  14, <<"opqrstuvwxyz\n1">>, Xfn),
    ?line circular_info(Pid,  28, <<"234567890\nabcd">>, Xfn),
    ?line circular_info(Pid,  42, <<"efghijklmnopqr">>, Xfn),
    ?line circular_info(Pid,  56, <<"stuvwxyz\n12345">>, Xfn),
    ?line circular_info(Pid,  70, <<"67890\nabcdefgh">>, Xfn),

    case proplists:get_value(num_procs, Opts) of
	undefined -> ok;
	NumProcs ->
	    WorkerProcs = get_linked_procs(Pid),
	    ?line NumProcs = length(WorkerProcs)
    end,

    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).

circ_f19_info(Opts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(Opts, Xfn)),
    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid),
    ?line {pct_complete, is_circular} = gen_stream:pct_complete(Pid),
    ?line circular_info(Pid,   0, <<"abcdefghijklmnopqrs">>, Xfn),
    ?line circular_info(Pid,  19, <<"tuvwxyz\n1234567890\n">>, Xfn),
    ?line circular_info(Pid,  38, <<"abcdefghijklmnopqrs">>, Xfn),
    ?line circular_info(Pid,  57, <<"tuvwxyz\n1234567890\n">>, Xfn),

    case proplists:get_value(num_procs, Opts) of
	undefined -> ok;
	NumProcs ->
	    WorkerProcs = get_linked_procs(Pid),
	    ?line NumProcs = length(WorkerProcs)
    end,

    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).

circ_f60_info(Opts, Xfn) ->
    ?line {ok, Pid} = gen_stream:start(create_opts(Opts, Xfn)),
    ?line {stream_size, is_circular} = gen_stream:stream_size(Pid),
    ?line {pct_complete, is_circular} = gen_stream:pct_complete(Pid),
    ?line circular_info(Pid,   0, <<"abcdefghijklmnopqrstuvwxyz\n"
				    "1234567890\nabcdefghijklmnop"
				    "qrstuv"
				  >>, Xfn),
    ?line circular_info(Pid,  60, <<"wxyz\n1234567890\nabcdefghijk"
				    "lmnopqrstuvwxyz\n1234567890\n"
				    "abcdef"
				  >>, Xfn),
    ?line circular_info(Pid, 120, <<"ghijklmnopqrstuvwxyz\n123456"
				    "7890\nabcdefghijklmnopqrstuv"
				    "wxyz\n1"
				  >>, Xfn),

    case proplists:get_value(num_procs, Opts) of
	undefined -> ok;
	NumProcs ->
	    WorkerProcs = get_linked_procs(Pid),
	    ?line NumProcs = length(WorkerProcs)
    end,

    ?line stopped = gen_stream:stop(Pid),
    ?line busy_wait_for_process_to_end(Pid, 600),
    ?line {'EXIT', {noproc,_}} = (catch gen_stream:stream_size(Pid)).


circular_info(Pid, ExpPos, ExpBin, Xfn) ->
    {stream_pos, ExpPos} = gen_stream:stream_pos(Pid),
    ExpResult = case Xfn of
		    undefined ->  {next_block, ExpBin};
		    {Mod, Fun} -> {next_block, Mod:Fun(ExpBin)};
		    Fun ->        {next_block, Fun(ExpBin)}
		end,
    ExpResult = gen_stream:next_block(Pid).


%%%--------------------------------------------------------
%%% Internal utility functions
%%%--------------------------------------------------------
test_file_path(Config, FileName) ->
    ?line filename:join(?datadir(Config), FileName).

file_size(FilePath) ->
    {ok, FileInfo} = file:read_file_info(FilePath),
    FileInfo#file_info.size.

odd_bin(Items) ->
    << << 0,0,0,N >> || N <- Items >>.

get_linked_procs(Pid) ->
    {links, Procs} = erlang:process_info(Pid, links),
    [ P || P <- Procs, P =/= self()].

check_procs_dead(WorkerProcs) ->
    receive after 1000 -> ok end,
    [] = [ P || P <- WorkerProcs, is_process_alive(P) ].


%%%--------------------------------------------------------
%%% Exported functions for x_mfa tests
%%%--------------------------------------------------------
reverse(Bin) ->
    list_to_binary(lists:reverse(binary_to_list(Bin))).

shell_bug_if_procs_leaked() -> c:i().
