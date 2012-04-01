%%%-------------------------------------------------------------------
%%% File: gen_stream_odd_nums.erl
%%% Author: Jay Nelson
%%% Description:
%%%   Generate odd numbers as a binary stream. This is an example of
%%%   a gen_stream behaviour which does not have any internal state.
%%%   To accomplish this, the functions need to be able to globally
%%%   compute stream elements based on the positional information
%%%   relative to the beginning of the stream.  Used for testing by
%%%   gen_stream_SUITE.
%%%-------------------------------------------------------------------
-module(gen_stream_odd_nums).

-behaviour(gen_stream).


-export([init/1, terminate/2, code_change/3, stream_size/1,
	 extract_block/5, extract_final_block/5, inc_progress/2]).

-export([gen_block/2]).

%% -include_lib("stdlib/include/gen_stream.hrl").
%% -include("../include/gen_stream.hrl").


%%%-------------------------------------------------------------------
%%% Argument parsing
%%%-------------------------------------------------------------------
get_size_option(Options) ->
    proplists:get_value(stream_size, Options, infinite).


%%%-------------------------------------------------------------------
%%% External gen_stream behaviour functions
%%%-------------------------------------------------------------------

%% This function is called when gen_stream gets {behaviour, odd_nums, InitArgs}
%% InitArgs should be [{stream_size, size}]
init(_Args) ->
    ok.

%% This function is called when gen_stream gets {behaviour, odd_nums, InitArgs}
%% InitArgs should be [{stream_size, Size}] if not 'infinite'
stream_size(Options) ->
    case get_size_option(Options) of
	Length when is_integer(Length) -> Length;
	_Other -> infinite
    end.

%% This function is called to track pct_complete
%% Since we only return binaries, blindly add its size.
inc_progress(Seen, Chunk) ->	    
    Seen + size(Chunk).

%% This behaviour has no state to cleanup.
terminate(_Reason, _State) ->
    ok.

%% Action to take on a code change.
code_change(_OldVsn, ModState, _Extra) ->
    ModState.

%% This function is called by gen_stream on normal block retrieval
extract_block(_State, Pos, NumBytes, _ChunkSize, _BlockFactor) ->
%%     Factor = ChunkSize div 4,
%%     gen_block(Pos div 4, Factor * BlockFactor).
    gen_block(Pos div 4, NumBytes div 4).

%% This function is only called when the last chunk is to be returned.
%% The ChunkSize should be the remaining length, not the original ChunkSize.
extract_final_block(State, Pos, NumBytes, ChunkSize, BlockFactor) ->
    %% This should never happen with an infinite stream,
    %% but is no different than normal extract_block, since
    %% ChunkSize has been reduced to just indicate the remaining.
    extract_block(State, Pos, NumBytes, ChunkSize, BlockFactor).


%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% Here is the generator code -- the real algorithm of this stream.
gen_block(Pos, Count) ->
    Counters = lists:seq(Pos, Pos + Count - 1),
    Elems = [ N * 2 + 1 || N <- Counters],
    Bins = [ << E:32 >> || E <- Elems],
    list_to_binary(Bins).
