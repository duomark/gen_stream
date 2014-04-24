# A Stream Server

Recently Tim Bray proposed a problem called [The Wide Finder
Project](http://www.tbray.org/ongoing/When/200x/2007/09/20/Wide-Finder)
which he thought would be appropriate for concurrent languages that could
take advantage of multicore machines. His Ruby program was around 10 lines
of code. Several erlang developers attempted to improve on the performance,
and ended up writing 300-400 lines of code to compete.

One of the themes that was common to the erlang solutions was the need to
read a file larger than memory and split it into bite-sized chunks to be
scanned in parallel. There were several approaches, and many inefficiencies
were rediscovered on the way to more efficient implementations. The lack of
a good idiom was the impetus for this proposal.

Our suggested solution is to add a module called *gen\_stream* which
provides a standard mechanism for delivering binary data in chunks so that
the large binaries don't overwhelm a node's resources. It has been optimized
to ensure good performance in a variety of situations, a consistent API and
a declarative set of options to help an application developer tune the
efficiency. The model is a sequential stream of successive chunks, with the
possibility of making it circular so that there is no end to the stream.

## Reference Implementation

The current implementation supports the following features:

- Binary source can be one of three types:
    -   An erlang binary
    -   A file which will be read in raw, binary mode
    -   A module implementing a gen\_stream behaviour
- Options for memory and process usage:
    -   Number of processes to use in chunking the binary source
    -   Number of chunk buffers per process
    -   Size of each chunk
    -   Whether the stream is one pass or endlessly repeating
- Missing features to be added:
    -   Proper handling of debug and sys messages in chunk procs

A gen\_stream is a gen\_server with configurable options. It supports the
following calls:

- `start_link(OptionList)`: start gen\_server
- `stream_size(Server)`: call gen\_server to return integer size or an atom (e.g., is\_circular, infinite, or undefined)
- `next_chunk(Server)`: call gen\_server to return next chunk
- `stream_pos(Server)`: call gen\_server for current number of bytes already seen
- `pct_complete(Server)`: call gen\_server to report percent done
- `stop(Server)`: tell gen\_server to shut down

A working implementation with unit tests can be downloaded so that others
can evaluate the usefulness of a gen\_stream module (the tar file creates a
new directory called *gen\_stream* with all files contained inside the new
directory):

- [EEP text](http://duomark.com/erlang/proposals/eep_gen_stream.txt)
- [gen\_stream.20071210.tar.gz](http://duomark.com/erlang/proposals/gen_stream.20071210.tar.gz) initial release (corrected to include gen\_stream.hrl)
- [github source](https://github.com/duomark/gen_stream)

## Implementation Details

Below are examples of how to use the gen\_stream module (note it is not
required that you call stream\_pos or pct\_complete, these are provided only
as usage patterns):

```erlang
process_fill(FileName, Fun, Opts) ->
  {ok, S} = gen_stream:start_link([{stream, {file, FileName}}] ++ Opts),
  {stream_size, Size} = gen_stream:stream_size(S),
  consume(S, Fun),
  gen_stream:stop(S).

consume(Stream, ProcessFn) ->
  case gen_stream:next_chunk(Stream) of
    {next_chunk, end_of_stream} -> ok;
    {next_chunk, Binary} ->
      {pct_complete, Pct} = gen_stream:pct_complete(Stream),
      {stream_pos, Pos} = gen_stream:stream_pos(Stream),
      ProcessFn(Binary, Pct, Pos),
      consume(Stream, ProcessFn)
  end.
```

While the use of a gen\_server causes the serialized gen\_stream to be a
narrow pipe when processing chunks concurrently, it gives the cleanest
interface and most control over the flow of binary chunks into an
application. The performance may not be as fast as a hand-tuned
implementation, but the gain in flexibility by the standardized approach is
valuable and imposes a minimal overhead on performance. By declaratively
altering the options, architectural choices with differing number of
processes or buffers can be tried to find the optimal configuration for a
particular system.


## Notes

1. Buffer is being copied twice in next_block return

1. Should have an option to not enforce sequential ordering
    - next_block can skip a process if no block available

## License

- New BSD license
- see https://github.com/duomark/gen_stream/blob/master/LICENSE
