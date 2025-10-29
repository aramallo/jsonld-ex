# JSON-LD Framing Performance Benchmarks

This directory contains performance benchmarks for the JSON-LD Framing implementation.

## Running Benchmarks

### With Benchee (Recommended)

1. Add Benchee to your dependencies in `mix.exs`:

```elixir
{:benchee, "~> 1.0", only: :dev}
```

2. Install dependencies:

```bash
mix deps.get
```

3. Run the benchmarks:

```bash
mix run bench/framing_bench.exs
```

### Without Benchee

If you don't want to install Benchee, the benchmark script will automatically fall back to a simple manual timing test:

```bash
mix run bench/framing_bench.exs
```

## Benchmark Categories

The benchmark suite includes the following test categories:

### 1. **Graph Size Scaling**
Tests how framing performance scales with different graph sizes:
- Small graphs (10 nodes)
- Medium graphs (100 nodes)
- Large graphs (1000 nodes)

### 2. **Embed Options**
Compares performance of different `@embed` options:
- `@embed: :once` (default) - Embed first occurrence only
- `@embed: :always` - Embed every occurrence
- `@embed: :never` - Only include references

### 3. **Explicit Property Filtering**
Measures the impact of the `@explicit` flag:
- `explicit: false` - Include all properties (default)
- `explicit: true` - Include only frame properties

### 4. **Nested Structure Depth**
Tests performance with various nesting depths:
- Depth 3
- Depth 5
- Depth 7

### 5. **Real-World Scenario**
Complex bibliographic library scenario with:
- Multiple entity types
- Embedded references
- Real-world property structures

### 6. **Frame Matching Complexity**
Compares different matching strategies:
- Simple type matching
- Property value matching
- Wildcard property matching

### 7. **Memory Usage**
Analyzes memory consumption with different options:
- Default options
- With explicit filtering
- With embed: never

## Understanding Results

### Key Metrics

- **Average time**: Mean execution time across iterations
- **Standard deviation**: Variation in execution time
- **Memory usage**: Total memory allocated during execution
- **Iterations per second**: Throughput metric

### Expected Performance Characteristics

1. **Linear scaling**: Framing time should scale roughly linearly with graph size
2. **Memory efficiency**: Stream-based processing keeps memory usage low
3. **Caching benefits**: Repeated frame matching benefits from cache
4. **Embedding overhead**: `@embed: :always` uses more memory than `:never`

## Optimization Tips

Based on benchmark results:

1. **Use `@embed: :never`** for large graphs when full embedding isn't needed
2. **Use `@explicit: true`** to reduce processing when only specific properties are needed
3. **Use `@requireAll: false`** for faster matching when not all properties must match
4. **Process in batches** for extremely large datasets (10,000+ nodes)

## Example Results

Typical benchmark results on a modern laptop:

```
Small graph (10 nodes):      ~2-5 ms
Medium graph (100 nodes):    ~15-30 ms
Large graph (1000 nodes):    ~150-300 ms
```

Memory usage:
```
Medium graph:  ~2-5 MB
Large graph:   ~15-30 MB
```

## Continuous Performance Monitoring

To track performance over time:

1. Run benchmarks before and after changes
2. Compare results to detect regressions
3. Document significant performance improvements

## Custom Benchmarks

To add custom benchmarks, edit `framing_bench.exs` and add new test cases following the existing patterns.

Example:

```elixir
Benchee.run(
  %{
    "my custom test" => fn ->
      JSON.LD.frame(my_input, my_frame, my_options)
    end
  },
  time: 3,
  memory_time: 1
)
```
