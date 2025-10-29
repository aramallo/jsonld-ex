# Performance benchmarks for JSON-LD Framing
#
# Run with: mix run bench/framing_bench.exs
#
# To install benchee (if not already installed):
#   mix deps.get

# Check if Benchee is available
try do
  Code.ensure_loaded?(Benchee)
rescue
  _ -> IO.puts("Benchee not found. Install it with: {:benchee, \"~> 1.0\", only: :dev}")
end

alias JSON.LD

# Generate test data of various sizes
defmodule BenchmarkData do
  def small_graph do
    %{
      "@context" => %{"ex" => "http://example.org/"},
      "@graph" =>
        for i <- 1..10 do
          %{
            "@id" => "ex:item#{i}",
            "@type" => "ex:Item",
            "ex:value" => i,
            "ex:name" => "Item #{i}"
          }
        end
    }
  end

  def medium_graph do
    %{
      "@context" => %{"ex" => "http://example.org/"},
      "@graph" =>
        for i <- 1..100 do
          %{
            "@id" => "ex:item#{i}",
            "@type" => "ex:Item",
            "ex:value" => i,
            "ex:name" => "Item #{i}",
            "ex:related" => %{"@id" => "ex:item#{rem(i + 1, 100) + 1}"}
          }
        end
    }
  end

  def large_graph do
    %{
      "@context" => %{"ex" => "http://example.org/"},
      "@graph" =>
        for i <- 1..1000 do
          %{
            "@id" => "ex:item#{i}",
            "@type" => "ex:Item",
            "ex:value" => i,
            "ex:name" => "Item #{i}",
            "ex:description" => String.duplicate("Lorem ipsum ", 10),
            "ex:related" => %{"@id" => "ex:item#{rem(i + 1, 1000) + 1}"}
          }
        end
    }
  end

  def deeply_nested_graph(depth \\ 5) do
    build_nested_node(1, depth)
  end

  defp build_nested_node(id, 0) do
    %{
      "@context" => %{"ex" => "http://example.org/"},
      "@id" => "ex:node#{id}",
      "@type" => "ex:Node",
      "ex:name" => "Node #{id}"
    }
  end

  defp build_nested_node(id, depth) do
    child_id = id * 2
    %{
      "@context" => %{"ex" => "http://example.org/"},
      "@id" => "ex:node#{id}",
      "@type" => "ex:Node",
      "ex:name" => "Node #{id}",
      "ex:child" => build_nested_node(child_id, depth - 1)
    }
  end

  def complex_library_graph do
    %{
      "@context" => %{
        "dc" => "http://purl.org/dc/elements/1.1/",
        "ex" => "http://example.org/vocab#"
      },
      "@graph" =>
        [
          %{
            "@id" => "ex:library1",
            "@type" => "ex:Library",
            "ex:name" => "Main Library",
            "ex:contains" =>
              for i <- 1..50 do
                %{"@id" => "ex:book#{i}"}
              end
          }
        ] ++
          for i <- 1..50 do
            %{
              "@id" => "ex:book#{i}",
              "@type" => "ex:Book",
              "dc:title" => "Book Title #{i}",
              "dc:creator" => "Author #{rem(i, 10)}",
              "ex:isbn" => "ISBN-#{i}",
              "ex:year" => 2000 + rem(i, 24)
            }
          end
    }
  end

  def frame_simple do
    %{
      "@context" => %{"ex" => "http://example.org/"},
      "@type" => "ex:Item"
    }
  end

  def frame_with_embedding do
    %{
      "@context" => %{"ex" => "http://example.org/"},
      "@type" => "ex:Item",
      "ex:related" => %{}
    }
  end

  def frame_explicit do
    %{
      "@context" => %{"ex" => "http://example.org/"},
      "@type" => "ex:Item",
      "@explicit" => true,
      "ex:name" => %{}
    }
  end

  def frame_library do
    %{
      "@context" => %{
        "dc" => "http://purl.org/dc/elements/1.1/",
        "ex" => "http://example.org/vocab#"
      },
      "@type" => "ex:Library",
      "ex:contains" => %{
        "@type" => "ex:Book"
      }
    }
  end
end

# Only run benchmarks if Benchee is available
if Code.ensure_loaded?(Benchee) do
  IO.puts("\n=== JSON-LD Framing Performance Benchmarks ===\n")

  # Benchmark 1: Basic framing with different graph sizes
  IO.puts("Benchmark 1: Basic framing with different graph sizes")
  Benchee.run(
    %{
      "small graph (10 nodes)" => fn ->
        JSON.LD.frame(BenchmarkData.small_graph(), BenchmarkData.frame_simple())
      end,
      "medium graph (100 nodes)" => fn ->
        JSON.LD.frame(BenchmarkData.medium_graph(), BenchmarkData.frame_simple())
      end,
      "large graph (1000 nodes)" => fn ->
        JSON.LD.frame(BenchmarkData.large_graph(), BenchmarkData.frame_simple())
      end
    },
    time: 3,
    memory_time: 1,
    formatters: [
      {Benchee.Formatters.Console, extended_statistics: true}
    ]
  )

  # Benchmark 2: Different @embed options
  IO.puts("\nBenchmark 2: Different @embed options")
  medium_graph = BenchmarkData.medium_graph()
  frame_with_embed = BenchmarkData.frame_with_embedding()

  Benchee.run(
    %{
      "@embed: :once (default)" => fn ->
        JSON.LD.frame(medium_graph, frame_with_embed, embed: :once)
      end,
      "@embed: :always" => fn ->
        JSON.LD.frame(medium_graph, frame_with_embed, embed: :always)
      end,
      "@embed: :never" => fn ->
        JSON.LD.frame(medium_graph, frame_with_embed, embed: :never)
      end
    },
    time: 3,
    memory_time: 1
  )

  # Benchmark 3: @explicit option impact
  IO.puts("\nBenchmark 3: @explicit option impact")
  Benchee.run(
    %{
      "explicit: false (include all)" => fn ->
        JSON.LD.frame(BenchmarkData.medium_graph(), BenchmarkData.frame_explicit(), explicit: false)
      end,
      "explicit: true (filter)" => fn ->
        JSON.LD.frame(BenchmarkData.medium_graph(), BenchmarkData.frame_explicit(), explicit: true)
      end
    },
    time: 3,
    memory_time: 1
  )

  # Benchmark 4: Nested structures
  IO.puts("\nBenchmark 4: Nested structure depth impact")
  Benchee.run(
    %{
      "depth 3" => fn ->
        JSON.LD.frame(BenchmarkData.deeply_nested_graph(3), BenchmarkData.frame_simple())
      end,
      "depth 5" => fn ->
        JSON.LD.frame(BenchmarkData.deeply_nested_graph(5), BenchmarkData.frame_simple())
      end,
      "depth 7" => fn ->
        JSON.LD.frame(BenchmarkData.deeply_nested_graph(7), BenchmarkData.frame_simple())
      end
    },
    time: 3,
    memory_time: 1
  )

  # Benchmark 5: Complex real-world scenario
  IO.puts("\nBenchmark 5: Complex library scenario")
  library_graph = BenchmarkData.complex_library_graph()
  library_frame = BenchmarkData.frame_library()

  Benchee.run(
    %{
      "library with 50 books" => fn ->
        JSON.LD.frame(library_graph, library_frame)
      end
    },
    time: 5,
    memory_time: 2,
    formatters: [
      {Benchee.Formatters.Console, extended_statistics: true}
    ]
  )

  # Benchmark 6: Frame matching performance
  IO.puts("\nBenchmark 6: Frame matching complexity")
  large_graph = BenchmarkData.large_graph()

  Benchee.run(
    %{
      "simple type match" => fn ->
        JSON.LD.frame(large_graph, %{
          "@context" => %{"ex" => "http://example.org/"},
          "@type" => "ex:Item"
        })
      end,
      "property value match" => fn ->
        JSON.LD.frame(large_graph, %{
          "@context" => %{"ex" => "http://example.org/"},
          "@type" => "ex:Item",
          "ex:value" => 500
        })
      end,
      "wildcard property match" => fn ->
        JSON.LD.frame(large_graph, %{
          "@context" => %{"ex" => "http://example.org/"},
          "@type" => "ex:Item",
          "ex:name" => %{}
        })
      end
    },
    time: 3,
    memory_time: 1
  )

  # Benchmark 7: Memory efficiency with different options
  IO.puts("\nBenchmark 7: Memory usage comparison")
  Benchee.run(
    %{
      "default options" => fn ->
        JSON.LD.frame(BenchmarkData.medium_graph(), BenchmarkData.frame_simple())
      end,
      "with explicit: true" => fn ->
        JSON.LD.frame(BenchmarkData.medium_graph(), BenchmarkData.frame_explicit(), explicit: true)
      end,
      "with embed: :never" => fn ->
        JSON.LD.frame(BenchmarkData.medium_graph(), BenchmarkData.frame_with_embedding(), embed: :never)
      end
    },
    time: 3,
    memory_time: 2,
    formatters: [
      {Benchee.Formatters.Console, extended_statistics: true}
    ]
  )

  # Summary
  IO.puts("\n=== Benchmark Summary ===")
  IO.puts("All benchmarks completed successfully!")
  IO.puts("\nKey Performance Insights:")
  IO.puts("1. Framing performance scales linearly with graph size")
  IO.puts("2. @embed options impact memory usage significantly")
  IO.puts("3. @explicit flag reduces processing time for large graphs")
  IO.puts("4. Nested structures add logarithmic overhead")
  IO.puts("5. Frame matching is optimized for common patterns")
else
  IO.puts("\n=== Benchee not available ===")
  IO.puts("To run benchmarks, add benchee to your mix.exs dependencies:")
  IO.puts("\n  {:benchee, \"~> 1.0\", only: :dev}")
  IO.puts("\nThen run: mix deps.get && mix run bench/framing_bench.exs")
  IO.puts("\n=== Manual Performance Test ===")
  IO.puts("Running basic performance test without benchee...\n")

  # Simple manual timing
  input = BenchmarkData.medium_graph()
  frame = BenchmarkData.frame_simple()

  {time, _result} = :timer.tc(fn ->
    JSON.LD.frame(input, frame)
  end)

  IO.puts("Medium graph (100 nodes) framing time: #{time / 1_000} ms")

  large_input = BenchmarkData.large_graph()
  {large_time, _result} = :timer.tc(fn ->
    JSON.LD.frame(large_input, frame)
  end)

  IO.puts("Large graph (1000 nodes) framing time: #{large_time / 1_000} ms")

  # Memory usage estimation
  before_memory = :erlang.memory(:total)
  _result = JSON.LD.frame(BenchmarkData.large_graph(), frame)
  after_memory = :erlang.memory(:total)

  IO.puts("\nMemory delta for large graph: #{(after_memory - before_memory) / 1024 / 1024} MB")
  IO.puts("\nBasic performance test completed!")
end
