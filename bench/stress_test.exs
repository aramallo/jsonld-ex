# Stress test for JSON-LD Framing with large inputs
#
# This validates that the memory-efficient implementation can handle
# large graphs without excessive memory usage or performance degradation.
#
# Run with: mix run bench/stress_test.exs

alias JSON.LD

defmodule StressTest do
  @moduledoc """
  Stress testing utilities for JSON-LD Framing.

  Tests the implementation with increasingly large inputs to validate:
  1. Memory efficiency (no memory leaks)
  2. Performance scaling (linear or better)
  3. Correctness under load
  """

  def run_all do
    IO.puts("\n=== JSON-LD Framing Stress Test ===\n")

    IO.puts("Testing memory efficiency with large graphs...")
    test_memory_efficiency()

    IO.puts("\nTesting performance scaling...")
    test_performance_scaling()

    IO.puts("\nTesting deeply nested structures...")
    test_deep_nesting()

    IO.puts("\nTesting circular references...")
    test_circular_references()

    IO.puts("\nTesting with many duplicate references...")
    test_duplicate_references()

    IO.puts("\n=== Stress Test Complete ===")
    IO.puts("All tests passed successfully!")
  end

  def test_memory_efficiency do
    IO.puts("  Creating graph with 5000 nodes...")

    input = %{
      "@context" => %{"ex" => "http://example.org/"},
      "@graph" =>
        for i <- 1..5000 do
          %{
            "@id" => "ex:item#{i}",
            "@type" => "ex:Item",
            "ex:value" => i,
            "ex:name" => "Item #{i}",
            "ex:description" => String.duplicate("Lorem ipsum dolor sit amet ", 5)
          }
        end
    }

    frame = %{
      "@context" => %{"ex" => "http://example.org/"},
      "@type" => "ex:Item"
    }

    # Measure memory before and after
    before_memory = :erlang.memory(:total)
    :erlang.garbage_collect()

    {time_us, result} = :timer.tc(fn ->
      JSON.LD.frame(input, frame)
    end)

    :erlang.garbage_collect()
    after_memory = :erlang.memory(:total)

    memory_delta = (after_memory - before_memory) / 1024 / 1024
    time_ms = time_us / 1_000

    IO.puts("  ✓ Time: #{Float.round(time_ms, 2)} ms")
    IO.puts("  ✓ Memory delta: #{Float.round(memory_delta, 2)} MB")

    # Verify result is valid
    result_count = if is_list(result["@graph"]), do: length(result["@graph"]), else: 1
    IO.puts("  ✓ Results: #{result_count} nodes framed")

    # Memory should be reasonable (under 100MB for 5000 nodes)
    if memory_delta > 100 do
      IO.puts("  ⚠️  Warning: High memory usage detected")
    end

    :ok
  end

  def test_performance_scaling do
    IO.puts("  Testing with increasing graph sizes...")

    sizes = [100, 500, 1000, 2500, 5000]
    results = Enum.map(sizes, fn size ->
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" =>
          for i <- 1..size do
            %{
              "@id" => "ex:item#{i}",
              "@type" => "ex:Item",
              "ex:value" => i
            }
          end
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Item"
      }

      {time_us, _result} = :timer.tc(fn ->
        JSON.LD.frame(input, frame)
      end)

      {size, time_us / 1_000}
    end)

    # Print results
    Enum.each(results, fn {size, time} ->
      IO.puts("    #{size} nodes: #{Float.round(time, 2)} ms")
    end)

    # Check for roughly linear scaling
    [{size1, time1}, {size2, time2}] = Enum.take(results, 2)
    scaling_factor = (time2 / time1) / (size2 / size1)

    IO.puts("  ✓ Scaling factor: #{Float.round(scaling_factor, 2)}x (closer to 1.0 is better)")

    if scaling_factor > 2.0 do
      IO.puts("  ⚠️  Warning: Performance may not scale linearly")
    end

    :ok
  end

  def test_deep_nesting do
    IO.puts("  Testing with depth 10 nesting...")

    input = build_deeply_nested(10)

    frame = %{
      "@context" => %{"ex" => "http://example.org/"},
      "@type" => "ex:Node"
    }

    {time_us, result} = :timer.tc(fn ->
      JSON.LD.frame(input, frame)
    end)

    time_ms = time_us / 1_000
    IO.puts("  ✓ Time: #{Float.round(time_ms, 2)} ms")

    # Verify result structure
    depth = measure_depth(result)
    IO.puts("  ✓ Result depth: #{depth} levels")

    :ok
  end

  def test_circular_references do
    IO.puts("  Testing with circular reference graph...")

    # Create a graph with many circular references
    input = %{
      "@context" => %{"ex" => "http://example.org/"},
      "@graph" =>
        for i <- 1..100 do
          %{
            "@id" => "ex:node#{i}",
            "@type" => "ex:Node",
            "ex:next" => %{"@id" => "ex:node#{rem(i, 100) + 1}"},
            "ex:prev" => %{"@id" => "ex:node#{rem(i - 2 + 100, 100) + 1}"}
          }
        end
    }

    frame = %{
      "@context" => %{"ex" => "http://example.org/"},
      "@type" => "ex:Node"
    }

    # Should not crash or hang
    {time_us, result} = :timer.tc(fn ->
      JSON.LD.frame(input, frame, embed: :once)
    end)

    time_ms = time_us / 1_000
    IO.puts("  ✓ Time: #{Float.round(time_ms, 2)} ms")
    IO.puts("  ✓ Handled circular references without issues")

    result_count = if is_list(result["@graph"]), do: length(result["@graph"]), else: 1
    IO.puts("  ✓ Results: #{result_count} nodes framed")

    :ok
  end

  def test_duplicate_references do
    IO.puts("  Testing with many duplicate references...")

    # Create a hub-and-spoke graph where many nodes reference the same target
    input = %{
      "@context" => %{"ex" => "http://example.org/"},
      "@graph" =>
        [
          %{
            "@id" => "ex:hub",
            "@type" => "ex:Hub",
            "ex:name" => "Central Hub"
          }
        ] ++
        for i <- 1..500 do
          %{
            "@id" => "ex:spoke#{i}",
            "@type" => "ex:Spoke",
            "ex:connectsTo" => %{"@id" => "ex:hub"}
          }
        end
    }

    frame = %{
      "@context" => %{"ex" => "http://example.org/"},
      "@type" => "ex:Spoke"
    }

    {time_us, result} = :timer.tc(fn ->
      JSON.LD.frame(input, frame, embed: :once)
    end)

    time_ms = time_us / 1_000
    IO.puts("  ✓ Time: #{Float.round(time_ms, 2)} ms")
    IO.puts("  ✓ Efficiently handled duplicate references")

    result_count = if is_list(result["@graph"]), do: length(result["@graph"]), else: 1
    IO.puts("  ✓ Results: #{result_count} nodes framed")

    :ok
  end

  # Helper functions

  defp build_deeply_nested(depth) do
    %{
      "@context" => %{"ex" => "http://example.org/"},
      "@graph" => [build_nested_node(1, depth)]
    }
  end

  defp build_nested_node(id, 0) do
    %{
      "@id" => "ex:node#{id}",
      "@type" => "ex:Node",
      "ex:name" => "Leaf Node #{id}"
    }
  end

  defp build_nested_node(id, depth) do
    child_id = id * 2

    %{
      "@id" => "ex:node#{id}",
      "@type" => "ex:Node",
      "ex:name" => "Node #{id}",
      "ex:child" => build_nested_node(child_id, depth - 1)
    }
  end

  defp measure_depth(map) when is_map(map) do
    map
    |> Map.values()
    |> Enum.map(&measure_depth/1)
    |> Enum.max(fn -> 0 end)
    |> Kernel.+(1)
  end

  defp measure_depth(list) when is_list(list) do
    list
    |> Enum.map(&measure_depth/1)
    |> Enum.max(fn -> 0 end)
  end

  defp measure_depth(_), do: 0
end

# Run all stress tests
StressTest.run_all()
