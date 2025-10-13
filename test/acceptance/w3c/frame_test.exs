defmodule JSON.LD.W3C.FrameTest do
  @moduledoc """
  The official W3C JSON-LD 1.1 Test Suite for the _Framing Algorithm_.

  See <https://w3c.github.io/json-ld-framing/tests/frame-manifest.html>.
  """

  use ExUnit.Case, async: false
  use RDF.Test.EarlFormatter, test_suite: :"json-ld-framing"

  import JSON.LD.TestSuite
  import JSON.LD.Case

  @test_suite_name "frame"
  @base_url "https://w3c.github.io/json-ld-framing/tests/"

  # Fetch manifest from remote at compile time
  @external_resource manifest_url =
                       "https://w3c.github.io/json-ld-framing/tests/frame-manifest.jsonld"

  @manifest (fn ->
               case Tesla.get(manifest_url) do
                 {:ok, %Tesla.Env{status: 200, body: body}} ->
                   Jason.decode!(body)

                 {:ok, %{status: status}} ->
                   raise "HTTP request of #{manifest_url} failed with status #{status}"

                 {:error, error} ->
                   raise "Failed to fetch framing test manifest: #{inspect(error)}"
               end
             end).()

  # Helper to fetch remote JSON file
  defp fetch_json(file_path) do
    url = @base_url <> file_path

    case Tesla.get(url) do
      {:ok, %Tesla.Env{status: 200, body: body}} ->
        Jason.decode!(body)

      {:ok, %{status: status}} ->
        raise "HTTP request of #{url} failed with status #{status}"

      {:error, error} ->
        raise "Failed to fetch #{url}: #{inspect(error)}"
    end
  end

  # Extract options from test case
  defp test_case_frame_options(test_case) do
    test_case
    |> Map.get("option", %{})
    |> Enum.map(fn {key, value} ->
      {key |> Macro.underscore() |> String.to_atom(), value}
    end)
  end

  # Skip JSON-LD 1.0 tests for now (focus on 1.1)
  @skipped_1_0_tests [
    "#t0001"
    # Add more 1.0 test IDs as needed
  ]

  @manifest
  |> Map.get("sequence", [])
  |> Enum.group_by(fn test_case ->
    types = List.wrap(test_case["@type"])

    cond do
      "jld:PositiveEvaluationTest" in types -> :positive_evaluation_test
      "jld:NegativeEvaluationTest" in types -> :negative_evaluation_test
      true -> :unknown
    end
  end)
  |> Enum.each(fn
    {:positive_evaluation_test, test_cases} ->
      for %{"@id" => id, "name" => name} = test_case <- test_cases do
        if id in @skipped_1_0_tests do
          @tag skip: "JSON-LD 1.0 test"
        end

        @tag :test_suite
        @tag :frame_test_suite
        @tag test_case: RDF.iri(@base_url <> "frame-manifest" <> id)
        @tag data: test_case
        test "frame#{id}: #{name}", %{data: test_case} do
          input = fetch_json(test_case["input"])
          frame = fetch_json(test_case["frame"])
          expected = fetch_json(test_case["expect"])
          options = test_case_frame_options(test_case)

          result = JSON.LD.frame(input, frame, options)

          assert result == expected,
                 """
                 Frame test #{test_case["@id"]} failed: #{test_case["name"]}

                 Input: #{inspect(input, pretty: true)}
                 Frame: #{inspect(frame, pretty: true)}
                 Expected: #{inspect(expected, pretty: true)}
                 Got: #{inspect(result, pretty: true)}
                 """
        end
      end

    {:negative_evaluation_test, test_cases} ->
      for %{"@id" => id, "name" => name} = test_case <- test_cases do
        @tag :test_suite
        @tag :frame_test_suite
        @tag test_case: RDF.iri(@base_url <> "frame-manifest" <> id)
        @tag data: test_case
        test "frame#{id}: #{name}", %{data: test_case} do
          input = fetch_json(test_case["input"])
          frame = fetch_json(test_case["frame"])
          error = test_case["expectErrorCode"]
          options = test_case_frame_options(test_case)

          assert_raise_json_ld_error error, fn ->
            JSON.LD.frame(input, frame, options)
          end
        end
      end

    _ ->
      :ok
  end)
end
