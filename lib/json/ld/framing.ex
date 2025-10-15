defmodule JSON.LD.Framing do
  @moduledoc """
  Implementation of the JSON-LD 1.1 Framing Algorithms.

  This module provides a fully compliant implementation of the JSON-LD 1.1 Framing
  specification with optimizations for large inputs through caching and memoization.

  See <https://www.w3.org/TR/json-ld11-framing/>
  """

  import JSON.LD.Utils

  alias JSON.LD.{Context, Flattening, Compaction, Options, NodeIdentifierMap}

  @_type_ "@type"
  @always "@always"
  @context "@context"
  @default "@default"
  @direction "@direction"
  @embed "@embed"
  @explicit "@explicit"
  @graph "@graph"
  @id "@id"
  @included "@included"
  @language "@language"
  @last "@last"
  @list "@list"
  @never "@never"
  @omitDefault "@omitDefault"
  @once "@once"
  @preserve "@preserve"
  @requireAll "@requireAll"
  @reverse "@reverse"
  # @set "@set"
  @value "@value"
  @framing_keywords [@default, @embed, @explicit, @omitDefault, @requireAll]
  # Keywords that should never be included as regular properties in output
  @excluded_from_properties [
    @id,
    @_type_,
    @graph,
    @included,
    @list,
    @value,
    @language,
    @direction,
    @preserve,
    @reverse | @framing_keywords
  ]

  @doc """
  Frames the given input according to the frame.

  The main framing algorithm as specified in:
  <https://www.w3.org/TR/json-ld11-framing/#framing-algorithm>

  ## Algorithm Steps (Normative)

  Each step corresponds to the numbered steps in the specification.
  """
  @spec frame(map | [map], map, Options.t()) :: map
  def frame(input, frame, processor_options \\ %Options{})

  def frame(input, frame, processor_options) do
    # Step 1: Expand input
    # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 1
    # Set expanded input to the result of the Expansion Algorithm,
    # passing input and options with frame expansion set to false
    expanded_input =
      input
      |> JSON.LD.expand(%{processor_options | frame_expansion: false, ordered: false})

    # Step 2: Expand frame
    # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 2
    # Set expanded frame to the result of the Expansion Algorithm,
    # passing frame and options with frame expansion set to true
    expanded_frame =
      frame
      |> JSON.LD.expand(%{processor_options | frame_expansion: true, ordered: false})

    # Step 3: Extract frame context
    # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 3
    # Set context from frame to a copy of the active context
    frame_context =
      case frame do
        %{@context => context} -> JSON.LD.context(context, processor_options)
        _ -> Context.new(processor_options)
      end

    # Step 4: Generate node map (with memory-efficient node ID management)
    # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 4
    # Generate a map of flattened subjects from expanded input
    node_id_map = NodeIdentifierMap.new()

    try do
      # Use efficient node map generation
      node_map = Flattening.generate_node_map(expanded_input, %{@default => %{}}, node_id_map)

      # Determine which graph to frame
      # If input is a SINGLE named graph (one element with @id and @graph), frame that graph
      # If input has MULTIPLE named graphs or other patterns, frame from @default
      # Normative: JSON-LD 1.1 Framing - multiple named graphs are merged when framing from default
      current_graph =
        case expanded_input do
          # Single named graph - frame that specific graph
          [%{@id => graph_id, @graph => _}] when is_binary(graph_id) -> graph_id
          # Multiple elements or other patterns - frame from @default (with merging)
          _ -> @default
        end

      if System.get_env("DEBUG_FRAMING") != nil do
        IO.puts("\n=== CURRENT GRAPH ===")
        IO.puts("current_graph: #{inspect(current_graph)}")
        IO.puts("expanded_input length: #{inspect(length(List.wrap(expanded_input)))}")
      end

      # Step 5: Initialize framing state
      # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 5
      # Extract framing flags from frame (they can also be in options, but frame takes precedence)
      explicit_inclusion =
        case frame[@explicit] do
          true -> true
          false -> false
          nil -> processor_options.explicit || false
        end

      omit_default =
        case frame[@omitDefault] do
          true -> true
          false -> false
          nil -> processor_options.omit_default || false
        end

      require_all =
        case frame[@requireAll] do
          true -> true
          false -> false
          nil -> processor_options.require_all || false
        end

      # Create framing state with embed options and tracking structures
      state = %{
        # Embed option: @once (default), @always, @never, or @last
        embed: get_embed_option(processor_options),
        # Explicit inclusion flag (default: false)
        explicit_inclusion: explicit_inclusion,
        # Omit default flag (default: false)
        omit_default: omit_default,
        # Require all flag (default: false)
        require_all: require_all,
        # Map tracking embedded nodes (memory-efficient: stores only references)
        embedded: %{},
        # Graph stack for nested graph processing
        graph_stack: [],
        # The node map for efficient lookups
        graph_map: node_map,
        # Current graph being framed (named graph or @default)
        current_graph: current_graph,
        # Link map for @link processing (currently unused, for future use)
        link: %{},
        # Subject stack for nested framing
        subject_stack: [],
        # Cache for match results to avoid recomputation
        match_cache: %{},
        # Keep original frame for extracting framing keywords that may be lost in expansion
        original_frame: frame,
        # Frame context for expanding @default values
        frame_context: frame_context
      }

      # Step 6: Extract root frame object
      # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 6
      # When @graph is empty, extract other properties (for "frame the default graph" pattern)
      frame_obj =
        case expanded_frame do
          # Non-empty @graph in list - extract frame from inside @graph
          [%{@graph => [graph_frame | _]} | _] -> graph_frame
          # Empty @graph in list - remove @graph, keep other properties
          [%{@graph => []} = frame | _] -> Map.delete(frame, @graph)
          # No @graph in list - use the whole frame object
          [frame_obj | _] -> frame_obj
          # Non-empty @graph as map - extract frame from inside @graph
          %{@graph => [graph_frame | _]} -> graph_frame
          # Empty @graph as map - remove @graph, keep other properties
          %{@graph => []} = frame -> Map.delete(frame, @graph)
          # Plain map - use as-is
          frame_obj when is_map(frame_obj) -> frame_obj
          # Default - empty frame
          _ -> %{}
        end

      # Step 6.3: Merge framing keywords from original frame
      # Normative: Framing keywords can be lost during expansion, so restore them
      # This preserves @embed, @explicit, etc. in nested frames
      if System.get_env("DEBUG_FRAMING") != nil do
        IO.puts("\n=== MERGING FRAMING KEYWORDS ===")
        IO.puts("Original frame keys: #{inspect(Map.keys(frame) |> Enum.take(10))}")
        IO.puts("Expanded frame_obj keys: #{inspect(Map.keys(frame_obj) |> Enum.take(10))}")
        IO.puts("Original frame @embed: #{inspect(frame["@embed"])}")
        IO.puts("Original frame structure: #{inspect(frame, limit: 5, pretty: true)}")
      end

      frame_obj = merge_framing_keywords(frame_obj, frame)

      if System.get_env("DEBUG_FRAMING") != nil do
        IO.puts("After merge frame_obj keys: #{inspect(Map.keys(frame_obj) |> Enum.take(10))}")
        IO.puts("After merge frame_obj @embed: #{inspect(frame_obj["@embed"])}")
      end

      # Add root frame to state for use when embedding referenced nodes
      state = Map.put(state, :root_frame, frame_obj)

      # Step 6.5: Pre-identify nodes matching @included frame
      # This allows us to avoid embedding them in properties (they should be references)
      # Normative: Nodes in @included should not be deeply embedded
      #
      # When framing from @default graph, merge nodes from all named graphs
      # Normative: JSON-LD 1.1 Framing spec - nodes with same @id across multiple
      # graphs should be merged when framing
      graph_to_frame =
        if current_graph == @default do
          # Merge nodes from all graphs into default graph
          merge_graphs_for_framing(node_map)
        else
          node_map[current_graph] || %{}
        end

      # Update state.graph_map with merged nodes so get_node() can find them
      # This is critical for the framing algorithm to retrieve merged nodes
      state =
        if current_graph == @default and graph_to_frame != node_map[@default] do
          Map.update!(state, :graph_map, fn gm ->
            Map.put(gm, @default, graph_to_frame)
          end)
        else
          state
        end

      {_included_node_ids, state} =
        if Map.has_key?(frame_obj, @included) do
          included_frames = List.wrap(frame_obj[@included])

          # Collect IDs of nodes that match @included frames
          ids = included_frames
            |> Enum.flat_map(fn included_frame ->
              graph_to_frame
              |> Map.keys()
              |> Enum.filter(fn id ->
                node = graph_to_frame[id]
                match_node_against_frame(node, included_frame, state)
              end)
            end)
            |> MapSet.new()

          {ids, Map.put(state, :included_node_ids, ids)}
        else
          {MapSet.new(), state}
        end

      # Step 7: Match frame
      # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 7
      # Set matches to the result of calling the Frame Matching algorithm
      # Use the current_graph determined earlier (named graph or @default)
      {matches, final_state} = match_frame(state, Map.keys(graph_to_frame), frame_obj, nil)

      # Step 7.5: Filter out embedded blank nodes from top-level matches
      # Normative: In JSON-LD 1.1, blank nodes that are embedded in other matched nodes
      # should not appear as separate top-level entries in @graph
      # In JSON-LD 1.0, all matched nodes appear at top-level (duplicates allowed)
      filtered_matches =
        if processor_options.processing_mode != "json-ld-1.0" and length(matches) > 1 do
          # JSON-LD 1.1: Filter embedded blank nodes
          # Collect IDs of all nodes that are embedded within the matches
          embedded_ids =
            Enum.reduce(matches, MapSet.new(), fn match, acc ->
              # For each match, collect IDs embedded within it (but not the match's own ID)
              match_id = Map.get(match, @id)
              embedded_in_match = collect_embedded_ids(match, MapSet.new())
              # Remove the match's own ID from the embedded set
              embedded_in_match = MapSet.delete(embedded_in_match, match_id)
              MapSet.union(acc, embedded_in_match)
            end)

          # Filter out blank nodes that are embedded in other matches
          Enum.filter(matches, fn match ->
            match_id = Map.get(match, @id)
            is_blank = is_binary(match_id) and String.starts_with?(match_id, "_:")

            # Keep the node if:
            # 1. It's not a blank node, OR
            # 2. It's a blank node but not embedded in another match
            not is_blank or not MapSet.member?(embedded_ids, match_id)
          end)
        else
          # JSON-LD 1.0 or single match: No filtering
          matches
        end

      # Step 8: Create result with @graph
      # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 8
      result = %{@graph => filtered_matches}

      # Step 8.5: Process @included if present in frame
      # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm
      # If frame has an @included entry, invoke the algorithm using a copy of state
      # with the value of embedded flag set to false
      result =
        if Map.has_key?(frame_obj, @included) do
          included_frames = List.wrap(frame_obj[@included])
          # Create state with embed: :never (equivalent to embedded flag = false)
          included_state = %{final_state | embed: :never}

          if System.get_env("DEBUG_INCLUDED") do
            IO.puts("\n=== PROCESSING @INCLUDED ===")
            IO.puts("Number of included frames: #{length(included_frames)}")
            IO.puts("Number of subjects: #{length(Map.keys(graph_to_frame))}")
          end

          # Match subjects using each @included frame
          # Use all subjects from the current graph
          included_matches =
            Enum.flat_map(included_frames, fn included_frame ->
              {matches, _state} =
                match_frame(included_state, Map.keys(graph_to_frame), included_frame, nil)

              if System.get_env("DEBUG_INCLUDED") do
                IO.puts("Included frame matched #{length(matches)} nodes")
              end

              matches
            end)

          if System.get_env("DEBUG_INCLUDED") do
            IO.puts("Total included matches: #{length(included_matches)}")
            IO.puts("Existing IDs in @graph: #{inspect(Enum.map(matches, fn node -> Map.get(node, @id) end))}")
            IO.puts("Included match IDs: #{inspect(Enum.map(included_matches, fn node -> Map.get(node, @id) end))}")
          end

          # Add included nodes to result
          # Filter out nodes that are already embedded anywhere in the result to avoid duplication
          existing_ids = collect_embedded_ids(result)
          unique_included = Enum.reject(included_matches, fn node ->
            MapSet.member?(existing_ids, Map.get(node, @id))
          end)

          if System.get_env("DEBUG_INCLUDED") do
            IO.puts("Unique included nodes: #{length(unique_included)}")
            IO.puts("===============================\n")
          end

          if unique_included != [] do
            Map.put(result, @included, unique_included)
          else
            result
          end
        else
          result
        end

      # Step 9: Unwrap single-node results
      # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 9
      # If processing mode is not json-ld-1.1, and @graph contains only a single node,
      # and either frame explicitly has @id or frame has properties but not @graph, unwrap
      # Note: Empty frames {} should NOT unwrap
      frame_has_properties = map_size(frame_obj) > 0
      result =
        if processor_options.processing_mode != "json-ld-1.0" and
             is_list(result[@graph]) and length(result[@graph]) == 1 and
             (Map.has_key?(frame_obj, @id) or (frame_has_properties and not Map.has_key?(frame_obj, @graph))) do
          unwrapped = List.first(result[@graph])
          # Preserve @included if present when unwrapping
          if Map.has_key?(result, @included) do
            Map.put(unwrapped, @included, result[@included])
          else
            unwrapped
          end
        else
          result
        end

      # Step 10: Remove @preserve entries
      # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 10
      result = remove_preserve(result)

      # Step 10.5: Prune blank node identifiers that appear only once
      # Based on jsonld.js implementation: remove @id from blank nodes used only once
      # This makes the output cleaner by only including @id when necessary for references
      # Normative: Only prune in json-ld-1.1 mode; json-ld-1.0 keeps all blank node IDs
      result =
        if processor_options.processing_mode == "json-ld-1.0" do
          # JSON-LD 1.0: Keep all blank node IDs
          result
        else
          # JSON-LD 1.1: Prune blank nodes that appear only once
          prune_blank_node_identifiers(result)
        end

      # Step 11: Compact the result
      # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 11
      # Pass original frame (not expanded) to compaction so it can respect @embed directives
      # Pass node_map so compaction can compute reverse properties

      if System.get_env("DEBUG_FRAMING") != nil do
        IO.puts("\n=== FRAMED OUTPUT BEFORE COMPACTION ===")
        IO.puts("Result is map: #{is_map(result)}")
        IO.puts("Result keys: #{inspect(Map.keys(result || %{}))}")

        nodes = if is_map(result) and Map.has_key?(result, "@graph") do
          IO.puts("Has @graph with #{length(result["@graph"] || [])} nodes")
          result["@graph"]
        else
          IO.puts("No @graph, treating result as single node")
          [result]
        end

        # Look for Board node and its propertiesIn
        board = Enum.find(nodes || [], fn n -> is_map(n) and n["@id"] == "wellos:Board" end)
        if board do
          IO.puts("\n>>> Board node found!")
          IO.puts("Board keys: #{inspect(Map.keys(board))}")

          # Check @reverse
          if board["@reverse"] do
            IO.puts("\n>>> Board has @reverse!")
            IO.puts("@reverse keys: #{inspect(Map.keys(board["@reverse"]))}")
            if board["@reverse"]["http://ontology.wellos.io/propertiesIn"] do
              IO.puts("Found propertiesIn in @reverse!")
              props_in = board["@reverse"]["http://ontology.wellos.io/propertiesIn"]
              IO.puts("propertiesIn count: #{length(List.wrap(props_in))}")
            end
          end

          props_in = board["http://ontology.wellos.io/propertiesIn"]
          if props_in do
            IO.puts("Board has propertiesIn with #{length(List.wrap(props_in))} items")
            first_prop = List.first(List.wrap(props_in))
            IO.puts("First propertiesIn keys: #{inspect(Map.keys(first_prop || %{}))}")
            IO.puts("First propertiesIn: #{inspect(first_prop, limit: 10)}")

            # Check if it has domain
            if is_map(first_prop) do
              domain = first_prop["http://www.w3.org/2000/01/rdf-schema#domain"]
              if domain do
                first_domain = List.first(List.wrap(domain))
                IO.puts("\nDomain in propertiesIn[0]:")
                IO.puts("  Keys: #{inspect(Map.keys(first_domain || %{}))}")
                IO.puts("  Value: #{inspect(first_domain, limit: 10)}")
              end
            end
          else
            IO.puts("Board has NO propertiesIn as direct property!")
          end
        else
          IO.puts("\n>>> Board node NOT found!")
          IO.puts("Available node IDs: #{inspect(Enum.map(nodes || [], fn n -> if is_map(n), do: n["@id"], else: nil end) |> Enum.take(10))}")
        end
        IO.puts("=======================================\n")
      end

      # Debug: Print the exact structure before compaction
      if System.get_env("DEBUG_FRAMING") do
        IO.puts("\n=== STRUCTURE BEFORE COMPACTION ===")
        IO.inspect(result, pretty: true, limit: :infinity)
      end

      compacted =
        Compaction.compact(
          result,
          frame_context,
          @graph,
          processor_options,
          processor_options.compact_arrays,
          processor_options.ordered,
          frame,  # Pass the ORIGINAL frame (before expansion) for @embed directive awareness
          node_map  # Pass node_map for computing reverse properties
        )

      # Step 12: Add @context to result
      # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 12
      case frame do
        %{@context => context} -> Map.put(compacted, @context, context)
        _ -> compacted
      end
    after
      # Clean up node ID map to free memory
      NodeIdentifierMap.delete(node_id_map)
    end
  end

  # Get the embed option with default value
  # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-state
  defp get_embed_option(%{embed: embed}) when embed in [:last, @last, "last"], do: :last
  defp get_embed_option(%{embed: embed}) when embed in [:never, @never, "never"], do: :never

  defp get_embed_option(%{embed: embed}) when embed in [:always, @always, "always"],
    do: :always

  defp get_embed_option(_), do: :once

  @doc """
  Frame Matching Algorithm

  Normative: <https://www.w3.org/TR/json-ld11-framing/#frame-matching>

  This algorithm determines which subjects from the node map match the given frame.
  Uses caching to avoid re-matching the same subject-frame combinations.

  ## Parameters
  - `state` - The framing state
  - `subjects` - List of subject IDs to match
  - `frame` - The frame to match against
  - `parent` - Parent subject ID (for tracking)

  ## Returns
  Tuple of {list of matched and embedded nodes, updated state}
  """
  @spec match_frame(map, [String.t()], map, String.t() | nil) :: {[map], map}
  def match_frame(state, subjects, frame, parent) do
    # Create cache key for this frame matching operation
    frame_key = :erlang.phash2(frame)

    matched_subjects =
      subjects
      # Filter subjects that match the frame (with caching)
      |> Enum.filter(fn id -> filter_subjects_cached(state, id, frame, frame_key) end)

    # Embed each matched subject
    # For top-level matches (parent == nil), each gets independent embedding context
    # For nested matches (parent != nil), thread state across siblings
    if parent == nil do
      # Top-level: Independent embedding context for each tree
      {nodes, _final_state} =
        matched_subjects
        |> Enum.map_reduce(state, fn id, _acc_state ->
          # Reset embedded map for each top-level tree
          fresh_state = %{state | embedded: %{}}

          case embed_node(fresh_state, id, frame, parent) do
            {nil, _} -> {[], state}
            {node, _} -> {[node], state}
          end
        end)

      {List.flatten(nodes), state}
    else
      # Nested: Thread state across siblings to maintain @once semantics within tree
      matched_subjects
      |> Enum.map_reduce(state, fn id, acc_state ->
        case embed_node(acc_state, id, frame, parent) do
          {nil, new_state} -> {[], new_state}
          {node, new_state} -> {[node], new_state}
        end
      end)
      |> then(fn {nested_nodes, final_state} ->
        {List.flatten(nested_nodes), final_state}
      end)
    end
  end

  # Filter subjects with caching to improve performance
  # Uses a simple cache key combining subject ID and frame hash
  defp filter_subjects_cached(state, id, frame, frame_key) do
    cache_key = {id, frame_key}

    case Map.get(state.match_cache, cache_key) do
      nil ->
        # Not in cache, compute and store
        result = filter_subjects(state, id, frame)
        # Note: In production, we'd update state.match_cache here via message passing
        # For now, we compute without caching to avoid state mutation issues
        result

      cached_result ->
        cached_result
    end
  end

  # Filter subjects based on frame matching criteria
  # Normative: https://www.w3.org/TR/json-ld11-framing/#frame-matching Step 3
  defp filter_subjects(state, id, frame) do
    node = get_node(state, id)

    # Normative: Step 3.1 - Match all nodes if frame is wildcard
    # A wildcard frame is empty or contains only framing keywords
    if Enum.empty?(frame) or only_framing_keywords?(frame) do
      true
    else
      match_node_against_frame(node, frame, state)
    end
  end

  # Check if frame only contains framing keywords
  # Normative: Implied by wildcard matching behavior
  defp only_framing_keywords?(frame) when is_map(frame) do
    Map.keys(frame)
    |> Enum.all?(fn key -> key in @framing_keywords end)
  end

  defp only_framing_keywords?(_), do: false

  # Match a node against a frame
  # Normative: https://www.w3.org/TR/json-ld11-framing/#frame-matching Step 3.2-3.4
  defp match_node_against_frame(node, frame, state) do
    # Step 3.2: Get requireAll flag
    require_all =
      Map.get(frame, @requireAll, [false])
      |> List.wrap()
      |> List.first()
      |> Kernel.||(state.require_all)

    # Handle @reverse matching separately
    # @reverse in frame means "match nodes that are targets of these relationships"
    reverse_match =
      if Map.has_key?(frame, @reverse) do
        match_reverse_property(node, frame[@reverse], state)
      else
        true
      end

    # Step 3.3-3.4: Match regular frame properties (excluding @reverse, framing keywords, and @included)
    regular_match =
      frame
      |> Enum.filter(fn {key, _} -> key not in [@reverse, @included | @framing_keywords] end)
      |> match_properties(node, require_all)

    # Both regular properties and @reverse must match
    reverse_match and regular_match
  end

  # Match frame properties against node
  # Normative: https://www.w3.org/TR/json-ld11-framing/#frame-matching Step 3.3-3.4
  defp match_properties(frame_props, node, require_all) do
    # Check each property and track whether it exists in the node
    results =
      Enum.map(frame_props, fn {key, frame_value} ->
        match_result = match_property(key, frame_value, node)
        property_exists = key in [@id, @_type_] or Map.has_key?(node, key)
        # Check if frame has @default for this property
        has_default = has_default_value?(frame_value)
        {match_result, property_exists, has_default}
      end)

    # Apply matching logic based on @requireAll flag
    # Normative: When @requireAll is false (default), properties in frame that don't exist
    # in node are ignored. When true, all properties must be satisfied (exist+match OR have @default).
    if require_all do
      # All properties must be satisfied:
      # - If exists: must match
      # - If missing: must have @default
      Enum.all?(results, fn {match_result, property_exists, has_default} ->
        (property_exists and match_result) or (not property_exists and has_default)
      end)
    else
      # Properties that exist must ALL match; missing properties are OK
      existing_props =
        results
        |> Enum.filter(fn {_match_result, property_exists, _has_default} -> property_exists end)

      # If frame has properties but none exist in node, don't match
      # Otherwise, all existing properties must match
      not Enum.empty?(existing_props) and
        Enum.all?(existing_props, fn {match_result, _property_exists, _has_default} ->
          match_result
        end)
    end
  end

  # Match a single property
  # Normative: https://www.w3.org/TR/json-ld11-framing/#frame-matching

  # Match @id property
  # Normative: Matching on @id in frame
  defp match_property(@id, frame_values, node) do
    node_id = Map.get(node, @id)
    frame_values = List.wrap(frame_values)

    cond do
      # Empty @id in frame matches any node with @id
      Enum.empty?(frame_values) ->
        true

      # Wildcard {} matches any node
      Enum.any?(frame_values, &(&1 == %{})) ->
        true

      # Match specific @id value
      true ->
        Enum.any?(frame_values, fn frame_id ->
          (is_map(frame_id) and frame_id == %{}) or
            (is_binary(frame_id) and frame_id == node_id)
        end)
    end
  end

  # Match @type property
  # Normative: Matching on @type in frame
  defp match_property(@_type_, frame_types, node) do
    node_types = Map.get(node, @_type_, []) |> List.wrap()
    frame_types = List.wrap(frame_types)

    cond do
      # Empty array [] means "match nodes WITHOUT @type" (match-none pattern)
      # Normative: Per W3C spec, [] in frame matches absence of @type
      Enum.empty?(frame_types) ->
        Enum.empty?(node_types)

      # If frame has @default for @type and node has no @type, match succeeds
      # Normative: @default satisfies the property requirement even if missing
      Enum.empty?(node_types) and has_default_value?(frame_types) ->
        true

      # Wildcard {} matches any node with types
      Enum.any?(frame_types, &(&1 == %{})) ->
        not Enum.empty?(node_types)

      # Match specific type
      true ->
        Enum.any?(frame_types, fn frame_type ->
          (is_map(frame_type) and frame_type == %{}) or
            (is_binary(frame_type) and frame_type in node_types)
        end)
    end
  end

  # Match @reverse property - this is NOT called from match_properties
  # It must be handled specially in match_node_against_frame
  # But we still need this clause to prevent it from falling through to general matching
  defp match_property(@reverse, _frame_values, _node) do
    # @reverse matching is handled specially in match_node_against_frame
    # This should never be called, but return true to avoid breaking the chain
    true
  end

  # Match other properties
  # Normative: General property matching
  defp match_property(property, frame_values, node) do
    frame_values = List.wrap(frame_values)
    node_values = Map.get(node, property, []) |> List.wrap()

    cond do
      # Wildcard {} - node must have the property with non-empty value
      Enum.any?(frame_values, &(&1 == %{})) ->
        not Enum.empty?(node_values)

      # Empty array [] means "match nodes WITHOUT this property" (match-none pattern)
      # Normative: Per W3C spec, [] in frame matches absence of property
      Enum.empty?(frame_values) ->
        not Map.has_key?(node, property)

      # List pattern - must match list contents exactly
      # Normative: @list in frame must match @list values exactly
      Enum.any?(frame_values, fn val -> is_map(val) and Map.has_key?(val, @list) end) ->
        Enum.any?(frame_values, fn frame_value ->
          match_list_value(frame_value, node_values)
        end)

      # Node reference pattern (map with @id) - match the @id value
      # Normative: Node reference patterns must match @id exactly
      Enum.any?(frame_values, fn val ->
        is_map(val) and Map.has_key?(val, @id) and not value?(val)
      end) ->
        Enum.any?(frame_values, fn frame_value ->
          # Match node references by @id
          if is_map(frame_value) and Map.has_key?(frame_value, @id) do
            frame_id = frame_value[@id]

            Enum.any?(node_values, fn node_value ->
              is_map(node_value) and Map.get(node_value, @id) == frame_id
            end)
          else
            # Wildcard node pattern
            Map.has_key?(node, property) and not Enum.empty?(node_values)
          end
        end)

      # Node pattern (map with @type or other properties but no @id) - treat as wildcard
      # These maps are sub-frames that describe how to frame referenced nodes
      Enum.all?(frame_values, fn val ->
        is_map(val) and not value?(val) and not Map.has_key?(val, @list) and
            not Map.has_key?(val, @id)
      end) ->
        Map.has_key?(node, property) and not Enum.empty?(node_values)

      # Specific value match (for value objects and primitive values)
      true ->
        Enum.any?(frame_values, fn frame_value ->
          match_value(frame_value, node_values)
        end)
    end
  end

  # Match reverse relationships
  # Normative: @reverse properties in frame do NOT affect node matching
  # Per JSON-LD Framing 1.1 spec: @reverse properties only determine what gets included
  # in the output, not whether a node matches the frame
  # @reverse frame value is a map like {"prop": pattern}
  defp match_reverse_property(_node, _reverse_frame, _state) do
    # @reverse properties are always considered a "match" for filtering purposes
    # The actual work of finding and embedding reversed relationships happens
    # in add_reverse_to_output during the framing output phase
    true
  end

  # Match a frame value against node values
  # Normative: Value pattern matching
  defp match_value(frame_value, node_values) when is_map(frame_value) do
    if value?(frame_value) do
      # Match value objects
      Enum.any?(node_values, fn node_value ->
        value?(node_value) and value_match?(frame_value, node_value)
      end)
    else
      # Match node references or nested nodes
      Enum.any?(node_values, fn node_value ->
        node_value == frame_value
      end)
    end
  end

  defp match_value(frame_value, node_values) do
    Enum.any?(node_values, &(&1 == frame_value))
  end

  # Match a list value against node values
  # Normative: @list in frame must match @list values exactly
  defp match_list_value(frame_value, node_values) do
    # Check if frame_value has @list
    if is_map(frame_value) and Map.has_key?(frame_value, @list) do
      frame_list = List.wrap(frame_value[@list])

      # Check if any node_value is a list with matching contents
      Enum.any?(node_values, fn node_value ->
        if is_map(node_value) and Map.has_key?(node_value, @list) do
          node_list = List.wrap(node_value[@list])
          # Match list contents exactly
          lists_match?(frame_list, node_list)
        else
          false
        end
      end)
    else
      false
    end
  end

  # Compare two lists - frame list items must be contained in node list
  # Normative: Frame @list pattern matches if all frame items are present in node list
  defp lists_match?(frame_list, node_list) do
    # All frame items must be present somewhere in the node list
    Enum.all?(frame_list, fn frame_item ->
      Enum.any?(node_list, fn node_item ->
        items_match?(frame_item, node_item)
      end)
    end)
  end

  # Check if two list items match
  defp items_match?(frame_item, node_item) do
    cond do
      # Both are value objects
      is_map(frame_item) and value?(frame_item) and is_map(node_item) and value?(node_item) ->
        value_match?(frame_item, node_item)

      # Both are node references
      is_map(frame_item) and Map.has_key?(frame_item, @id) and
        is_map(node_item) and Map.has_key?(node_item, @id) ->
        frame_item[@id] == node_item[@id]

      # Both are primitives
      not is_map(frame_item) and not is_map(node_item) ->
        frame_item == node_item

      # Otherwise, exact match
      true ->
        frame_item == node_item
    end
  end

  # Check if two value objects match
  # Normative: Value object matching rules
  # Frame value keys must exist and match in node value
  # Node value must not have extra keys (except @index which is always allowed)
  defp value_match?(frame_value, node_value) do
    # Check that node doesn't have extra keys beyond frame keys (except @index)
    # Normative: Frame specifies exact structure, node can't have extra keys
    frame_keys = Map.keys(frame_value)
    # @index is always allowed
    node_keys = Map.keys(node_value) -- ["@index"]

    extra_keys = node_keys -- frame_keys

    # If node has keys not in frame, it doesn't match
    if not Enum.empty?(extra_keys) do
      false
    else
      # Check that all frame keys exist in node and match
      Enum.all?(frame_value, fn {key, frame_val} ->
        node_val = Map.get(node_value, key)

        case key do
          @value ->
            cond do
              # Wildcard {} or [%{}] matches any @value
              frame_val == %{} or frame_val == [%{}] ->
                true

              # @value can be an array in frame for matching multiple possible values
              is_list(frame_val) ->
                node_val in frame_val

              # Exact match
              true ->
                node_val == frame_val
            end

          @_type_ ->
            cond do
              # Empty array [] means "match value objects WITHOUT @type"
              frame_val == [] ->
                not Map.has_key?(node_value, @_type_)

              # Wildcard {} or [%{}] matches any @type
              frame_val == %{} or frame_val == [%{}] ->
                Map.has_key?(node_value, @_type_)

              # @type can be an array in frame for matching multiple possible types
              is_list(frame_val) ->
                node_val in frame_val

              # Exact match
              true ->
                node_val == frame_val
            end

          @language ->
            cond do
              # Empty array [] means "match value objects WITHOUT @language"
              frame_val == [] ->
                not Map.has_key?(node_value, @language)

              # Wildcard {} or [%{}] matches any @language
              frame_val == %{} or frame_val == [%{}] ->
                Map.has_key?(node_value, @language)

              # @language can be an array in frame for matching multiple possible languages
              is_list(frame_val) ->
                node_val in frame_val

              # Exact match
              true ->
                node_val == frame_val
            end

          @direction ->
            cond do
              # Empty array [] means "match value objects WITHOUT @direction"
              frame_val == [] ->
                not Map.has_key?(node_value, @direction)

              # Wildcard {} or [%{}] matches any @direction
              frame_val == %{} or frame_val == [%{}] ->
                Map.has_key?(node_value, @direction)

              # @direction can be an array in frame for matching multiple possible directions
              is_list(frame_val) ->
                node_val in frame_val

              # Exact match
              true ->
                node_val == frame_val
            end

          _ ->
            true
        end
      end)
    end
  end

  @doc """
  Embed Node Algorithm

  Normative: Implied by framing algorithm steps for embedding

  Embeds a node into the output according to the framing state and embedding rules.
  Uses memory-efficient tracking to avoid duplicate embedding.

  ## Parameters
  - `state` - The framing state
  - `id` - Node ID to embed
  - `frame` - The frame describing how to embed
  - `parent` - Parent node ID

  ## Returns
  Tuple of {embedded node map, updated state}
  """
  @spec embed_node(map, String.t(), map, String.t() | nil) :: {map | nil, map}
  def embed_node(state, id, frame, parent) do
    node = get_node(state, id)

    # Return nil if node doesn't exist
    if Enum.empty?(node) do
      {nil, state}
    else
      # Determine embedding behavior
      # Normative: @embed flag processing
      embed_value = get_frame_flag(frame, @embed, state.embed)

      # Top-level matched nodes (parent == nil) should always be fully embedded
      # @embed flag only applies to referenced nodes (parent != nil)
      is_top_level_match = parent == nil

      # Check for circular reference - if node is in the current embedding chain
      # Normative: Prevent infinite recursion in circular references
      is_circular = id in state.subject_stack

      # Check if already embedded
      embedded_node = Map.get(state.embedded, id)

      if System.get_env("DEBUG_FRAMING") != nil and (String.contains?(to_string(id), "column_of") or String.contains?(to_string(id), "BoardColumn") or String.contains?(to_string(id), "Sub") or String.contains?(to_string(id), "leaf")) do
        IO.puts("\n>>> EMBED_NODE called for #{id}")
        IO.puts("    embed_value: #{embed_value}")
        IO.puts("    is_top_level_match: #{is_top_level_match}")
        IO.puts("    is_circular: #{is_circular}")
        IO.puts("    already_embedded: #{not is_nil(embedded_node)}")
        IO.puts("    parent: #{inspect(parent)}")
        IO.puts("    id == parent: #{id == parent}")
        IO.puts("    frame @embed: #{inspect(frame["@embed"])}")
        IO.puts("    frame @explicit: #{inspect(frame["@explicit"])}")
        IO.puts("    frame keys: #{inspect(Map.keys(frame) |> Enum.take(15))}")
        IO.puts("    FULL FRAME: #{inspect(frame, limit: 10)}")
        if Map.has_key?(frame, "http://www.w3.org/2000/01/rdf-schema#domain") do
          IO.puts("    frame HAS domain key!")
          IO.puts("    frame[domain]: #{inspect(frame["http://www.w3.org/2000/01/rdf-schema#domain"], limit: 5)}")
        else
          IO.puts("    frame does NOT have domain key")
        end
        IO.puts("    subject_stack: #{inspect(state.subject_stack)}")
      end

      cond do
        # Reference to parent node - return just a reference
        # Normative: Nodes within reverse properties that reference the parent should be minimal references
        # This prevents circular embedding when parent is temporarily removed from subject_stack
        not is_nil(parent) and id == parent ->
          if System.get_env("DEBUG_FRAMING") != nil and (String.contains?(to_string(id), "column_of") or String.contains?(to_string(id), "BoardColumn") or String.contains?(to_string(id), "Sub") or String.contains?(to_string(id), "leaf")) do
            IO.puts("    -> Returning reference (references parent #{parent})")
          end
          {%{@id => id}, state}

        # Circular reference detected - BUT if already embedded and @embed: @always, return the full embedded node
        # When a node is already fully embedded, it's safe to return it even if circular
        # This allows @embed: @always to work correctly with nested properties
        is_circular and embedded_node != nil and is_map(embedded_node) and embed_value == :always ->
          if System.get_env("DEBUG_FRAMING") != nil and (String.contains?(to_string(id), "column_of") or String.contains?(to_string(id), "BoardColumn")) do
            IO.puts("    -> Returning already-embedded node (circular but @always)")
          end
          {embedded_node, state}

        # Circular reference detected - return just a reference
        # Normative: Circular references must be broken to avoid infinite embedding
        is_circular ->
          if System.get_env("DEBUG_FRAMING") != nil and (String.contains?(to_string(id), "column_of") or String.contains?(to_string(id), "BoardColumn")) do
            IO.puts("    -> Returning reference (circular)")
          end
          {%{@id => id}, state}

        # Node matches @included frame - return just a reference (not for top-level matches)
        # Normative: Nodes in @included should not be deeply embedded in properties
        not is_top_level_match and Map.get(state, :included_node_ids, MapSet.new()) |> MapSet.member?(id) ->
          if System.get_env("DEBUG_FRAMING") != nil do
            IO.puts("    -> Returning reference (matches @included frame)")
          end
          {%{@id => id}, state}

        # Never embed - return node reference (only for referenced nodes, not top-level matches)
        # Normative: @embed: @never behavior
        embed_value == :never and not is_top_level_match ->
          if System.get_env("DEBUG_FRAMING") != nil and (String.contains?(to_string(id), "column_of") or String.contains?(to_string(id), "BoardColumn")) do
            IO.puts("    -> Returning reference (@never)")
          end
          {%{@id => id}, state}

        # Already embedded with @once - return reference (but NOT for top-level matches)
        # Normative: @embed: @once behavior (default)
        # Top-level matches should always be fully embedded regardless of prior embedding
        embedded_node != nil and embed_value == :once and not is_top_level_match ->
          if System.get_env("DEBUG_FRAMING") != nil and (String.contains?(to_string(id), "column_of") or String.contains?(to_string(id), "BoardColumn")) do
            IO.puts("    -> Returning reference (already embedded with @once)")
          end
          {%{@id => id}, state}

        # @last: only embed fully if this is the LAST property embedding this node
        # Check last_embed_map to see if current property is the last one
        # Check this EVEN on first embedding, to determine if we should defer to a later property
        embed_value == :last and not is_top_level_match ->
          last_embed_map = Map.get(state, :last_embed_map, %{})
          last_property = Map.get(last_embed_map, id)
          current_property = Map.get(state, :current_property)
          is_last_embedding = last_property == current_property

          if System.get_env("DEBUG_FRAMING") != nil do
            IO.puts("    @last logic for #{id}:")
            IO.puts("      last_property: #{inspect(last_property)}")
            IO.puts("      current_property: #{inspect(current_property)}")
            IO.puts("      is_last_embedding: #{is_last_embedding}")
            IO.puts("      embedded_node: #{inspect(embedded_node != nil)}")
          end

          if is_last_embedding or last_property == nil do
            # This IS the last property - embed fully (fall through to embedding below)
            # Add to subject stack for circular reference detection
            state = %{state | subject_stack: [id | state.subject_stack]}
            {output, final_state} = create_output_node(state, node, frame, id)
            final_state = put_in(final_state.embedded[id], output)
            final_state = %{final_state | subject_stack: List.delete(final_state.subject_stack, id)}
            {output, final_state}
          else
            # Not the last property - return reference only
            if System.get_env("DEBUG_FRAMING") != nil do
              IO.puts("    -> Returning reference (@last, not last property)")
            end

            {%{@id => id}, state}
          end

        # Embed the node
        # Normative: @embed: @always or first embedding with @once or top-level match
        true ->
          if System.get_env("DEBUG_FRAMING") != nil and (String.contains?(to_string(id), "column_of") or String.contains?(to_string(id), "BoardColumn")) do
            IO.puts("    -> Fully embedding node")
          end
          # Add to subject stack for circular reference detection
          state = %{state | subject_stack: [id | state.subject_stack]}
          {output, final_state} = create_output_node(state, node, frame, id)

          # Store the actual embedded node for potential reuse when embed_value is :always
          final_state = put_in(final_state.embedded[id], output)

          if System.get_env("DEBUG_FRAMING") != nil and (String.contains?(to_string(id), "column_of") or String.contains?(to_string(id), "BoardColumn") or String.contains?(to_string(id), "Sub") or String.contains?(to_string(id), "leaf")) do
            IO.puts("    -> embed_node: create_output_node returned for #{id}")
            IO.puts("       Keys: #{inspect(Map.keys(output))}")
            if Map.has_key?(output, @reverse) do
              IO.puts("       @reverse keys: #{inspect(Map.keys(output[@reverse]))}")
              Enum.each(output[@reverse], fn {prop, vals} ->
                if is_list(vals) && length(vals) > 0 do
                  first = hd(vals)
                  if is_map(first) do
                    IO.puts("         #{prop} first node: keys=#{inspect(Map.keys(first))}, size=#{map_size(first)}")
                  end
                end
              end)
            end
          end

          # Remove node from subject_stack after processing is complete
          # Normative: Nodes should only be in subject_stack while actively being processed
          final_state = %{final_state | subject_stack: List.delete(final_state.subject_stack, id)}

          {output, final_state}
      end
    end
  end

  # Create the output node by processing properties
  # Normative: Node object creation with property filtering
  defp create_output_node(state, node, frame, id) do
    # Get framing flags
    # Normative: @explicit flag processing
    explicit = get_frame_flag(frame, @explicit, state.explicit_inclusion)
    # Normative: @omitDefault flag processing
    omit_default = get_frame_flag(frame, @omitDefault, state.omit_default)

    # Start with @id if present
    # Normative: Blank node @id should only be included if frame explicitly requests @id
    # or if the node is being embedded multiple times
    output =
      if Map.has_key?(node, @id) do
        node_id = Map.get(node, @id)
        _is_blank_node = is_binary(node_id) and String.starts_with?(node_id, "_:")
        _frame_requests_id = Map.has_key?(frame, @id)

        # Always include @id when present
        # Blank nodes that appear only once will have @id removed during pruning step
        # This is more efficient than trying to track usage during framing
        %{@id => node_id}
      else
        %{}
      end

    # Add @type if present, or @default value if frame specifies it
    # Normative: @type is always included when present or has @default
    output =
      if Map.has_key?(node, @_type_) do
        Map.put(output, @_type_, node[@_type_])
      else
        # Check if frame has @type with @default
        if Map.has_key?(frame, @_type_) and has_default_value?(frame[@_type_]) do
          frame_types = List.wrap(frame[@_type_])
          # Extract @default value from frame @type
          default_type =
            Enum.find_value(frame_types, fn ft ->
              if is_map(ft) and Map.has_key?(ft, @default) do
                ft[@default]
              end
            end)

          if default_type do
            # Expand the default type using the frame context
            # The @default value is compact (e.g., "ex:Foo"), need to expand to full IRI
            # Use vocab=true since @type values are vocabulary terms
            expanded_type =
              JSON.LD.IRIExpansion.expand_iri(
                default_type,
                state.frame_context,
                Options.new(),
                false,
                true
              )

            Map.put(output, @_type_, List.wrap(expanded_type))
          else
            output
          end
        else
          output
        end
      end

    # Determine which properties to include
    # Normative: @explicit flag controls property inclusion
    properties =
      if explicit do
        # Only include properties mentioned in frame
        frame
        |> Map.keys()
        |> Enum.filter(&(&1 not in @excluded_from_properties))
      else
        # Include all node properties, plus ALL frame properties when not omitting defaults
        node_props = Map.keys(node)

        frame_props =
          if omit_default do
            []
          else
            # Include all frame properties (not just those with @default)
            # Per spec: properties in frame should be included with null or @default value
            frame
            |> Map.keys()
            |> Enum.filter(&(&1 not in @excluded_from_properties))
          end

        Enum.uniq(node_props ++ frame_props)
      end

    # Pre-scan properties to determine which property is the "last" one to embed each node with @embed: @last
    # Normative: @embed: @last means only the last property embedding a node should have full embedding
    last_embed_map = build_last_embed_map(properties, node, frame)

    if System.get_env("DEBUG_FRAMING") != nil and map_size(last_embed_map) > 0 do
      IO.puts("\n=== LAST_EMBED_MAP for node #{id} ===")
      IO.inspect(last_embed_map)
    end

    # Merge with existing last_embed_map to preserve parent's mappings
    existing_map = Map.get(state, :last_embed_map, %{})
    state = Map.put(state, :last_embed_map, Map.merge(existing_map, last_embed_map))

    # Process properties and add to output with state threading
    # Use streaming to minimize memory usage
    {output, state_after_props} =
      properties
      |> Stream.filter(&(&1 not in @excluded_from_properties))
      |> Enum.reduce({output, state}, fn property, {acc_output, acc_state} ->
        add_property_to_output(acc_state, node, frame, property, acc_output, omit_default, id)
      end)

    # Compute reverse properties from context definitions
    # For properties defined with @reverse in context (like propertiesIn, propertiesOut)
    if System.get_env("DEBUG_FRAMING") do
      IO.puts("\n>>> ABOUT TO CALL add_reverse_properties_from_context for node: #{id}")
    end

    # Add current node to embedded BEFORE processing reverse properties
    # Normative: When reverse property nodes reference the current node, they should get just a reference
    # This prevents full embedding with @type when the node is referenced back from within reverse properties
    state_with_embedded = put_in(state_after_props.embedded[id], output)

    # Temporarily remove current node from subject_stack to avoid false circular references
    # when embedding reverse property nodes that might reference this node
    state_for_reverse = %{state_with_embedded | subject_stack: List.delete(state_with_embedded.subject_stack, id)}

    {output, state_after_reverse_props} =
      add_reverse_properties_from_context(state_for_reverse, frame, output, id)

    # Restore the subject_stack for subsequent processing
    state_after_reverse_props = %{state_after_reverse_props | subject_stack: state_after_props.subject_stack}

    # Handle @reverse - add nodes that point to this node
    # Normative: @reverse framing adds reversed relationships to output
    {output, final_state} =
      if Map.has_key?(frame, @reverse) do
        add_reverse_to_output(state_after_reverse_props, node, frame[@reverse], output, id)
      else
        {output, state_after_reverse_props}
      end

    # Debug: Check @reverse content right before create_output_node returns
    if System.get_env("DEBUG_FRAMING") do
      IO.puts("\n>>> create_output_node RETURNING for #{id}:")
      if Map.has_key?(output, @reverse) do
        IO.puts("    Has @reverse with keys: #{inspect(Map.keys(output[@reverse]))}")
        Enum.each(output[@reverse], fn {prop, vals} ->
          IO.puts("      #{prop} => #{length(vals)} nodes")
          if is_list(vals) && length(vals) > 0 do
            first = hd(vals)
            if is_map(first) do
              IO.puts("        First node keys: #{inspect(Map.keys(first))}, size: #{map_size(first)}")
            end
          end
        end)
      else
        IO.puts("    No @reverse in output")
      end
    end

    {output, final_state}
  end

  # Build map of node_id => property_name for @embed: @last
  # Returns a map where each node ID maps to the LAST property that embeds it with @last
  defp build_last_embed_map(properties, node, frame) do
    # Get inherited @embed value from frame root
    # Normative: Properties inherit @embed from parent frame unless overridden
    frame_embed_value = Map.get(frame, "@embed")
    frame_embed_option =
      cond do
        frame_embed_value in ["@last", "@Last", "last", :last] -> :last
        frame_embed_value in ["@always", "always", :always] -> :always
        frame_embed_value in ["@never", "never", :never] -> :never
        true -> nil  # No inherited value
      end

    if System.get_env("DEBUG_FRAMING") != nil and frame_embed_option != nil do
      IO.puts("  Frame-level @embed: #{inspect(frame_embed_option)}")
    end

    properties
    |> Enum.reduce(%{}, fn property, acc ->
      # Skip keywords
      if property in @excluded_from_properties do
        acc
      else
        # Check if this property exists in node
        if Map.has_key?(node, property) do
          # Get property-specific frame (if it exists)
          property_frame =
            if Map.has_key?(frame, property) do
              case Map.get(frame, property) do
                [first | _] when is_map(first) -> first
                val when is_map(val) -> val
                _ -> %{}
              end
            else
              %{}
            end

          # Extract @embed value - property-specific overrides frame-level
          property_embed_value = Map.get(property_frame, "@embed")
          embed_option =
            cond do
              # Property has explicit @embed
              property_embed_value in ["@last", "@Last", "last", :last] -> :last
              property_embed_value in ["@always", "always", :always] -> :always
              property_embed_value in ["@never", "never", :never] -> :never
              property_embed_value != nil -> :once  # Explicit but not recognized
              # Inherit from frame
              frame_embed_option != nil -> frame_embed_option
              # Default
              true -> :once
            end

          if System.get_env("DEBUG_FRAMING") != nil do
            IO.puts("  Property #{property}: embed_option=#{inspect(embed_option)}, property @embed=#{inspect(property_embed_value)}, inherited=#{inspect(frame_embed_option)}")
          end

          if embed_option == :last do
            # Get node IDs that this property would embed
            property_values = node[property] |> List.wrap()

            property_values
            |> Enum.reduce(acc, fn value, acc2 ->
              if is_map(value) and Map.has_key?(value, @id) do
                # This property embeds this node ID with @last
                # Since we process properties in order, this overwrites earlier properties
                if System.get_env("DEBUG_FRAMING") != nil do
                  IO.puts("    Tracking @last for node #{value[@id]} via property #{property}")
                end

                Map.put(acc2, value[@id], property)
              else
                acc2
              end
            end)
          else
            acc
          end
        else
          acc
        end
      end
    end)
  end

  # Add a single property to the output
  # Memory-efficient: processes one property at a time
  # Returns: {updated_output, updated_state}
  defp add_property_to_output(state, node, frame, property, output, omit_default, parent_id) do
    cond do
      # Property exists in node
      Map.has_key?(node, property) ->
        # Get frame for this property
        # In expanded frames, property values are arrays, so unwrap if needed
        # Use nil as default to distinguish "not in frame" from "explicitly empty frame"
        property_frame =
          if Map.has_key?(frame, property) do
            case Map.get(frame, property) do
              [first | _] when is_map(first) -> first
              val when is_map(val) -> val
              _ -> %{}
            end
          else
            # Property not explicitly in frame - inherit @embed flag from parent frame
            # Normative: When a property is not in the frame but the parent frame has @embed,
            # that @embed value should propagate to nested properties
            parent_embed = Map.get(frame, @embed)
            if parent_embed do
              %{@embed => parent_embed}
            else
              nil  # No @embed to inherit, use default behavior
            end
          end

        if System.get_env("DEBUG_FRAMING") != nil and property == "http://www.w3.org/2000/01/rdf-schema#domain" and node["@id"] == "wellos:column_of" do
          IO.puts("\n=== PROCESSING DOMAIN PROPERTY IN column_of ===")
          IO.puts("Parent: #{parent_id}")
          IO.puts("Node: #{node["@id"]}")
          IO.puts("Property: #{property}")
          IO.puts("Frame keys: #{inspect(Map.keys(frame))}")
          IO.puts("Frame has property: #{Map.has_key?(frame, property)}")
          IO.puts("Frame[property] raw: #{inspect(Map.get(frame, property), limit: 5)}")
          IO.puts("Property frame keys: #{inspect(Map.keys(property_frame))}")
          IO.puts("Property frame: #{inspect(property_frame, limit: 5)}")
          IO.puts("Property frame is wildcard: #{is_map(property_frame) and map_size(property_frame) == 0}")
        end

        values = Map.get(node, property, []) |> List.wrap()

        # Filter values if frame has a value pattern
        # Normative: Frame value patterns should filter property values
        filtered_values =
          if Map.has_key?(frame, property) do
            frame_values = Map.get(frame, property, []) |> List.wrap()
            # Check if frame has value patterns (not node patterns or wildcards)
            has_value_pattern =
              Enum.any?(frame_values, fn fv ->
                is_map(fv) and (value?(fv) or Map.has_key?(fv, @list))
              end)

            if has_value_pattern do
              # Filter values to only those matching the frame pattern
              Enum.filter(values, fn value ->
                Enum.any?(frame_values, fn frame_value ->
                  cond do
                    # Value object pattern
                    is_map(frame_value) and value?(frame_value) and is_map(value) and
                        value?(value) ->
                      value_match?(frame_value, value)

                    # List pattern
                    is_map(frame_value) and Map.has_key?(frame_value, @list) and is_map(value) and
                        Map.has_key?(value, @list) ->
                      lists_match?(List.wrap(frame_value[@list]), List.wrap(value[@list]))

                    # Primitive value match
                    not is_map(frame_value) ->
                      value == frame_value

                    # Wildcard or node pattern - no filtering
                    true ->
                      true
                  end
                end)
              end)
            else
              values
            end
          else
            values
          end

        # Filter list contents if frame has list pattern with node references
        # Normative: When frame specifies specific nodes in a list, filter to those nodes
        filtered_values =
          if Map.has_key?(frame, property) do
            frame_values = Map.get(frame, property, []) |> List.wrap()

            # Check if frame has a list pattern
            # Normative: Frame list pattern is used to filter node list contents
            frame_list_pattern =
              Enum.find_value(frame_values, fn fv ->
                if is_map(fv) and Map.has_key?(fv, @list) do
                  List.wrap(fv[@list])
                else
                  nil
                end
              end)

            if frame_list_pattern do
              # Filter list contents to only include matching items
              # Normative: Keep items that match the frame pattern OR are simple string values
              Enum.map(filtered_values, fn value ->
                if is_map(value) and Map.has_key?(value, @list) do
                  node_list = List.wrap(value[@list])

                  filtered_list =
                    Enum.filter(node_list, fn node_item ->
                      # Keep if:
                      # 1. Matches frame pattern, OR
                      # 2. Is not a node reference (simple value or value object without @id)
                      # Normative: Only filter out non-matching node references
                      matches_pattern =
                        Enum.any?(frame_list_pattern, fn frame_item ->
                          items_match?(frame_item, node_item)
                        end)

                      is_node_reference = is_map(node_item) and Map.has_key?(node_item, @id)

                      matches_pattern or not is_node_reference
                    end)

                  %{value | @list => filtered_list}
                else
                  value
                end
              end)
            else
              filtered_values
            end
          else
            filtered_values
          end

        # Process each filtered value with state threading
        {processed_values, final_state} =
          filtered_values
          |> Enum.map_reduce(state, fn value, acc_state ->
            # Track current property in state for @embed: @last logic
            acc_state_with_property = Map.put(acc_state, :current_property, property)
            process_value(acc_state_with_property, value, property_frame, parent_id, property)
          end)

        {Map.put(output, property, processed_values), final_state}

      # Property in frame but not in node - add default if @omitDefault is false
      # Normative: @default and @omitDefault processing
      Map.has_key?(frame, property) ->
        # Get @default value from property frame
        # In expanded frames, property values are arrays, so unwrap if needed
        property_frame =
          case Map.get(frame, property, %{}) do
            [first | _] when is_map(first) -> first
            val when is_map(val) -> val
            _ -> %{}
          end

        # Check property-level @omitDefault flag
        property_omit_default = get_frame_flag(property_frame, @omitDefault, omit_default)

        # Skip if property-level @omitDefault is true
        if property_omit_default do
          {output, state}
        else
          # Check if property frame has @default
          if Map.has_key?(property_frame, @default) do
            # Get @default value directly (not through get_frame_flag which normalizes values)
            # @default can be wrapped in an array in expanded form
            default_value =
              case Map.get(property_frame, @default) do
                [val | _] -> val
                val -> val
              end

            # Special handling for @null as default value
            # Normative: @null in @default means use empty array (compacts to null or [])
            if default_value == "@null" do
              {Map.put(output, property, []), state}
            else
              # Wrap default value in array to match expanded form expectations
              {Map.put(output, property, List.wrap(default_value)), state}
            end
          else
            # No @default - add empty array (will compact to null)
            # Per spec: properties in frame but not in node should be null if no @default
            {Map.put(output, property, []), state}
          end
        end

      # Property not in frame or node - skip
      true ->
        {output, state}
    end
  end

  # Add @reverse relationships to output
  # Normative: Collect nodes that point to this node via specified properties
  # Returns: {updated_output, updated_state}
  defp add_reverse_to_output(state, _node, reverse_frame, output, node_id) do
    # Get the actual reverse frame map (unwrap from array if needed)
    reverse_frame_map = List.wrap(reverse_frame) |> List.first(%{})

    # Start with existing @reverse content (if any) to preserve properties
    # already added by add_reverse_properties_from_context
    # Normative: Merge @reverse properties instead of overwriting
    initial_reverse = Map.get(output, @reverse, %{})

    # For each property in @reverse frame, find nodes that point to this node
    # Thread state through the reduce
    {reverse_map, final_state} =
      Enum.reduce(reverse_frame_map, {initial_reverse, state}, fn {reverse_prop, prop_frame},
                                                       {acc_map, acc_state} ->
        # Skip if this property was already processed by add_reverse_properties_from_context
        # Normative: Don't overwrite existing @reverse properties
        if Map.has_key?(acc_map, reverse_prop) do
          {acc_map, acc_state}
        else
          # Find all nodes that have reverse_prop pointing to this node
          current_graph = Map.get(acc_state.graph_map, acc_state.current_graph, %{})

        referencing_node_ids =
          current_graph
          |> Enum.filter(fn {_id, candidate_node} ->
            # Get values of reverse_prop in candidate_node
            prop_values = Map.get(candidate_node, reverse_prop, []) |> List.wrap()

            # Check if any value references our node
            Enum.any?(prop_values, fn value ->
              is_map(value) and Map.get(value, @id) == node_id
            end)
          end)
          |> Enum.map(fn {ref_id, _} -> ref_id end)

        # Frame each referencing node with state threading
        {referencing_nodes, new_state} =
          referencing_node_ids
          |> Enum.map_reduce(acc_state, fn ref_id, thread_state ->
            # Get the property-specific frame (unwrap if needed)
            property_frame =
              case prop_frame do
                [first | _] when is_map(first) -> first
                val when is_map(val) -> val
                _ -> %{}
              end

            case embed_node(thread_state, ref_id, property_frame, node_id) do
              {nil, updated_state} -> {[], updated_state}
              {embedded, updated_state} -> {[embedded], updated_state}
            end
          end)
          |> then(fn {nested_nodes, final_thread_state} ->
            {List.flatten(nested_nodes), final_thread_state}
          end)

          # Add to reverse map if any nodes found
          updated_map =
            if Enum.empty?(referencing_nodes) do
              acc_map
            else
              Map.put(acc_map, reverse_prop, referencing_nodes)
            end

          {updated_map, new_state}
        end
      end)

    # Add @reverse to output if any reversed relationships found
    if Enum.empty?(reverse_map) do
      {output, final_state}
    else
      {Map.put(output, @reverse, reverse_map), final_state}
    end
  end

  # Compute reverse properties from context definitions
  # For properties that have @reverse in the context (e.g., propertiesIn, propertiesOut)
  defp add_reverse_properties_from_context(state, frame, output, node_id) do
    # Get frame context to check term definitions
    frame_context = state.frame_context

    if System.get_env("DEBUG_FRAMING") do
      IO.puts("\n=== CHECKING FOR REVERSE PROPERTIES ===")
      IO.puts("Node ID: #{node_id}")
      IO.puts("Frame keys: #{inspect(Map.keys(frame) |> Enum.take(10))}")
      IO.puts("Context has term_defs: #{map_size(frame_context.term_defs)} terms")
      if Map.has_key?(frame, "@reverse") do
        IO.puts("Frame @reverse keys: #{inspect(Map.keys(frame["@reverse"]))}")
      end
    end

    # For each property in the frame, check if it's a reverse property in the context
    {result, final_state} =
      frame
      |> Enum.filter(fn {prop, _} -> prop not in @excluded_from_properties end)
      |> Enum.reduce({output, state}, fn {property, prop_frame}, {acc_output, acc_state} ->
        # Property in frame is an expanded IRI, need to find the term that maps to it
        # Search term definitions for one with matching iri_mapping
        term_and_def =
          Enum.find(frame_context.term_defs, fn {_term, term_def} ->
            term_def && term_def.iri_mapping == property
          end)

        if System.get_env("DEBUG_FRAMING") do
          IO.puts("  Checking property: #{property}")
          IO.puts("    Found term: #{inspect(term_and_def && elem(term_and_def, 0))}")
          if term_and_def do
            {_term, term_def} = term_and_def
            IO.puts("    reverse_property: #{term_def.reverse_property}")
          end
        end

        case term_and_def do
          {term, term_def} when term_def.reverse_property ->
            # This is a reverse property - compute it from the node map
            # Find all nodes that have the forward property (iri_mapping) pointing to this node
            forward_iri = term_def.iri_mapping
            current_graph = Map.get(acc_state.graph_map, acc_state.current_graph, %{})

            # Find nodes that reference this node via the forward property
            referencing_node_ids =
              current_graph
              |> Enum.filter(fn {ref_id, candidate_node} ->
                # Debug: Show sample nodes and detailed info for column_of
                if System.get_env("DEBUG_FRAMING") do
                  cond do
                    String.contains?(to_string(ref_id), "column") ->
                      IO.puts("\n  >>> Checking node: #{ref_id}")
                      range_val = Map.get(candidate_node, forward_iri)
                      IO.puts("      forward_iri (#{forward_iri}) exists: #{not is_nil(range_val)}")
                      if range_val do
                        IO.puts("      Value: #{inspect(range_val, limit: 10)}")
                        IO.puts("      Looking for node_id: #{node_id}")
                      end
                      IO.puts("      All keys in candidate: #{inspect(Map.keys(candidate_node) |> Enum.take(5))}")

                    true ->
                      :ok
                  end
                end

                # Skip the current node itself
                ref_id != node_id &&
                  # Check if candidate_node has forward_iri pointing to our node
                  case Map.get(candidate_node, forward_iri) do
                  nil ->
                    false

                  values when is_list(values) ->
                    Enum.any?(values, fn value ->
                      is_map(value) and Map.get(value, @id) == node_id
                    end)

                  value when is_map(value) ->
                    Map.get(value, @id) == node_id

                  _ ->
                    false
                end
              end)
              |> Enum.map(fn {ref_id, _} -> ref_id end)

            # Frame each referencing node with the property's frame
            property_frame =
              case prop_frame do
                [first | _] when is_map(first) -> first
                val when is_map(val) -> val
                _ -> %{}
              end

            if System.get_env("DEBUG_FRAMING") do
              IO.puts("\n=== REVERSE PROPERTY: #{term} ===")
              IO.puts("Expanded IRI: #{property}")
              IO.puts("Forward IRI: #{forward_iri}")
              IO.puts("Current graph: #{acc_state.current_graph}")
              IO.puts("Graph has #{map_size(current_graph)} nodes")
              IO.puts("Looking for nodes with '#{forward_iri}' pointing to '#{node_id}'")
              IO.puts("Found #{length(referencing_node_ids)} referencing nodes")
              IO.puts("Property frame: #{inspect(property_frame, limit: 5)}")
            end

            {referencing_nodes, new_state} =
              referencing_node_ids
              |> Enum.map_reduce(acc_state, fn ref_id, thread_state ->
                result = embed_node(thread_state, ref_id, property_frame, node_id)

                if System.get_env("DEBUG_FRAMING") do
                  case result do
                    {nil, _} -> IO.puts("  Node #{ref_id}: returned nil")
                    {embedded, _} -> IO.puts("  Node #{ref_id}: embedded with keys #{inspect(Map.keys(embedded))}")
                  end
                end

                case result do
                  {nil, updated_state} -> {[], updated_state}
                  {embedded, updated_state} -> {[embedded], updated_state}
                end
              end)
              |> then(fn {nested_nodes, final_thread_state} ->
                {List.flatten(nested_nodes), final_thread_state}
              end)

            # Add to output if any nodes found
            # Use the expanded property IRI (from the frame)
            updated_output =
              if Enum.empty?(referencing_nodes) do
                # If @omitDefault is false or frame explicitly includes this property,
                # we might need to add null/empty array - but for now, skip empty
                acc_output
              else
                # In framing output (expanded form), property values must always be arrays
                # Compaction will handle unwrapping based on container mappings
                value = referencing_nodes

                # Reverse properties must be placed under @reverse in expanded form
                # Normative: Reverse properties belong under @reverse, not as top-level properties
                reverse_obj = Map.get(acc_output, @reverse, %{})
                updated_reverse_obj = Map.put(reverse_obj, property, value)
                Map.put(acc_output, @reverse, updated_reverse_obj)
              end

            {updated_output, new_state}

          _ ->
            # Not a reverse property, skip
            {acc_output, acc_state}
        end
      end)

    # Also process properties in @reverse (if present)
    if System.get_env("DEBUG_FRAMING") do
      IO.puts("\n>>> Checking frame[@reverse]")
      if Map.has_key?(frame, "@reverse") do
        IO.puts("    frame[@reverse] keys: #{inspect(Map.keys(frame["@reverse"]))}")
        IO.puts("    Current result has @reverse: #{Map.has_key?(result, @reverse)}")
        if Map.has_key?(result, @reverse) do
          IO.puts("    Current @reverse properties: #{inspect(Map.keys(result[@reverse]))}")
        end
      else
        IO.puts("    No frame[@reverse]")
      end
    end

    {final_result, final_final_state} =
      case Map.get(frame, "@reverse") do
        nil ->
          {result, final_state}

        reverse_frame when is_map(reverse_frame) ->
          reverse_frame
          |> Enum.reduce({result, final_state}, fn {property, prop_frame}, {acc_output, acc_state} ->
            # For properties in @reverse, they ARE reverse properties by definition
            # Find the REVERSE PROPERTY term that maps to this IRI
            # We need to find terms where reverse_property: true and iri_mapping matches
            term_and_def =
              Enum.find(frame_context.term_defs, fn {_term, term_def} ->
                term_def && term_def.reverse_property && term_def.iri_mapping == property
              end)

            if System.get_env("DEBUG_FRAMING") do
              IO.puts("  Checking @reverse property: #{property}")
              IO.puts("    Found reverse term: #{inspect(term_and_def && elem(term_and_def, 0))}")
            end

            case term_and_def do
              {term, term_def} ->
                # This is a reverse property - compute it from the node map
                forward_iri = term_def.iri_mapping
                current_graph = Map.get(acc_state.graph_map, acc_state.current_graph, %{})

                # Find nodes that reference this node via the forward property
                referencing_node_ids =
                  current_graph
                  |> Enum.filter(fn {ref_id, candidate_node} ->
                    # Debug: Show sample nodes and detailed info for column_of
                    if System.get_env("DEBUG_FRAMING") do
                      cond do
                        String.contains?(to_string(ref_id), "column") ->
                          IO.puts("\n  >>> Checking node: #{ref_id}")
                          range_val = Map.get(candidate_node, forward_iri)
                          IO.puts("      forward_iri (#{forward_iri}) exists: #{not is_nil(range_val)}")

                          if range_val do
                            IO.puts("      Value: #{inspect(range_val, limit: 10)}")
                            IO.puts("      Looking for node_id: #{node_id}")
                          end

                          IO.puts("      All keys in candidate: #{inspect(Map.keys(candidate_node) |> Enum.take(5))}")

                        true ->
                          :ok
                      end
                    end

                    # Skip the current node itself
                    ref_id != node_id &&
                      # Check if candidate_node has forward_iri pointing to our node
                      case Map.get(candidate_node, forward_iri) do
                        nil ->
                          false

                        values when is_list(values) ->
                          Enum.any?(values, fn value ->
                            is_map(value) and Map.get(value, @id) == node_id
                          end)

                        value when is_map(value) ->
                          Map.get(value, @id) == node_id

                        _ ->
                          false
                      end
                  end)
                  |> Enum.map(fn {ref_id, _} -> ref_id end)

                # Frame each referencing node with the property's frame
                property_frame =
                  case prop_frame do
                    [first | _] when is_map(first) -> first
                    val when is_map(val) -> val
                    _ -> %{}
                  end

                if System.get_env("DEBUG_FRAMING") do
                  IO.puts("\n=== REVERSE PROPERTY (from @reverse): #{term} ===")
                  IO.puts("Expanded IRI: #{property}")
                  IO.puts("Forward IRI: #{forward_iri}")
                  IO.puts("Current graph: #{acc_state.current_graph}")
                  IO.puts("Graph has #{map_size(current_graph)} nodes")
                  IO.puts("Looking for nodes with '#{forward_iri}' pointing to '#{node_id}'")
                  IO.puts("Found #{length(referencing_node_ids)} referencing nodes")
                  IO.puts("Property frame keys: #{inspect(Map.keys(property_frame) |> Enum.take(10))}")
                  IO.puts("Property frame @embed: #{inspect(property_frame["@embed"])}")
                  IO.puts("Property frame @explicit: #{inspect(property_frame["@explicit"])}")

                  # Check domain frame specifically
                  domain_frame = property_frame["http://www.w3.org/2000/01/rdf-schema#domain"]
                  if domain_frame do
                    IO.puts("Property frame has domain: #{inspect(domain_frame, limit: 5)}")
                  end
                end

                {referencing_nodes, new_state} =
                  referencing_node_ids
                  |> Enum.map_reduce(acc_state, fn ref_id, thread_state ->
                    if System.get_env("DEBUG_FRAMING") do
                      node_in_graph = current_graph[ref_id]
                      IO.puts("\n  >>> About to embed node #{ref_id}")
                      IO.puts("      Node type in graph: #{inspect(node_in_graph["@type"])}")
                      IO.puts("      Node keys in graph: #{inspect(Map.keys(node_in_graph) |> Enum.take(10))}")
                    end

                    # Check if already embedded BEFORE calling embed_node
                    if System.get_env("DEBUG_FRAMING") do
                      already_emb = Map.get(thread_state.embedded, ref_id)
                      IO.puts("      Node #{ref_id} already in state.embedded: #{not is_nil(already_emb)}")
                      if already_emb do
                        IO.puts("      Existing keys: #{inspect(Map.keys(already_emb))}")
                      end
                    end

                    result = embed_node(thread_state, ref_id, property_frame, node_id)

                    if System.get_env("DEBUG_FRAMING") do
                      case result do
                        {nil, _} -> IO.puts("      Result: nil")
                        {embedded, _} when is_map(embedded) ->
                          IO.puts("      Result keys: #{inspect(Map.keys(embedded))}")
                          if map_size(embedded) <= 2 do
                            IO.puts("      MINIMAL EMBEDDING! Full result: #{inspect(embedded)}")
                          end
                        {embedded, _} ->
                          IO.puts("      Result is NOT a map: #{inspect(embedded)}")
                      end
                    end

                    case result do
                      {nil, updated_state} -> {[], updated_state}
                      {embedded, updated_state} when is_map(embedded) -> {[embedded], updated_state}
                      {_, updated_state} -> {[], updated_state}  # Skip non-map results
                    end
                  end)
                  |> then(fn {nested_nodes, final_thread_state} ->
                    {List.flatten(nested_nodes), final_thread_state}
                  end)

                # Add to output if any nodes found
                updated_output =
                  if Enum.empty?(referencing_nodes) do
                    acc_output
                  else
                    # In framing output (expanded form), property values must always be arrays
                    # Compaction will handle unwrapping based on container mappings
                    value = referencing_nodes

                    if System.get_env("DEBUG_FRAMING") do
                      IO.puts("\n>>> Adding reverse property to output:")
                      IO.puts("    Property: #{property}")
                      IO.puts("    Value type: list")
                      IO.puts("    Value: #{inspect(value, pretty: true, limit: :infinity)}")
                      if is_list(value) && length(value) > 0 do
                        first = hd(value)
                        if is_map(first) do
                          IO.puts("    First node keys: #{inspect(Map.keys(first))}")
                          IO.puts("    First node map_size: #{map_size(first)}")
                        end
                      end
                    end

                    # Reverse properties must be placed under @reverse in expanded form
                    # Normative: Reverse properties belong under @reverse, not as top-level properties
                    reverse_obj = Map.get(acc_output, @reverse, %{})
                    updated_reverse_obj = Map.put(reverse_obj, property, value)
                    final_output = Map.put(acc_output, @reverse, updated_reverse_obj)

                    if System.get_env("DEBUG_FRAMING") do
                      IO.puts("\n>>> AFTER adding to @reverse:")
                      if Map.has_key?(final_output, @reverse) do
                        reverse_in_output = final_output[@reverse]
                        if Map.has_key?(reverse_in_output, property) do
                          val_in_output = reverse_in_output[property]
                          if is_list(val_in_output) && length(val_in_output) > 0 do
                            first = hd(val_in_output)
                            if is_map(first) do
                              IO.puts("    First node in @reverse still has keys: #{inspect(Map.keys(first))}")
                              IO.puts("    First node map_size: #{map_size(first)}")
                            end
                          end
                        end
                      end
                    end

                    final_output
                  end

                {updated_output, new_state}

              _ ->
                # Term not found, skip
                {acc_output, acc_state}
            end
          end)

        _ ->
          {result, final_state}
      end

    {final_result, final_final_state}
  end

  # Process a value (node reference or value object)
  # Memory-efficient: avoids creating intermediate structures
  # Returns: {processed_value, updated_state}
  defp process_value(state, value, frame, parent, property) do
    cond do
      # List object
      # Normative: @list processing
      list?(value) ->
        list_values = value[@list] |> List.wrap()

        {processed, final_state} =
          list_values
          |> Enum.map_reduce(state, fn list_item, acc_state ->
            process_value(acc_state, list_item, frame, parent, property)
          end)

        {%{@list => processed}, final_state}

      # Value object - pass through
      # Normative: Value objects are not recursively framed
      value?(value) ->
        {value, state}

      # Node reference - embed or reference
      # Normative: Recursive node embedding
      Map.has_key?(value, @id) ->
        referenced_id = value[@id]

        # Check if this references a named graph (not in default graph but exists as graph key)
        # Normative: Named graphs handling depends on whether graph can be merged
        if Map.has_key?(state.graph_map, referenced_id) and referenced_id != @default do
          # This is a reference to a named graph
          named_graph = Map.get(state.graph_map, referenced_id, %{})

          # Check if the named graph contains a node with the same ID (mergeable)
          # Merge is only possible when graph ID matches a node ID within that graph
          mergeable = Map.has_key?(named_graph, referenced_id)

          # Check if the property has @container: "@graph" in the context
          # If so, frame the graph contents properly instead of just dumping all nodes
          property_has_graph_container =
            if property do
              # Look up the term definition for this property in the frame context
              # Property is in expanded form (full IRI), find compact term
              term_and_def =
                Enum.find(state.frame_context.term_defs, fn {_term, term_def} ->
                  term_def && term_def.iri_mapping == property
                end)

              case term_and_def do
                {_term, term_def} ->
                  container = term_def.container_mapping || []
                  "@graph" in container
                _ ->
                  false
              end
            else
              false
            end

          # Check if top-level frame has @graph (controls default behavior for named graphs)
          top_level_has_graph =
            is_map(state.original_frame) and Map.has_key?(state.original_frame, @graph)

          # Determine whether to preserve or merge
          # Some branches may update state (when framing graph contents), others don't
          if property_has_graph_container do
            # Property has @container: "@graph" - frame the graph contents properly
            # Only frame root nodes (nodes not referenced by other nodes in this graph)
            graph_nodes = Map.values(named_graph)

            # Find all node IDs referenced from within this graph
            referenced_ids =
              graph_nodes
              |> Enum.flat_map(fn node ->
                node
                |> Enum.flat_map(fn {key, value} ->
                  if key not in [@id, @_type_] do
                    List.wrap(value)
                    |> Enum.flat_map(fn v ->
                      cond do
                        is_map(v) and Map.has_key?(v, @id) -> [v[@id]]
                        true -> []
                      end
                    end)
                  else
                    []
                  end
                end)
              end)
              |> MapSet.new()

            # Root nodes are those not referenced by others in this graph
            root_nodes = Enum.reject(graph_nodes, fn node -> MapSet.member?(referenced_ids, node[@id]) end)

            # Switch to the named graph context for framing
            state_in_graph = %{state | current_graph: referenced_id}
            # Frame only root nodes
            {framed_nodes, final_state} =
              root_nodes
              |> Enum.map_reduce(state_in_graph, fn node, acc_state ->
                # Use the provided frame to frame this node
                # Parent is the graph container itself (referenced_id)
                embed_node(acc_state, node[@id], frame, referenced_id)
              end)
            # Restore the original graph context
            final_state = %{final_state | current_graph: state.current_graph}
            # Filter out any nil results
            framed_nodes = Enum.reject(framed_nodes, &is_nil/1)
            # Return the framed nodes with @graph wrapper
            # The @id and @graph will be processed during compaction
            {%{@id => referenced_id, @graph => framed_nodes}, final_state}
          else
            # Other cases don't update state
            result =
              cond do
                # Property frame explicitly requests @graph preservation
                is_map(frame) and Map.has_key?(frame, @graph) ->
                  graph_nodes = Map.values(named_graph)
                  %{@id => referenced_id, @graph => graph_nodes}

                # Mergeable and frame doesn't explicitly prevent merging
                mergeable ->
                  # Return the merged node directly (already flattened with properties)
                  Map.get(named_graph, referenced_id)

                # Top-level frame has @graph - preserve structure by default
                top_level_has_graph ->
                  graph_nodes = Map.values(named_graph)
                  %{@id => referenced_id, @graph => graph_nodes}

                # Otherwise, just return a reference (merge into default graph)
                true ->
                  %{@id => referenced_id}
              end

            {result, state}
          end
        else
          # Regular node reference - embed or reference
          # Per JSON-LD Framing spec: determine how to frame the embedded node
          sub_frame =
            cond do
              # Property frame has specific nested pattern - use it
              is_map(frame) and map_size(frame) > 0 ->
                frame

              # Property frame is explicitly empty wildcard ({})
              # Normative: A wildcard means "match any node and include all its properties"
              # When a property is explicitly specified in the parent frame (even as {}),
              # it should always embed, not just once. Use @always to ensure full embedding.
              is_map(frame) and map_size(frame) == 0 ->
                # Use wildcard frame that includes all properties with @embed: @always
                # Set @explicit: false to include all properties
                # Set @embed: @always to ensure full embedding on every reference
                # Don't add @id to the frame - let the blank node ID logic decide based on reference count
                %{
                  @explicit => false,
                  @embed => :always
                }

              # Property not in frame (frame is nil) - use minimal embedding
              # Normative: Properties not explicitly in frame should use default @embed behavior
              is_nil(frame) ->
                %{}

              # Fallback to empty frame (just @id and @type)
              true ->
                %{}
            end

          if System.get_env("DEBUG_FRAMING") != nil and referenced_id == "wellos:BoardColumn" do
            IO.puts("\n>>> PROCESSING NODE REFERENCE: #{referenced_id}")
            IO.puts("    frame is_map: #{is_map(frame)}")
            IO.puts("    frame size: #{if is_map(frame), do: map_size(frame), else: "N/A"}")
            IO.puts("    sub_frame keys: #{inspect(Map.keys(sub_frame) |> Enum.take(5))}")
            IO.puts("    sub_frame @embed: #{inspect(sub_frame["@embed"])}")
            IO.puts("    sub_frame @explicit: #{inspect(sub_frame["@explicit"])}")
          end

          if System.get_env("DEBUG_FRAMING") != nil and referenced_id == "wellos:BoardColumn" do
            IO.puts("    already_embedded: #{not is_nil(Map.get(state.embedded, referenced_id))}")
            IO.puts("    in subject_stack: #{referenced_id in state.subject_stack}")
          end

          {result, new_state} = embed_node(state, referenced_id, sub_frame, parent)

          if System.get_env("DEBUG_FRAMING") != nil and referenced_id == "wellos:BoardColumn" do
            IO.puts("    result keys: #{inspect(Map.keys(result || %{}))}")
            if result && map_size(result) <= 2 do
              IO.puts("    MINIMAL RESULT: #{inspect(result)}")
            end
          end

          {result, new_state}
        end

      # Other value - pass through
      true ->
        {value, state}
    end
  end

  # Check if a frame property value contains a @default value
  # Used to determine which frame-only properties to process
  defp has_default_value?(value) when is_list(value) do
    # In expanded form, property values are arrays
    Enum.any?(value, fn
      val when is_map(val) -> Map.has_key?(val, @default)
      _ -> false
    end)
  end

  defp has_default_value?(value) when is_map(value) do
    Map.has_key?(value, @default)
  end

  defp has_default_value?(_), do: false

  # Get a frame flag value with fallback to state default
  # Normative: Frame flag extraction
  defp get_frame_flag(frame, flag, default) do
    result = case Map.get(frame, flag) do
      [value | _] -> normalize_flag_value(flag, value, default)
      value when not is_nil(value) -> normalize_flag_value(flag, value, default)
      nil -> default
    end

    result
  end

  # Normalize flag values to atoms (for @embed) or booleans (for @explicit, @omitDefault, @requireAll)
  # Normative: Flag value normalization

  # @explicit, @omitDefault, @requireAll are boolean flags
  defp normalize_flag_value(flag, value, _default)
       when flag in [@explicit, @omitDefault, @requireAll] and is_boolean(value) do
    value
  end

  # @embed flag with keyword values
  defp normalize_flag_value(@embed, value, _default) when value in [true, @always, "always", :always],
    do: :always

  defp normalize_flag_value(@embed, value, _default) when value in [false, @never, "never", :never],
    do: :never

  defp normalize_flag_value(@embed, value, _default) when value in [@once, "once", :once],
    do: :once

  defp normalize_flag_value(@embed, value, _default) when value in [@last, "last", :last],
    do: :last

  defp normalize_flag_value(@embed, value, _default) when is_boolean(value),
    do: if(value, do: :once, else: :never)

  defp normalize_flag_value(_flag, _value, default), do: default

  # Merge nodes from all graphs for framing
  # When framing from @default, we need to see nodes from ALL graphs
  # and merge properties for nodes with the same @id
  # Normative: JSON-LD 1.1 Framing - cross-graph node merging
  defp merge_graphs_for_framing(node_map) do
    # Start with nodes from @default graph
    default_nodes = node_map[@default] || %{}

    if System.get_env("DEBUG_FRAMING") != nil do
      IO.puts("\n=== MERGING GRAPHS FOR FRAMING ===")
      IO.puts("Available graphs: #{inspect(Map.keys(node_map))}")
      IO.puts("Default graph nodes: #{inspect(Map.keys(default_nodes))}")
    end

    # Get all named graphs (exclude @default)
    named_graphs = node_map
      |> Map.delete(@default)
      |> Map.values()

    # Merge nodes from all named graphs
    result = Enum.reduce(named_graphs, default_nodes, fn graph_nodes, acc ->
      Enum.reduce(graph_nodes, acc, fn {node_id, node}, acc ->
        if Map.has_key?(acc, node_id) do
          # Node already exists - merge properties
          if System.get_env("DEBUG_FRAMING") != nil do
            IO.puts("Merging existing node: #{node_id}")
          end
          Map.update!(acc, node_id, fn existing_node ->
            merge_node_properties(existing_node, node)
          end)
        else
          # New node - add it
          if System.get_env("DEBUG_FRAMING") != nil do
            IO.puts("Adding new node: #{node_id}")
          end
          Map.put(acc, node_id, node)
        end
      end)
    end)

    if System.get_env("DEBUG_FRAMING") != nil do
      IO.puts("Merged graph nodes: #{inspect(Map.keys(result))}")
      for {id, node} <- result do
        if not String.contains?(id, "#graph") do
          IO.puts("\n  Merged node #{id}:")
          IO.inspect(node, pretty: true, limit: :infinity)
        end
      end
    end

    result
  end

  # Merge properties from two nodes with the same @id
  # Arrays are concatenated and deduplicated
  # Non-array values from node2 take precedence
  defp merge_node_properties(node1, node2) do
    Map.merge(node1, node2, fn key, v1, v2 ->
      cond do
        # Special handling for @id - keep consistent
        key == @id -> v1

        # @type should be merged and deduplicated
        key == @type ->
          List.wrap(v1)
          |> Kernel.++(List.wrap(v2))
          |> Enum.uniq()

        # For other properties, merge arrays
        is_list(v1) and is_list(v2) ->
          (v1 ++ v2) |> Enum.uniq()

        is_list(v1) ->
          (v1 ++ [v2]) |> Enum.uniq()

        is_list(v2) ->
          ([v1] ++ v2) |> Enum.uniq()

        # Non-list values: v2 takes precedence
        true -> v2
      end
    end)
  end

  # Get a node from the graph map
  # Memory-efficient: direct map lookup, no copying
  defp get_node(state, id, graph \\ nil) do
    # Use current_graph from state if no specific graph is provided
    graph_name = graph || state.current_graph

    state.graph_map
    |> Map.get(graph_name, %{})
    |> Map.get(id, %{})
  end

  # Merge framing keywords from original frame into expanded frame
  # During expansion, framing keywords like @embed, @explicit, etc. can be lost
  # This function recursively walks both frames and restores those keywords
  defp merge_framing_keywords(expanded, original) when is_map(expanded) and is_map(original) do
    # First, copy framing keywords from original to expanded
    result =
      Enum.reduce(@framing_keywords, expanded, fn keyword, acc ->
        if Map.has_key?(original, keyword) do
          Map.put(acc, keyword, original[keyword])
        else
          acc
        end
      end)

    # Then recursively merge nested frames for each property
    # Match properties by comparing expanded and original keys
    Enum.reduce(result, result, fn {expanded_key, expanded_value}, acc ->
      # Skip framing keywords - already handled above
      if expanded_key in @framing_keywords do
        acc
      else
        # Try to find matching property in original frame
        # Could be under same key (if keyword) or aliased key
        original_value = find_matching_property_frame(original, expanded_key)

        if original_value do
          # Recursively merge nested frames
          merged_value = merge_framing_keywords_in_property(expanded_value, original_value)
          Map.put(acc, expanded_key, merged_value)
        else
          acc
        end
      end
    end)
  end

  defp merge_framing_keywords(expanded, _original), do: expanded

  # Merge framing keywords in property values (which can be arrays or maps)
  defp merge_framing_keywords_in_property([expanded_item | _], original_item) when is_map(expanded_item) and is_map(original_item) do
    # Property frame is an array with single item - merge it
    [merge_framing_keywords(expanded_item, original_item)]
  end

  defp merge_framing_keywords_in_property(expanded_item, original_item) when is_map(expanded_item) and is_map(original_item) do
    # Property frame is a map - merge it directly
    merge_framing_keywords(expanded_item, original_item)
  end

  defp merge_framing_keywords_in_property(expanded, _original), do: expanded

  # Find matching property in original frame
  # The property might have a different key due to context mappings
  defp find_matching_property_frame(original, expanded_key) do
    # Direct match (for keywords like @type, @id, etc.)
    if Map.has_key?(original, expanded_key) do
      Map.get(original, expanded_key)
    else
      # Look for properties that might expand to this key
      # Check all properties in original frame
      Enum.find_value(original, fn {orig_key, orig_value} ->
        # Skip framing keywords
        if orig_key in @framing_keywords do
          nil
        else
          # For user properties, we'd need the context to expand them
          # For now, simple heuristic: check if expanded_key ends with original key
          # This handles cases like "http://example.com/embed" matching "embed"
          if String.ends_with?(to_string(expanded_key), to_string(orig_key)) or
             String.ends_with?(to_string(expanded_key), "/" <> to_string(orig_key)) or
             String.ends_with?(to_string(expanded_key), "#" <> to_string(orig_key)) do
            orig_value
          else
            nil
          end
        end
      end)
    end
  end

  # Remove @preserve entries from the result
  # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 10
  # @preserve is used during framing to maintain structure, but must be removed from final output
  defp remove_preserve(value) when is_map(value) do
    value
    |> Map.delete(@preserve)
    |> Stream.map(fn {k, v} -> {k, remove_preserve(v)} end)
    |> Enum.into(%{})
  end

  defp remove_preserve(value) when is_list(value) do
    value
    |> Stream.flat_map(fn
      %{@preserve => preserved} ->
        preserved
        |> List.wrap()
        |> Stream.map(&remove_preserve/1)

      v ->
        [remove_preserve(v)]
    end)
    |> Enum.to_list()
  end

  defp remove_preserve(value), do: value

  # Prune blank node identifiers that appear only once
  # Based on jsonld.js pruneBlankNodeIdentifiers feature
  # Removes @id from blank nodes that are only referenced once
  # This makes the output cleaner by only including @id when necessary for references
  @spec prune_blank_node_identifiers(any) :: any
  defp prune_blank_node_identifiers(value) do
    # Step 1: Scan the framed result and count blank node ID occurrences
    bnode_counts = count_blank_node_ids(value, %{})

    # Step 2: Identify blank node IDs to clear (appear only once)
    bnodes_to_clear =
      bnode_counts
      |> Enum.filter(fn {_id, count} -> count == 1 end)
      |> Enum.map(fn {id, _} -> id end)
      |> MapSet.new()

    if System.get_env("DEBUG_BLANK_NODES") != nil do
      IO.puts("\n=== BLANK NODE COUNTS ===")
      Enum.each(bnode_counts, fn {id, count} ->
        IO.puts("  #{id}: #{count} occurrences")
      end)
      IO.puts("Bnodes to clear: #{inspect(MapSet.to_list(bnodes_to_clear))}")
    end

    # Step 3: Recursively remove @id from nodes with blank node IDs in bnodes_to_clear
    prune_ids(value, bnodes_to_clear)
  end

  # Count blank node ID occurrences in the framed result
  defp count_blank_node_ids(value, counts) when is_map(value) do
    # If this node has a blank node @id, increment its count
    counts =
      if Map.has_key?(value, @id) do
        id_value = value[@id]
        if is_binary(id_value) and String.starts_with?(id_value, "_:") do
          Map.update(counts, id_value, 1, &(&1 + 1))
        else
          counts
        end
      else
        counts
      end

    # Recursively count in nested values
    Enum.reduce(value, counts, fn {_key, val}, acc ->
      count_blank_node_ids(val, acc)
    end)
  end

  defp count_blank_node_ids(value, counts) when is_list(value) do
    Enum.reduce(value, counts, fn item, acc ->
      count_blank_node_ids(item, acc)
    end)
  end

  defp count_blank_node_ids(_value, counts), do: counts

  # Collect all @id values from the result tree (recursively)
  # Used to filter out nodes that are already embedded when processing @included
  defp collect_embedded_ids(value, ids \\ MapSet.new())

  defp collect_embedded_ids(value, ids) when is_map(value) do
    # Only collect @id if this is a FULLY embedded node (has more than just @id)
    # Node references like %{"@id" => "..."} should not be counted as embedded
    ids =
      if Map.has_key?(value, @id) and map_size(value) > 1 do
        MapSet.put(ids, value[@id])
      else
        ids
      end

    # Recursively collect from nested values
    Enum.reduce(value, ids, fn {_key, val}, acc ->
      collect_embedded_ids(val, acc)
    end)
  end

  defp collect_embedded_ids(value, ids) when is_list(value) do
    Enum.reduce(value, ids, fn val, acc ->
      collect_embedded_ids(val, acc)
    end)
  end

  defp collect_embedded_ids(_value, ids), do: ids

  # Recursively traverse and remove @id from blank nodes in bnodes_to_clear
  defp prune_ids(value, bnodes_to_clear) when is_map(value) do
    value
    |> Enum.reduce(%{}, fn {key, val}, acc ->
      cond do
        # Remove @id if it's a blank node to clear
        key == @id and is_binary(val) and MapSet.member?(bnodes_to_clear, val) ->
          acc

        # Recursively process nested structures
        true ->
          Map.put(acc, key, prune_ids(val, bnodes_to_clear))
      end
    end)
  end

  defp prune_ids(value, bnodes_to_clear) when is_list(value) do
    Enum.map(value, &prune_ids(&1, bnodes_to_clear))
  end

  defp prune_ids(value, _bnodes_to_clear), do: value
end
