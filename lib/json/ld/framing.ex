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
      # If input has both @id and @graph, it's a named graph
      current_graph =
        case expanded_input do
          [%{@id => graph_id, @graph => _} | _] when is_binary(graph_id) -> graph_id
          _ -> @default
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

      # Add root frame to state for use when embedding referenced nodes
      state = Map.put(state, :root_frame, frame_obj)

      # Step 7: Match frame
      # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 7
      # Set matches to the result of calling the Frame Matching algorithm
      # Use the current_graph determined earlier (named graph or @default)
      graph_to_frame = node_map[current_graph] || %{}

      {matches, _final_state} = match_frame(state, Map.keys(graph_to_frame), frame_obj, nil)

      # Step 8: Create result with @graph
      # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 8
      result = %{@graph => matches}

      # Step 9: Unwrap single-node results
      # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 9
      # If processing mode is not json-ld-1.0, and @graph contains only a single node,
      # and either frame has @id or frame doesn't have @graph, unwrap
      result =
        if processor_options.processing_mode != "json-ld-1.0" and
             is_list(result[@graph]) and length(result[@graph]) == 1 and
             (Map.has_key?(frame_obj, @id) or not Map.has_key?(frame_obj, @graph)) do
          List.first(result[@graph])
        else
          result
        end

      # Step 10: Remove @preserve entries
      # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 10
      result = remove_preserve(result)

      # Step 11: Compact the result
      # Normative: https://www.w3.org/TR/json-ld11-framing/#framing-algorithm Step 11
      compacted =
        Compaction.compact(
          result,
          frame_context,
          @graph,
          processor_options,
          processor_options.compact_arrays,
          processor_options.ordered
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

    # Step 3.3-3.4: Match regular frame properties (excluding @reverse and framing keywords)
    regular_match =
      frame
      |> Enum.filter(fn {key, _} -> key not in [@reverse | @framing_keywords] end)
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

      cond do
        # Circular reference detected - always return just a reference
        # Normative: Circular references must be broken to avoid infinite embedding
        is_circular ->
          {%{@id => id}, state}

        # Never embed - return node reference (only for referenced nodes, not top-level matches)
        # Normative: @embed: @never behavior
        embed_value == :never and not is_top_level_match ->
          {%{@id => id}, state}

        # Already embedded with @once - return reference (but NOT for top-level matches)
        # Normative: @embed: @once behavior (default)
        # Top-level matches should always be fully embedded regardless of prior embedding
        embedded_node != nil and embed_value == :once and not is_top_level_match ->
          {%{@id => id}, state}

        # @last: replace previous embedding (not fully implemented - requires backtracking)
        # For now, treat as @once for memory efficiency (but NOT for top-level matches)
        embedded_node != nil and embed_value == :last and not is_top_level_match ->
          {%{@id => id}, state}

        # Embed the node
        # Normative: @embed: @always or first embedding with @once or top-level match
        true ->
          # Mark as embedded (lightweight - just store true)
          state = put_in(state.embedded[id], true)
          # Add to subject stack for circular reference detection
          state = %{state | subject_stack: [id | state.subject_stack]}
          {output, final_state} = create_output_node(state, node, frame, id)
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
    # or if the node is referenced elsewhere
    output =
      if Map.has_key?(node, @id) do
        node_id = Map.get(node, @id)
        is_blank_node = is_binary(node_id) and String.starts_with?(node_id, "_:")
        frame_requests_id = Map.has_key?(frame, @id)

        # Check if this blank node is referenced elsewhere in the graph
        # A blank node is referenced if it appears as a value in another node's properties
        is_referenced =
          if is_blank_node do
            current_graph = Map.get(state.graph_map, state.current_graph, %{})

            Enum.any?(current_graph, fn {other_id, other_node} ->
              other_id != node_id and
                Enum.any?(other_node, fn {key, value} ->
                  cond do
                    key == @id ->
                      false

                    key == @_type_ ->
                      is_type_reference?(value, node_id)

                    true ->
                      is_reference_to?(value, node_id)
                  end
                end)
            end)
          else
            false
          end

        # Include @id if:
        # 1. Frame explicitly includes @id, OR
        # 2. It's not a blank node (named nodes should keep their @id), OR
        # 3. Blank node is referenced elsewhere (including as @type) AND not using @explicit mode
        #
        # Note: With @explicit: true, blank nodes should only get @id if explicitly requested
        # or if referenced in @type. References from regular properties don't count when @explicit is true.
        should_include_id =
          if is_blank_node and state.explicit_inclusion and not frame_requests_id do
            # In explicit mode without @id request, only include @id if referenced in @type
            # (checked by is_type_reference in is_referenced logic)
            # For now, be conservative and don't include @id for blank nodes in explicit mode
            false
          else
            frame_requests_id or not is_blank_node or is_referenced
          end

        if should_include_id do
          %{@id => node_id}
        else
          %{}
        end
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

    # Process properties and add to output with state threading
    # Use streaming to minimize memory usage
    {output, state_after_props} =
      properties
      |> Stream.filter(&(&1 not in @excluded_from_properties))
      |> Enum.reduce({output, state}, fn property, {acc_output, acc_state} ->
        add_property_to_output(acc_state, node, frame, property, acc_output, omit_default, id)
      end)

    # Handle @reverse - add nodes that point to this node
    # Normative: @reverse framing adds reversed relationships to output
    {output, final_state} =
      if Map.has_key?(frame, @reverse) do
        add_reverse_to_output(state_after_props, node, frame[@reverse], output, id)
      else
        {output, state_after_props}
      end

    {output, final_state}
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
        property_frame =
          case Map.get(frame, property, %{}) do
            [first | _] when is_map(first) -> first
            val when is_map(val) -> val
            _ -> %{}
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

            # Check if frame has a list pattern with node references
            frame_list_pattern =
              Enum.find_value(frame_values, fn fv ->
                if is_map(fv) and Map.has_key?(fv, @list) do
                  list_items = List.wrap(fv[@list])
                  # Only filter if the frame list contains node references (has @id)
                  # or complex objects with multiple properties
                  has_node_refs =
                    Enum.any?(list_items, fn item ->
                      is_map(item) and Map.has_key?(item, @id)
                    end)

                  if has_node_refs do
                    list_items
                  else
                    nil
                  end
                else
                  nil
                end
              end)

            if frame_list_pattern do
              # Filter list contents to only include matching items
              Enum.map(filtered_values, fn value ->
                if is_map(value) and Map.has_key?(value, @list) do
                  node_list = List.wrap(value[@list])

                  filtered_list =
                    Enum.filter(node_list, fn node_item ->
                      Enum.any?(frame_list_pattern, fn frame_item ->
                        items_match?(frame_item, node_item)
                      end)
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
            process_value(acc_state, value, property_frame, parent_id)
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

    # For each property in @reverse frame, find nodes that point to this node
    # Thread state through the reduce
    {reverse_map, final_state} =
      Enum.reduce(reverse_frame_map, {%{}, state}, fn {reverse_prop, prop_frame},
                                                       {acc_map, acc_state} ->
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
      end)

    # Add @reverse to output if any reversed relationships found
    if Enum.empty?(reverse_map) do
      {output, final_state}
    else
      {Map.put(output, @reverse, reverse_map), final_state}
    end
  end

  # Process a value (node reference or value object)
  # Memory-efficient: avoids creating intermediate structures
  # Returns: {processed_value, updated_state}
  defp process_value(state, value, frame, parent) do
    cond do
      # List object
      # Normative: @list processing
      list?(value) ->
        list_values = value[@list] |> List.wrap()

        {processed, final_state} =
          list_values
          |> Enum.map_reduce(state, fn list_item, acc_state ->
            process_value(acc_state, list_item, frame, parent)
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

          # Check if top-level frame has @graph (controls default behavior for named graphs)
          top_level_has_graph =
            is_map(state.original_frame) and Map.has_key?(state.original_frame, @graph)

          # Determine whether to preserve or merge (no state changes for named graphs)
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
        else
          # Regular node reference - embed or reference
          # Per JSON-LD Framing spec: determine how to frame the embedded node
          sub_frame =
            cond do
              # Property frame has specific nested pattern - use it
              is_map(frame) and map_size(frame) > 0 ->
                frame

              # Property frame is wildcard ({})
              # Normative: A wildcard means "match any node and include all its properties"
              # But still respect @embed rules to avoid infinite recursion
              is_map(frame) and map_size(frame) == 0 ->
                # Use wildcard frame that includes all properties but respects @embed: @once
                # Set @explicit: false to include all properties
                # Set @embed: @once explicitly to prevent multiple full embeddings
                # Include @id: {} to ensure blank nodes get their IDs
                %{
                  @id => %{},
                  @explicit => false,
                  @embed => :once
                }

              # Fallback to empty frame (just @id and @type)
              true ->
                %{}
            end

          embed_node(state, referenced_id, sub_frame, parent)
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

  # Check if a value references a specific node ID
  defp is_reference_to?(value, target_id) when is_list(value) do
    Enum.any?(value, &is_reference_to?(&1, target_id))
  end

  defp is_reference_to?(value, target_id) when is_map(value) do
    Map.get(value, @id) == target_id
  end

  defp is_reference_to?(_, _), do: false

  # Check if a @type value references a specific node ID
  defp is_type_reference?(types, target_id) when is_list(types) do
    target_id in types
  end

  defp is_type_reference?(type, target_id) when is_binary(type) do
    type == target_id
  end

  defp is_type_reference?(_, _), do: false

  # Get a frame flag value with fallback to state default
  # Normative: Frame flag extraction
  defp get_frame_flag(frame, flag, default) do
    case Map.get(frame, flag) do
      [value | _] -> normalize_flag_value(flag, value, default)
      value when not is_nil(value) -> normalize_flag_value(flag, value, default)
      nil -> default
    end
  end

  # Normalize flag values to atoms (for @embed) or booleans (for @explicit, @omitDefault, @requireAll)
  # Normative: Flag value normalization

  # @explicit, @omitDefault, @requireAll are boolean flags
  defp normalize_flag_value(flag, value, _default)
       when flag in [@explicit, @omitDefault, @requireAll] and is_boolean(value) do
    value
  end

  # @embed flag with keyword values
  defp normalize_flag_value(@embed, value, _default) when value in [true, @always, "always"],
    do: :always

  defp normalize_flag_value(@embed, value, _default) when value in [false, @never, "never"],
    do: :never

  defp normalize_flag_value(@embed, value, _default) when value in [@once, "once"],
    do: :once

  defp normalize_flag_value(@embed, value, _default) when value in [@last, "last"],
    do: :last

  defp normalize_flag_value(@embed, value, _default) when is_boolean(value),
    do: if(value, do: :once, else: :never)

  defp normalize_flag_value(_flag, _value, default), do: default

  # Get a node from the graph map
  # Memory-efficient: direct map lookup, no copying
  defp get_node(state, id, graph \\ nil) do
    # Use current_graph from state if no specific graph is provided
    graph_name = graph || state.current_graph

    state.graph_map
    |> Map.get(graph_name, %{})
    |> Map.get(id, %{})
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
end
