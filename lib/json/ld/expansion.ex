defmodule JSON.LD.Expansion do
  @moduledoc """
  Implementation of the JSON-LD 1.1 Expansion Algorithms.

  <https://www.w3.org/TR/json-ld11-api/#expansion-algorithm>
  """

  import JSON.LD.{IRIExpansion, Utils}

  alias JSON.LD.{Context, Options}
  alias JSON.LD.Context.TermDefinition

  defp init_options(options) do
    options
    |> Keyword.put_new(:frame_expansion, false)
    |> Keyword.put_new(:ordered, false)
    |> Keyword.put_new(:from_map, false)
  end

  def expand(active_context, active_property, element, options, processor_options) do
    options = init_options(options)
    # 2) If active property is @default, initialize the frameExpansion flag to false
    options =
      if active_property == "@default",
        do: Keyword.put(options, :frame_expansion, false),
        else: options

    # 3) If active property has a term definition in active context with a local context, initialize property-scoped context to that local context.
    property_scoped_context =
      if term_def = active_property && active_context.term_defs[active_property] do
        term_def.local_context
      end

    do_expand(
      active_context,
      active_property,
      element,
      Keyword.put(options, :property_scoped_context, property_scoped_context),
      processor_options
    )
  end

  # 1) If element is null, return null.
  defp do_expand(_, _, nil, _, _), do: nil

  # 4) If element is a scalar, ...
  defp do_expand(active_context, active_property, element, options, processor_options)
       when is_binary(element) or is_number(element) or is_boolean(element) do
    if active_property in [nil, "@graph"] do
      nil
    else
      active_context =
        if property_scoped_context = options[:property_scoped_context] do
          Context.update(active_context, property_scoped_context,
            base: (term_def = active_context.term_defs[active_property]) && term_def.base_url,
            processor_options: processor_options
          )
        else
          active_context
        end

      expand_value(active_context, active_property, element, processor_options)
    end
  end

  # 5) If element is an array, ...
  defp do_expand(active_context, active_property, element, options, processor_options)
       when is_list(element) do
    term_def = active_context.term_defs[active_property]
    container_mapping = term_def && term_def.container_mapping

    Enum.flat_map(element, fn item ->
      expanded_item = expand(active_context, active_property, item, options, processor_options)

      if container_mapping && "@list" in container_mapping && is_list(expanded_item) do
        %{"@list" => expanded_item}
      else
        expanded_item
      end
      |> List.wrap()
    end)
  end

  # 6) - 20)
  defp do_expand(active_context, active_property, element, options, processor_options)
       when is_map(element) do
    # 7)
    active_context =
      if active_context.previous_context do
        expanded_elements =
          Enum.map(element, fn {key, _} ->
            expand_iri(key, active_context, processor_options, false, true)
          end)

        if !options[:from_map] && "@value" not in expanded_elements &&
             expanded_elements != ["@id"] do
          active_context.previous_context
        else
          active_context
        end
      else
        active_context
      end

    # 8)
    active_context =
      if property_scoped_context = options[:property_scoped_context] do
        Context.update(
          active_context,
          property_scoped_context,
          base: (term_def = active_context.term_defs[active_property]) && term_def.base_url,
          override_protected: true,
          processor_options: processor_options
        )
      else
        active_context
      end

    # 9)
    active_context =
      if Map.has_key?(element, "@context") do
        Context.update(active_context, Map.get(element, "@context"), processor_options)
      else
        active_context
      end

    # 10)
    type_scoped_context = active_context

    # 11)
    sorted = Enum.sort_by(element, fn {key, _} -> key end)

    {active_context, first_type_key} =
      Enum.reduce(sorted, {active_context, nil}, fn {key, value}, {context, first_type_key} ->
        if expand_iri(key, context, processor_options, false, true) == "@type" do
          {
            value
            |> List.wrap()
            |> Enum.sort()
            |> Enum.reduce(context, fn
              term, context when is_binary(term) ->
                type_scoped_term_def = type_scoped_context.term_defs[term]

                if local_context = type_scoped_term_def && type_scoped_term_def.local_context do
                  term_def = context.term_defs[term]

                  Context.update(context, local_context,
                    base: (term_def && term_def.base_url) || processor_options.base,
                    propagate: false,
                    processor_options: processor_options
                  )
                else
                  context
                end

              _, context ->
                context
            end),
            first_type_key || key
          }
        else
          {context, first_type_key}
        end
      end)

    # 12)
    input_type =
      element[first_type_key]
      |> List.wrap()
      |> List.last()
      |> case do
        nil -> nil
        input_type -> expand_iri(input_type, active_context, processor_options, false, true)
      end

    # 13)
    result =
      if(processor_options.ordered, do: sorted, else: element)
      |> expand_object(
        active_property,
        active_context,
        type_scoped_context,
        input_type,
        processor_options,
        options
      )

    result =
      case result do
        # 15)
        %{"@value" => value} ->
          # 15.1)
          keys = Map.keys(result)

          if keys -- ~w[@direction @index @language @type @value] != [] ||
               (("@language" in keys or "@direction" in keys) and "@type" in keys) do
            raise JSON.LD.Error.invalid_value_object("value object with disallowed members")
          end

          cond do
            # 15.2)
            result["@type"] == "@json" ->
              result

            # 15.3)
            value == nil or value == [] ->
              nil

            # 15.4)
            # Skip validation for frame expansion - allow arrays and wildcards
            processor_options.frame_expansion ->
              result

            !is_binary(value) and Map.has_key?(result, "@language") ->
              raise JSON.LD.Error.invalid_language_tagged_value(
                      "@value '#{inspect(value)}' is tagged with a language"
                    )

            # 15.5)
            (type = result["@type"]) && !(is_binary(type) and valid_uri?(type)) ->
              raise JSON.LD.Error.invalid_typed_value(value, type)

            true ->
              result
          end

        # 16)
        %{"@type" => type} when not is_list(type) ->
          Map.put(result, "@type", [type])

        # 17)
        %{"@set" => set} ->
          validate_set_or_list_object(result)
          set

        %{"@list" => _} ->
          validate_set_or_list_object(result)
          result

        _ ->
          result
      end

    # 18) If result contains only the key @language, set result to null.
    result =
      if is_map(result) and map_size(result) == 1 and Map.has_key?(result, "@language"),
        do: nil,
        else: result

    # 19) If active property is null or @graph, drop free-floating values
    # SPEC ISSUE: the "only" in "or contains only the entries @value or @list" in 19.1 had to be ignored
    if active_property in [nil, "@graph"] and is_map(result) and
         (Enum.empty?(result) or
            (Map.has_key?(result, "@value") or Map.has_key?(result, "@list") or
               (map_size(result) == 1 and Map.has_key?(result, "@id") and
                  not processor_options.frame_expansion))),
       do: nil,
       else: result
  end

  defp expand_object(
         element,
         active_property,
         active_context,
         type_scoped_context,
         input_type,
         processor_options,
         options
       ) do
    processing_mode = processor_options.processing_mode
    frame_expansion = processor_options.frame_expansion

    # 13)
    {result, nests} =
      Enum.reduce(element, {%{}, []}, fn {key, value}, {result, nests} ->
        # 13.1)
        if key == "@context" do
          {result, nests}
        else
          # 13.2)
          expanded_property =
            expand_iri(key, active_context, processor_options, false, true)

          keyword? = JSON.LD.keyword?(expanded_property)
          # Check if this is a framing keyword (only valid during frame expansion)
          framing_keyword? =
            frame_expansion and
              expanded_property in [
                "@default",
                "@embed",
                "@explicit",
                "@omitDefault",
                "@requireAll"
              ]

          # 13.3)
          if expanded_property &&
               (String.contains?(expanded_property, ":") || keyword? || framing_keyword?) do
            # 13.4)
            if keyword? or framing_keyword? do
              # 13.4.1)
              if active_property == "@reverse" do
                raise JSON.LD.Error.invalid_reverse_property_map()
              end

              # 13.4.2)
              if Map.has_key?(result, expanded_property) and
                   (processing_mode == "json-ld-1.0" or
                      expanded_property not in ["@included", "@type"]) do
                raise JSON.LD.Error.colliding_keywords(expanded_property)
              end

              expanded_value =
                case expanded_property do
                  # 13.4.3)
                  "@id" ->
                    cond do
                      is_binary(value) ->
                        expanded =
                          expand_iri(value, active_context, processor_options, true, false)

                        # Validate: frames must not contain blank node identifiers in @id
                        if frame_expansion and is_binary(expanded) and
                             String.starts_with?(expanded, "_:") do
                          raise JSON.LD.Error.invalid_frame(
                                  "@id value cannot be a blank node identifier"
                                )
                        end

                        expanded

                      frame_expansion and is_list(value) and
                          Enum.all?(value, fn v -> v == %{} or is_binary(v) or is_map(v) end) ->
                        Enum.map(value, fn
                          %{} = item when map_size(item) == 0 ->
                            item

                          item when is_map(item) ->
                            # Handle @default in @id for frames
                            active_context
                            |> expand("@id", item, options, processor_options)
                            |> List.wrap()
                            |> hd()

                          item ->
                            expanded =
                              expand_iri(item, active_context, processor_options, true, false)

                            # Validate: frames must not contain blank node identifiers in @id
                            if is_binary(expanded) and String.starts_with?(expanded, "_:") do
                              raise JSON.LD.Error.invalid_frame(
                                      "@id value cannot be a blank node identifier"
                                    )
                            end

                            expanded
                        end)

                      frame_expansion and value == %{} ->
                        [value]

                      frame_expansion and is_map(value) ->
                        # Handle @default in @id for frames
                        active_context
                        |> expand("@id", value, options, processor_options)
                        |> List.wrap()

                      true ->
                        raise JSON.LD.Error.invalid_id_value(value)
                    end

                  # 13.4.4)
                  "@type" ->
                    expanded_value =
                      cond do
                        is_binary(value) ->
                          expanded =
                            expand_iri(value, type_scoped_context, processor_options, true, true)

                          # Validate: frames must not contain blank node identifiers in @type
                          if frame_expansion and is_binary(expanded) and
                               String.starts_with?(expanded, "_:") do
                            raise JSON.LD.Error.invalid_frame(
                                    "@type value cannot be a blank node identifier"
                                  )
                          end

                          expanded

                        is_list(value) and Enum.all?(value, &is_binary/1) ->
                          Enum.map(value, fn item ->
                            expanded =
                              expand_iri(item, type_scoped_context, processor_options, true, true)

                            # Validate: frames must not contain blank node identifiers in @type
                            if frame_expansion and is_binary(expanded) and
                                 String.starts_with?(expanded, "_:") do
                              raise JSON.LD.Error.invalid_frame(
                                      "@type value cannot be a blank node identifier"
                                    )
                            end

                            expanded
                          end)

                        frame_expansion and is_list(value) and
                            Enum.all?(value, fn v -> v == %{} or is_binary(v) or is_map(v) end) ->
                          Enum.map(value, fn
                            %{} = item when map_size(item) == 0 ->
                              item

                            item when is_map(item) ->
                              # Handle @default in @type for frames
                              active_context
                              |> expand("@type", item, options, processor_options)
                              |> List.wrap()
                              |> hd()

                            item ->
                              expanded =
                                expand_iri(
                                  item,
                                  type_scoped_context,
                                  processor_options,
                                  true,
                                  true
                                )

                              # Validate: frames must not contain blank node identifiers in @type
                              if is_binary(expanded) and String.starts_with?(expanded, "_:") do
                                raise JSON.LD.Error.invalid_frame(
                                        "@type value cannot be a blank node identifier"
                                      )
                              end

                              expanded
                          end)

                        frame_expansion and value == %{} ->
                          [value]

                        frame_expansion and is_map(value) ->
                          # Handle @default in @type for frames
                          active_context
                          |> expand("@type", value, options, processor_options)
                          |> List.wrap()

                        true ->
                          raise JSON.LD.Error.invalid_type_value(value)
                      end

                    if existing = result["@type"] do
                      List.wrap(existing) ++ List.wrap(expanded_value)
                    else
                      expanded_value
                    end

                  # 13.4.5)
                  "@graph" ->
                    active_context
                    |> expand("@graph", value, options, processor_options)
                    |> List.wrap()

                  # 13.4.6)
                  "@included" when processing_mode != "json-ld-1.0" ->
                    expanded_value =
                      active_context
                      |> expand(nil, value, options, processor_options)
                      |> to_list()

                    unless Enum.all?(expanded_value, &node?/1) do
                      raise JSON.LD.Error.invalid_included_value(expanded_value)
                    end

                    expanded_value ++ List.wrap(result["@included"])

                  # 13.4.7)
                  "@value" ->
                    cond do
                      input_type == "@json" and processing_mode != "json-ld-1.0" ->
                        value

                      frame_expansion and is_list(value) ->
                        value

                      frame_expansion and value == %{} ->
                        [value]

                      not (scalar?(value) or is_nil(value)) ->
                        raise JSON.LD.Error.invalid_value_object_value(value)

                      true ->
                        if is_nil(value) do
                          {:skip, Map.put(result, "@value", nil)}
                        else
                          value
                        end
                    end

                  # 13.4.8)
                  "@language" ->
                    cond do
                      frame_expansion and is_list(value) ->
                        Enum.map(value, &validate_and_normalize_language(&1, processor_options))

                      frame_expansion and value == %{} ->
                        [value]

                      is_binary(value) ->
                        expanded_value = validate_and_normalize_language(value, processor_options)

                        if frame_expansion do
                          [expanded_value]
                        else
                          expanded_value
                        end

                      true ->
                        raise JSON.LD.Error.invalid_language_tagged_string(value)
                    end

                  # 13.4.9)
                  "@direction" when processing_mode != "json-ld-1.0" ->
                    cond do
                      value in ~w[ltr rtl] ->
                        if frame_expansion do
                          [value]
                        else
                          value
                        end

                      frame_expansion and is_list(value) and
                          Enum.all?(value, &(&1 in ~w[ltr rtl])) ->
                        value

                      frame_expansion and value == %{} ->
                        [value]

                      true ->
                        raise JSON.LD.Error.invalid_base_direction(value)
                    end

                  # 13.4.10)
                  "@index" ->
                    if is_binary(value) do
                      value
                    else
                      raise JSON.LD.Error.invalid_index_value(value)
                    end

                  # 13.4.11)
                  "@list" ->
                    if active_property in [nil, "@graph"] do
                      {:skip, result}
                    else
                      active_context
                      |> expand(active_property, value, options, processor_options)
                      |> List.wrap()
                    end

                  # 13.4.12)
                  "@set" ->
                    expand(active_context, active_property, value, options, processor_options)

                  # 13.4.13)
                  "@reverse" ->
                    # 13.4.13.1)
                    unless is_map(value) do
                      raise JSON.LD.Error.invalid_reverse_value(value)
                    end

                    # 13.4.13.2)
                    expanded_value =
                      expand(active_context, "@reverse", value, options, processor_options)

                    # 13.4.13.3)
                    new_result =
                      if Map.has_key?(expanded_value, "@reverse") do
                        Enum.reduce(
                          expanded_value["@reverse"],
                          result,
                          fn {property, item}, new_result ->
                            items = List.wrap(item)

                            Map.update(new_result, property, items, &(&1 ++ items))
                          end
                        )
                      else
                        result
                      end

                    # 13.4.13.4)
                    new_result =
                      if Map.keys(expanded_value) != ["@reverse"] do
                        expanded_value = Map.delete(expanded_value, "@reverse")

                        reverse_map =
                          Enum.reduce(expanded_value, Map.get(new_result, "@reverse", %{}), fn
                            {property, items}, reverse_map ->
                              Enum.each(items, fn item ->
                                if value?(item) or list?(item) do
                                  raise JSON.LD.Error.invalid_reverse_property_value(item)
                                end
                              end)

                              Map.update(reverse_map, property, items, &(&1 ++ items))
                          end)

                        Map.put(new_result, "@reverse", reverse_map)
                      else
                        new_result
                      end

                    {:skip, new_result}

                  # 13.4.14)
                  "@nest" ->
                    {:nests, [key | nests]}

                  # 13.4.15)
                  expanded_property
                  when frame_expansion and
                         expanded_property in [
                           "@default",
                           "@embed",
                           "@explicit",
                           "@omitDefault",
                           "@requireAll"
                         ] ->
                    # Validate @embed value
                    if expanded_property == "@embed" do
                      valid_embed_values = ["@always", "@never", "@once", "@last", true, false]

                      unless value in valid_embed_values do
                        raise JSON.LD.Error.invalid_embed_value(
                                "Invalid @embed value: #{inspect(value)}. Must be one of: @always, @never, @once, @last, true, or false"
                              )
                      end
                    end

                    # For framing keywords, just return the value directly (don't expand)
                    value

                  _ ->
                    nil
                end

              # 13.4.16) and 13.4.17)
              case {expanded_value, expanded_property, input_type} do
                {{:nests, nests}, _, _} -> {result, nests}
                {{:skip, new_result}, _, _} -> {new_result, nests}
                {nil, "@value", type} when type != "@json" -> {result, nests}
                _ -> {Map.put(result, expanded_property, expanded_value), nests}
              end
            else
              term_def = active_context.term_defs[key]

              # 13.5)
              container_mapping = List.wrap(term_def && term_def.container_mapping)

              expanded_value =
                cond do
                  # 13.6)
                  term_def && term_def.type_mapping == "@json" ->
                    %{"@value" => value, "@type" => "@json"}

                  # 13.7)
                  is_map(value) and "@language" in container_mapping ->
                    direction = TermDefinition.direction(term_def, active_context)

                    value
                    |> maybe_sort_by(processor_options.ordered, fn {language, _} -> language end)
                    |> Enum.flat_map(fn {language, language_value} ->
                      expanded_language =
                        expand_iri(language, active_context, processor_options, false, true)

                      language_value
                      |> List.wrap()
                      |> Enum.flat_map(fn
                        nil ->
                          []

                        item when is_binary(item) ->
                          v = %{"@value" => item}

                          v =
                            if language == "@none" or expanded_language == "@none" do
                              v
                            else
                              Map.put(
                                v,
                                "@language",
                                validate_and_normalize_language(language, processor_options)
                              )
                            end

                          if direction do
                            Map.put(v, "@direction", to_string(direction))
                          else
                            v
                          end
                          |> List.wrap()

                        item ->
                          raise JSON.LD.Error.invalid_language_map_value(item)
                      end)
                    end)

                  # 13.8)
                  is_map(value) and Enum.any?(~w[@index @type @id], &(&1 in container_mapping)) ->
                    # 13.8.2)
                    index_key = (term_def && term_def.index_mapping) || "@index"

                    # 13.8.3.1)
                    # Note: We're doing this outside of the loop
                    container_context =
                      if "@type" in container_mapping or
                           "@id" in container_mapping do
                        active_context.previous_context
                      end || active_context

                    # 13.8.3)
                    value
                    |> maybe_sort_by(processor_options.ordered, fn {index, _} -> index end)
                    |> Enum.flat_map(fn {index, index_value} ->
                      # 13.8.3.1) is done above outside of this loop
                      # 13.8.3.2)
                      index_term_def = container_context.term_defs[index]

                      map_context =
                        if "@type" in container_mapping && index_term_def &&
                             index_term_def.local_context do
                          Context.update(container_context, index_term_def.local_context,
                            base: term_def.base_url,
                            processor_options: processor_options
                          )
                        else
                          container_context
                        end

                      # 13.8.3.4)
                      # SPEC ISSUE: which context should be used here?
                      expanded_index =
                        expand_iri(index, map_context, processor_options, false, true)

                      # 13.8.3.6)
                      map_context
                      |> expand(
                        key,
                        # 13.8.3.5)
                        List.wrap(index_value),
                        Keyword.put(options, :from_map, true),
                        processor_options
                      )
                      # 13.8.3.7)
                      |> Enum.map(fn item ->
                        # 13.8.3.7.1)
                        item =
                          if "@graph" in container_mapping and not graph?(item) do
                            %{"@graph" => List.wrap(item)}
                          else
                            item
                          end

                        cond do
                          expanded_index == "@none" ->
                            item

                          # 13.8.3.7.2)
                          "@index" in container_mapping and index_key != "@index" ->
                            re_expanded_index =
                              expand_value(active_context, index_key, index, processor_options)

                            # SPEC ISSUE: vocab option had to be set to true
                            expanded_index_key =
                              expand_iri(
                                index_key,
                                container_context,
                                processor_options,
                                false,
                                true
                              )

                            item =
                              Map.update(
                                item,
                                expanded_index_key,
                                [re_expanded_index],
                                &[re_expanded_index | List.wrap(&1)]
                              )

                            if value?(item) and map_size(Map.delete(item, "@value")) > 0 do
                              raise JSON.LD.Error.invalid_value_object(
                                      "Attempt to add illegal key to value object: #{inspect(item)}"
                                    )
                            else
                              item
                            end

                          # 13.8.3.7.3)
                          "@index" in container_mapping and not Map.has_key?(item, "@index") ->
                            Map.put(item, "@index", index)

                          # 13.8.3.7.4)
                          "@id" in container_mapping and not Map.has_key?(item, "@id") ->
                            Map.put(
                              item,
                              "@id",
                              expand_iri(index, map_context, processor_options, true, false)
                            )

                          # 13.8.3.7.5)
                          "@type" in container_mapping ->
                            Map.update(
                              item,
                              "@type",
                              [expanded_index],
                              &[expanded_index | List.wrap(&1)]
                            )

                          true ->
                            item
                        end
                      end)
                    end)

                  # 13.9)
                  true ->
                    expand(active_context, key, value, options, processor_options)
                end

              # 13.10)
              if is_nil(expanded_value) do
                {result, nests}
              else
                # 13.11)
                expanded_value =
                  if "@list" in container_mapping and not list?(expanded_value) do
                    %{"@list" => List.wrap(expanded_value)}
                  else
                    expanded_value
                  end

                # 13.12)
                expanded_value =
                  if "@graph" in container_mapping and
                       "@id" not in container_mapping and
                       "@index" not in container_mapping do
                    expanded_value
                    |> List.wrap()
                    |> Enum.map(fn ev -> %{"@graph" => List.wrap(ev)} end)
                  else
                    expanded_value
                  end

                # 13.13)
                if term_def && term_def.reverse_property do
                  reverse_map = Map.get(result, "@reverse", %{})

                  reverse_map =
                    expanded_value
                    |> List.wrap()
                    |> Enum.reduce(reverse_map, fn item, reverse_map ->
                      if Map.has_key?(item, "@value") or Map.has_key?(item, "@list") do
                        raise JSON.LD.Error.invalid_reverse_property_value(item)
                      end

                      Map.update(reverse_map, expanded_property, [item], fn members ->
                        members ++ [item]
                      end)
                    end)

                  {Map.put(result, "@reverse", reverse_map), nests}
                else
                  # 13.14)
                  expanded_value = List.wrap(expanded_value)

                  {
                    Map.update(result, expanded_property, expanded_value, fn values ->
                      expanded_value ++ values
                    end),
                    nests
                  }
                end
              end
            end
          else
            {result, nests}
          end
        end
      end)

    # 14)
    nests
    |> maybe_sort(processor_options.ordered)
    |> Enum.reduce(result, fn nesting_key, result ->
      # SPEC ISSUE: this nest_context creation is not specified, but required to pass #tc037 and #tc038
      term_def = active_context.term_defs[nesting_key]

      nest_context =
        if nest_context = term_def && term_def.local_context do
          Context.update(
            active_context,
            nest_context,
            override_protected: true,
            processor_options: processor_options
          )
        else
          active_context
        end

      nested_values =
        case element do
          element when is_map(element) ->
            element[nesting_key]

          element when is_list(element) ->
            Enum.find_value(element, fn {k, v} -> if k == nesting_key, do: v end)
        end
        |> List.wrap()

      Enum.reduce(nested_values, result, fn nested_value, result ->
        if not is_map(nested_value) or
             Enum.any?(nested_value, fn {k, _} ->
               expand_iri(k, nest_context, processor_options, false, true) == "@value"
             end) do
          raise JSON.LD.Error.invalid_nest_value(invalid: nested_value)
        end

        deep_merge(
          result,
          expand_object(
            nested_value,
            active_property,
            nest_context,
            type_scoped_context,
            input_type,
            processor_options,
            options
          )
        )
      end)
    end)
  end

  @spec validate_set_or_list_object(map) :: true
  defp validate_set_or_list_object(object) when map_size(object) == 1, do: true

  defp validate_set_or_list_object(%{"@index" => _} = object) when map_size(object) == 2, do: true

  defp validate_set_or_list_object(object) do
    raise JSON.LD.Error.invalid_set_or_list_object(object)
  end

  @doc """
  Details at <http://json-ld.org/spec/latest/json-ld-api/#value-expansion>
  """
  @spec expand_value(Context.t(), String.t(), any, Options.t()) :: map
  def expand_value(active_context, active_property, value, options \\ Options.new()) do
    term_def = Map.get(active_context.term_defs, active_property, %TermDefinition{})

    cond do
      # 1)
      term_def.type_mapping == "@id" and is_binary(value) ->
        %{"@id" => expand_iri(value, active_context, options, true, false)}

      # 2)
      term_def.type_mapping == "@vocab" and is_binary(value) ->
        %{"@id" => expand_iri(value, active_context, options, true, true)}

      true ->
        result = %{"@value" => value}

        cond do
          (type_mapping = term_def.type_mapping) && type_mapping not in ~w[@id @vocab @none] ->
            Map.put(result, "@type", type_mapping)

          is_binary(value) ->
            language = TermDefinition.language(term_def, active_context)
            direction = TermDefinition.direction(term_def, active_context)

            result =
              if not is_nil(language) do
                Map.put(result, "@language", language)
              else
                result
              end

            if not is_nil(direction) do
              Map.put(result, "@direction", to_string(direction))
            else
              result
            end

          true ->
            result
        end
    end
  end
end
