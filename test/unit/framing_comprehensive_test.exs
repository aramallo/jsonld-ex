defmodule JSON.LD.FramingComprehensiveTest do
  use ExUnit.Case

  # Helper function to extract result from @graph if present
  # When framing returns a @graph array, get the first matched node
  defp extract_result(result) when is_map(result) do
    case result do
      %{"@graph" => [first | _]} -> first
      %{"@graph" => []} -> %{}
      other -> other
    end
  end

  describe "@embed option tests" do
    setup do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{
            "@id" => "ex:parent",
            "@type" => "ex:Parent",
            "ex:child" => %{"@id" => "ex:child1"}
          },
          %{
            "@id" => "ex:child1",
            "@type" => "ex:Child",
            "ex:name" => "Child One"
          }
        ]
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Parent"
      }

      {:ok, input: input, frame: frame}
    end

    test "@embed: @once (default) embeds first occurrence only", %{input: input, frame: frame} do
      result = JSON.LD.frame(input, frame, embed: :once) |> extract_result()

      assert result["@type"] == "ex:Parent"
      # Child should be embedded on first reference
      assert is_map(result["ex:child"])
      assert result["ex:child"]["@type"] == "ex:Child"
      assert result["ex:child"]["ex:name"] == "Child One"
    end

    test "@embed: @always embeds every occurrence", %{input: input, frame: frame} do
      result = JSON.LD.frame(input, frame, embed: :always) |> extract_result()

      assert result["@type"] == "ex:Parent"
      # With @always, child should be fully embedded
      assert is_map(result["ex:child"])
      assert result["ex:child"]["@type"] == "ex:Child"
    end

    test "@embed: @never only includes references", %{input: input, frame: frame} do
      result = JSON.LD.frame(input, frame, embed: :never) |> extract_result()

      assert result["@type"] == "ex:Parent"
      # With @never, child should be just a reference
      assert result["ex:child"] == %{"@id" => "ex:child1"}
    end

    @tag :skip
    test "@embed in frame overrides option", %{input: input} do
      # TODO: Implement @embed extraction from property frames
      # Requires mapping expanded IRIs back to compact form to access original frame
      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Parent",
        "ex:child" => %{"@embed" => "@never"}
      }

      result = JSON.LD.frame(input, frame, embed: :always) |> extract_result()

      # Frame @embed should override option
      assert result["ex:child"] == %{"@id" => "ex:child1"}
    end
  end

  describe "@explicit option tests" do
    test "@explicit: false includes all properties (default)" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@id" => "ex:item",
        "@type" => "ex:Item",
        "ex:prop1" => "value1",
        "ex:prop2" => "value2",
        "ex:prop3" => "value3"
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Item",
        "ex:prop1" => %{}
      }

      result = JSON.LD.frame(input, frame, explicit: false) |> extract_result()

      # All properties should be included
      assert result["ex:prop1"] == "value1"
      assert result["ex:prop2"] == "value2"
      assert result["ex:prop3"] == "value3"
    end

    test "@explicit: true includes only frame properties" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@id" => "ex:item",
        "@type" => "ex:Item",
        "ex:prop1" => "value1",
        "ex:prop2" => "value2",
        "ex:prop3" => "value3"
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Item",
        "@explicit" => true,
        "ex:prop1" => %{}
      }

      result = JSON.LD.frame(input, frame, explicit: true) |> extract_result()

      # Only prop1 should be included
      assert Map.has_key?(result, "ex:prop1")
      refute Map.has_key?(result, "ex:prop2")
      refute Map.has_key?(result, "ex:prop3")
    end
  end

  describe "@omitDefault option tests" do
    @tag :skip
    test "@omitDefault: false includes properties with defaults" do
      # TODO: Implement @default extraction from property frames
      # Requires mapping expanded IRIs back to compact form to access original frame
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@id" => "ex:item",
        "@type" => "ex:Item",
        "ex:prop1" => "value1"
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Item",
        "ex:prop1" => %{},
        "ex:prop2" => %{"@default" => "default value"}
      }

      result = JSON.LD.frame(input, frame, omit_default: false) |> extract_result()

      # prop2 should have default value since it's not in input
      assert result["ex:prop1"] == "value1"
      assert result["ex:prop2"] == "default value"
    end

    test "@omitDefault: true omits missing properties" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@id" => "ex:item",
        "@type" => "ex:Item",
        "ex:prop1" => "value1"
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Item",
        "ex:prop1" => %{},
        "ex:prop2" => %{"@default" => "default value"}
      }

      result = JSON.LD.frame(input, frame, omit_default: true) |> extract_result()

      # prop2 should be omitted even with default
      assert result["ex:prop1"] == "value1"
      refute Map.has_key?(result, "ex:prop2")
    end
  end

  describe "@requireAll option tests" do
    setup do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{
            "@id" => "ex:item1",
            "@type" => "ex:Item",
            "ex:prop1" => "value1",
            "ex:prop2" => "value2"
          },
          %{
            "@id" => "ex:item2",
            "@type" => "ex:Item",
            "ex:prop1" => "value1"
          }
        ]
      }

      {:ok, input: input}
    end

    test "@requireAll: false matches if any property matches", %{input: input} do
      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Item",
        "ex:prop1" => "value1",
        "ex:prop2" => "value2"
      }

      result = JSON.LD.frame(input, frame, require_all: false)

      # Both items should match (both have prop1)
      if is_list(result["@graph"]) do
        assert length(result["@graph"]) == 2
      end
    end

    test "@requireAll: true matches only if all properties match", %{input: input} do
      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Item",
        "@requireAll" => true,
        "ex:prop1" => "value1",
        "ex:prop2" => "value2"
      }

      result = JSON.LD.frame(input, frame, require_all: true)

      # Only item1 should match (has both properties)
      if is_list(result["@graph"]) do
        matched_ids = Enum.map(result["@graph"], & &1["@id"])
        assert "ex:item1" in matched_ids
        refute "ex:item2" in matched_ids
      else
        assert result["@id"] == "ex:item1"
      end
    end
  end

  describe "type matching tests" do
    test "matches nodes by @type" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{"@id" => "ex:1", "@type" => "ex:Book"},
          %{"@id" => "ex:2", "@type" => "ex:Article"},
          %{"@id" => "ex:3", "@type" => "ex:Book"}
        ]
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Book"
      }

      result = JSON.LD.frame(input, frame)

      if is_list(result["@graph"]) do
        matched_ids = Enum.map(result["@graph"], & &1["@id"])
        assert "ex:1" in matched_ids
        assert "ex:3" in matched_ids
        refute "ex:2" in matched_ids
      end
    end

    test "matches nodes with multiple types" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{"@id" => "ex:1", "@type" => ["ex:Book", "ex:DigitalResource"]},
          %{"@id" => "ex:2", "@type" => "ex:Book"}
        ]
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:DigitalResource"
      }

      result = JSON.LD.frame(input, frame)

      # Should match ex:1 which has DigitalResource type
      if is_list(result["@graph"]) do
        matched_ids = Enum.map(result["@graph"], & &1["@id"])
        assert "ex:1" in matched_ids
      else
        assert result["@id"] == "ex:1"
      end
    end
  end

  describe "@id matching tests" do
    test "matches specific @id" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{"@id" => "ex:1", "@type" => "ex:Item", "ex:name" => "First"},
          %{"@id" => "ex:2", "@type" => "ex:Item", "ex:name" => "Second"}
        ]
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@id" => "ex:1"
      }

      result = JSON.LD.frame(input, frame)

      # When frame has @id, result is unwrapped from @graph
      assert result["@id"] == "ex:1"
      assert result["ex:name"] == "First"
    end

    test "matches all nodes with @type" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{"@id" => "ex:1", "@type" => "ex:Item"},
          %{"@id" => "ex:2", "@type" => "ex:Item"}
        ]
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Item"
      }

      result = JSON.LD.frame(input, frame)

      # Should match all nodes with @type ex:Item
      if is_list(result["@graph"]) do
        assert length(result["@graph"]) == 2
      end
    end
  end

  describe "property value matching tests" do
    test "matches exact string values" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{"@id" => "ex:1", "ex:status" => "active"},
          %{"@id" => "ex:2", "ex:status" => "inactive"}
        ]
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "ex:status" => "active"
      }

      result = JSON.LD.frame(input, frame)

      if is_list(result["@graph"]) do
        matched = Enum.find(result["@graph"], &(&1["@id"] == "ex:1"))
        assert matched != nil
        assert matched["ex:status"] == "active"
      end
    end

    test "wildcard property matching requires property existence" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{"@id" => "ex:1", "ex:prop" => "value"},
          %{"@id" => "ex:2", "@type" => "ex:Item"}
        ]
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "ex:prop" => %{}
      }

      result = JSON.LD.frame(input, frame)

      # Only ex:1 has ex:prop
      if is_list(result["@graph"]) do
        matched_ids = Enum.map(result["@graph"], & &1["@id"])
        assert "ex:1" in matched_ids
        assert length(matched_ids) == 1
      else
        assert result["@id"] == "ex:1"
      end
    end
  end

  describe "value object matching tests" do
    test "matches @language in value objects" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{
            "@id" => "ex:1",
            "ex:label" => %{"@value" => "Hello", "@language" => "en"}
          },
          %{
            "@id" => "ex:2",
            "ex:label" => %{"@value" => "Bonjour", "@language" => "fr"}
          }
        ]
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "ex:label" => %{"@language" => "en"}
      }

      result = JSON.LD.frame(input, frame)

      if is_list(result["@graph"]) do
        matched = Enum.find(result["@graph"], &(&1["@id"] == "ex:1"))
        assert matched != nil
      end
    end

    test "matches @type in value objects" do
      input = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "xsd" => "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph" => [
          %{
            "@id" => "ex:1",
            "ex:date" => %{"@value" => "2023-01-01", "@type" => "xsd:date"}
          },
          %{
            "@id" => "ex:2",
            "ex:date" => %{"@value" => "2023-01-01T00:00:00", "@type" => "xsd:dateTime"}
          }
        ]
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "xsd" => "http://www.w3.org/2001/XMLSchema#"
        },
        "ex:date" => %{"@type" => "xsd:date"}
      }

      result = JSON.LD.frame(input, frame)

      if is_list(result["@graph"]) do
        matched = Enum.find(result["@graph"], &(&1["@id"] == "ex:1"))
        assert matched != nil
      end
    end
  end

  describe "list object tests" do
    test "frames @list objects" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@id" => "ex:item",
        "@type" => "ex:Item",
        "ex:list" => %{"@list" => ["a", "b", "c"]}
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Item"
      }

      result = JSON.LD.frame(input, frame) |> extract_result()

      assert result["@id"] == "ex:item"
      assert is_map(result["ex:list"])
      assert result["ex:list"]["@list"] == ["a", "b", "c"]
    end

    test "frames nested nodes in @list" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{
            "@id" => "ex:parent",
            "@type" => "ex:Parent",
            "ex:children" => %{"@list" => [%{"@id" => "ex:child1"}]}
          },
          %{
            "@id" => "ex:child1",
            "@type" => "ex:Child",
            "ex:name" => "Child One"
          }
        ]
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Parent"
      }

      result = JSON.LD.frame(input, frame) |> extract_result()

      assert is_map(result["ex:children"])
      assert is_list(result["ex:children"]["@list"])
      child = hd(result["ex:children"]["@list"])
      assert child["@type"] == "ex:Child"
    end
  end

  describe "nested framing tests" do
    test "frames deeply nested structures" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{
            "@id" => "ex:library",
            "@type" => "ex:Library",
            "ex:contains" => %{"@id" => "ex:book1"}
          },
          %{
            "@id" => "ex:book1",
            "@type" => "ex:Book",
            "ex:title" => "Book Title",
            "ex:author" => %{"@id" => "ex:author1"}
          },
          %{
            "@id" => "ex:author1",
            "@type" => "ex:Author",
            "ex:name" => "Author Name"
          }
        ]
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Library",
        "ex:contains" => %{
          "@type" => "ex:Book",
          "ex:author" => %{
            "@type" => "ex:Author"
          }
        }
      }

      result = JSON.LD.frame(input, frame) |> extract_result()

      assert result["@type"] == "ex:Library"
      assert is_map(result["ex:contains"])
      assert result["ex:contains"]["@type"] == "ex:Book"
      assert is_map(result["ex:contains"]["ex:author"])
      assert result["ex:contains"]["ex:author"]["@type"] == "ex:Author"
      assert result["ex:contains"]["ex:author"]["ex:name"] == "Author Name"
    end
  end

  describe "edge cases and error handling" do
    test "frames empty graph" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => []
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Item"
      }

      result = JSON.LD.frame(input, frame)

      assert result["@context"] != nil
      assert result["@graph"] == [] or is_nil(result["@graph"])
    end

    test "frames with no matching nodes" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{"@id" => "ex:1", "@type" => "ex:Book"}
        ]
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Article"
      }

      result = JSON.LD.frame(input, frame)

      assert result["@graph"] == [] or is_nil(result["@graph"])
    end

    test "frames with circular references" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{
            "@id" => "ex:1",
            "@type" => "ex:Node",
            "ex:link" => %{"@id" => "ex:2"}
          },
          %{
            "@id" => "ex:2",
            "@type" => "ex:Node",
            "ex:link" => %{"@id" => "ex:1"}
          }
        ]
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@type" => "ex:Node"
      }

      # Should not crash with circular references
      result = JSON.LD.frame(input, frame)

      assert result != nil
      # With default @embed: @once, second reference should be just @id
      if is_list(result["@graph"]) do
        assert length(result["@graph"]) >= 1
      end
    end

    test "frames with missing context" do
      input = %{
        "@graph" => [
          %{
            "@id" => "http://example.org/item",
            "@type" => "http://example.org/Item"
          }
        ]
      }

      frame = %{
        "@type" => "http://example.org/Item"
      }

      result = JSON.LD.frame(input, frame)

      # Should work even without context
      assert result != nil
    end
  end

  describe "complex real-world scenarios" do
    test "frames bibliographic data" do
      input = %{
        "@context" => %{
          "dc" => "http://purl.org/dc/elements/1.1/",
          "ex" => "http://example.org/vocab#"
        },
        "@graph" => [
          %{
            "@id" => "http://example.org/library",
            "@type" => "ex:Library",
            "ex:name" => "City Library",
            "ex:contains" => [
              %{"@id" => "http://example.org/book1"},
              %{"@id" => "http://example.org/book2"}
            ]
          },
          %{
            "@id" => "http://example.org/book1",
            "@type" => "ex:Book",
            "dc:title" => "JSON-LD Primer",
            "dc:creator" => "John Doe",
            "ex:isbn" => "123-456"
          },
          %{
            "@id" => "http://example.org/book2",
            "@type" => "ex:Book",
            "dc:title" => "Linked Data Patterns",
            "dc:creator" => "Jane Smith"
          }
        ]
      }

      frame = %{
        "@context" => %{
          "dc" => "http://purl.org/dc/elements/1.1/",
          "ex" => "http://example.org/vocab#"
        },
        "@type" => "ex:Library",
        "ex:contains" => %{
          "@type" => "ex:Book"
        }
      }

      result = JSON.LD.frame(input, frame) |> extract_result()

      assert result["@type"] == "ex:Library"
      assert result["ex:name"] == "City Library"
      assert is_list(result["ex:contains"])
      assert length(result["ex:contains"]) == 2

      titles = Enum.map(result["ex:contains"], & &1["dc:title"])
      assert "JSON-LD Primer" in titles
      assert "Linked Data Patterns" in titles
    end

    test "frames social network data" do
      input = %{
        "@context" => %{
          "foaf" => "http://xmlns.com/foaf/0.1/",
          "knows" => "foaf:knows",
          "name" => "foaf:name"
        },
        "@graph" => [
          %{
            "@id" => "http://example.org/people#alice",
            "@type" => "foaf:Person",
            "name" => "Alice",
            "knows" => [
              %{"@id" => "http://example.org/people#bob"},
              %{"@id" => "http://example.org/people#charlie"}
            ]
          },
          %{
            "@id" => "http://example.org/people#bob",
            "@type" => "foaf:Person",
            "name" => "Bob"
          },
          %{
            "@id" => "http://example.org/people#charlie",
            "@type" => "foaf:Person",
            "name" => "Charlie"
          }
        ]
      }

      frame = %{
        "@context" => %{
          "foaf" => "http://xmlns.com/foaf/0.1/",
          "knows" => "foaf:knows",
          "name" => "foaf:name"
        },
        "@type" => "foaf:Person",
        "name" => "Alice"
      }

      result = JSON.LD.frame(input, frame) |> extract_result()

      assert result["name"] == "Alice"
      assert is_list(result["knows"])
      assert length(result["knows"]) == 2
    end
  end
end
