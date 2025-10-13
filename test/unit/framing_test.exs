defmodule JSON.LD.FramingTest do
  use ExUnit.Case

  doctest JSON.LD.Framing

  describe "JSON.LD.frame/3" do
    test "basic framing with type matching" do
      input = %{
        "@context" => %{
          "dc" => "http://purl.org/dc/elements/1.1/",
          "ex" => "http://example.org/vocab#"
        },
        "@graph" => [
          %{
            "@id" => "http://example.org/library",
            "@type" => "ex:Library",
            "ex:contains" => %{"@id" => "http://example.org/book1"}
          },
          %{
            "@id" => "http://example.org/book1",
            "@type" => "ex:Book",
            "dc:title" => "Example Book"
          }
        ]
      }

      frame = %{
        "@context" => %{
          "dc" => "http://purl.org/dc/elements/1.1/",
          "ex" => "http://example.org/vocab#"
        },
        "@type" => "ex:Library",
        "ex:contains" => %{}
      }

      result = JSON.LD.frame(input, frame)

      # In JSON-LD 1.1, single matches are unwrapped from @graph
      assert result["@type"] == "ex:Library"
      assert result["@id"] == "http://example.org/library"
      assert is_map(result["ex:contains"])
      assert result["ex:contains"]["@type"] == "ex:Book"
      assert result["ex:contains"]["dc:title"] == "Example Book"
    end

    test "framing with @embed: @never" do
      input = %{
        "@context" => %{"ex" => "http://example.org/vocab#"},
        "@graph" => [
          %{
            "@id" => "http://example.org/parent",
            "@type" => "ex:Parent",
            "ex:child" => %{"@id" => "http://example.org/child"}
          },
          %{
            "@id" => "http://example.org/child",
            "@type" => "ex:Child",
            "ex:name" => "Child Name"
          }
        ]
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/vocab#"},
        "@type" => "ex:Parent",
        "ex:child" => %{
          "@embed" => "@never"
        }
      }

      result = JSON.LD.frame(input, frame, embed: :never)

      # In JSON-LD 1.1, single matches are unwrapped from @graph
      # With @embed: @never, child should be just a reference
      assert result["ex:child"] == %{"@id" => "http://example.org/child"}
    end

    test "framing with @explicit: true" do
      input = %{
        "@context" => %{"ex" => "http://example.org/vocab#"},
        "@id" => "http://example.org/item",
        "@type" => "ex:Item",
        "ex:prop1" => "value1",
        "ex:prop2" => "value2",
        "ex:prop3" => "value3"
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/vocab#"},
        "@type" => "ex:Item",
        "@explicit" => true,
        "ex:prop1" => %{}
      }

      result = JSON.LD.frame(input, frame, explicit: true)

      # In JSON-LD 1.1, single matches are unwrapped from @graph
      # Only prop1 should be included (plus @id and @type)
      assert Map.has_key?(result, "ex:prop1")
      refute Map.has_key?(result, "ex:prop2")
      refute Map.has_key?(result, "ex:prop3")
    end

    test "framing with property matching" do
      input = %{
        "@context" => %{"ex" => "http://example.org/vocab#"},
        "@graph" => [
          %{
            "@id" => "http://example.org/item1",
            "@type" => "ex:Item",
            "ex:status" => "active"
          },
          %{
            "@id" => "http://example.org/item2",
            "@type" => "ex:Item",
            "ex:status" => "inactive"
          }
        ]
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/vocab#"},
        "@type" => "ex:Item",
        "ex:status" => "active"
      }

      result = JSON.LD.frame(input, frame)

      # In JSON-LD 1.1, single matches are unwrapped from @graph
      # Should only match item1 with status "active"
      assert result["@id"] == "http://example.org/item1"
      assert result["ex:status"] == "active"
    end

    test "framing with wildcard matching" do
      input = %{
        "@context" => %{"ex" => "http://example.org/vocab#"},
        "@graph" => [
          %{
            "@id" => "http://example.org/item1",
            "@type" => "ex:Item",
            "ex:property" => "value1"
          },
          %{
            "@id" => "http://example.org/item2",
            "@type" => "ex:Other"
          }
        ]
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/vocab#"},
        "@type" => "ex:Item",
        "ex:property" => %{}
      }

      result = JSON.LD.frame(input, frame)

      # Wildcard {} means "has the property with any value"
      # Should only match item1
      if is_list(result["@graph"]) do
        items = result["@graph"]
        assert length(items) == 1
        assert hd(items)["@id"] == "http://example.org/item1"
      else
        assert result["@id"] == "http://example.org/item1"
      end
    end

    test "framing empty input returns empty result" do
      input = %{
        "@context" => %{"ex" => "http://example.org/vocab#"},
        "@graph" => []
      }

      frame = %{
        "@context" => %{"ex" => "http://example.org/vocab#"},
        "@type" => "ex:Item"
      }

      result = JSON.LD.frame(input, frame)

      # Should return structure with empty @graph or no matches
      assert result["@context"] != nil
      assert result["@graph"] == [] or is_nil(result["@graph"])
    end
  end
end
