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

    test "user test" do
      input = %{
        "@context" => %{"@vocab" => "user:"},
        "@id" => "urn:user:id:TEST",
        "@type" => "ClassificationFacet",
        "account" => %{"@id" => "urn:user:id:Test"},
        "classification_scheme" => %{"@id" => "urn:user:id:CS1"},
        "created_at" => 1_761_663_884_124,
        "created_by" => "system",
        "external_context" => [
          %{
            "$id" => "urn:example:schema:is-equipment-affected",
            "$schema" => "https://json-schema.org/draft/2020-12/schema",
            "@context" => %{
              "$id" => "@id",
              "$schema" => %{
                "@id" => "https://json-schema.org/meta#schema",
                "@type" => "@id"
              },
              "$vocabulary" => %{
                "@container" => "@index",
                "@id" => "https://json-schema.org/meta#vocabulary",
                "@type" => "@id"
              },
              "@version" => 1.1,
              "@vocab" => "https://json-schema.org/draft/2020-12/vocab/core#",
              "additionalProperties" => %{
                "@id" =>
                  "https://json-schema.org/draft/2020-12/vocab/applicator#additionalProperties"
              },
              "default" => %{
                "@id" => "https://json-schema.org/draft/2020-12/vocab/meta-data#default"
              },
              "description" => %{
                "@id" => "https://json-schema.org/draft/2020-12/vocab/meta-data#description"
              },
              "properties" => %{
                "@container" => "@index",
                "@id" => "https://json-schema.org/draft/2020-12/vocab/applicator#properties"
              },
              "required" => %{
                "@container" => "@set",
                "@id" => "https://json-schema.org/draft/2020-12/vocab/validation#required"
              },
              "title" => %{
                "@id" => "https://json-schema.org/draft/2020-12/vocab/meta-data#title"
              },
              "type" => %{
                "@id" => "https://json-schema.org/draft/2020-12/vocab/validation#type"
              }
            },
            "additionalProperties" => false,
            "properties" => %{
              "foo" => %{
                "default" => false,
                "type" => "boolean",
                "description" => "A description.",
                "title" => "A title"
              }
            },
            "required" => ["foo"],
            "type" => "object"
          }
        ],
        "modified_at" => [1_761_663_884_124],
        "modified_by" => ["system"],
        "name" => ["Step 1"]
      }

      frame = %{
        "@context" => %{"@vocab" => "user:"},
        "@explicit" => false,
        "@type" => "ClassificationFacet",
        "user:account" => %{"@type" => %{"@default" => "Account"}},
        "user:classification_scheme" => %{
          "@type" => %{"@default" => "ClassificationScheme"}
        },
        "external_context" => %{
          "@context" => %{
            "$id" => "@id",
            "$schema" => %{
              "@id" => "https://json-schema.org/meta#schema",
              "@type" => "@id"
            },
            "@version" => 1.1,
            "@vocab" => "https://json-schema.org/draft/2020-12/vocab/core#",
            "additionalProperties" => %{
              "@id" =>
                "https://json-schema.org/draft/2020-12/vocab/applicator#additionalProperties"
            },
            "default" => %{
              "@id" => "https://json-schema.org/draft/2020-12/vocab/meta-data#default"
            },
            "description" => %{
              "@id" => "https://json-schema.org/draft/2020-12/vocab/meta-data#description"
            },
            "properties" => %{
              "@container" => "@index",
              "@id" => "https://json-schema.org/draft/2020-12/vocab/applicator#properties"
            },
            "required" => %{
              "@container" => "@set",
              "@id" => "https://json-schema.org/draft/2020-12/vocab/validation#required"
            },
            "title" => %{
              "@id" => "https://json-schema.org/draft/2020-12/vocab/meta-data#title"
            },
            "type" => %{
              "@id" => "https://json-schema.org/draft/2020-12/vocab/validation#type"
            }
          },
          "@explicit" => false
        }
      }

      expected = %{
        "@context" => %{"@vocab" => "user:"},
        "@id" => "urn:user:id:TEST",
        "@type" => "ClassificationFacet",
        "account" => %{"@id" => "urn:user:id:Test", "@type" => "Account"},
        "classification_scheme" => %{
          "@id" => "urn:user:id:CS1",
          "@type" => "ClassificationScheme"
        },
        "created_at" => 1_761_663_884_124,
        "created_by" => "system",
        "external_context" => %{
          "@context" => %{
            "$id" => "@id",
            "$schema" => %{
              "@id" => "https://json-schema.org/meta#schema",
              "@type" => "@id"
            },
            "@version" => 1.1,
            "@vocab" => "https://json-schema.org/draft/2020-12/vocab/core#",
            "additionalProperties" => %{
              "@id" =>
                "https://json-schema.org/draft/2020-12/vocab/applicator#additionalProperties"
            },
            "default" => %{
              "@id" => "https://json-schema.org/draft/2020-12/vocab/meta-data#default"
            },
            "description" => %{
              "@id" => "https://json-schema.org/draft/2020-12/vocab/meta-data#description"
            },
            "properties" => %{
              "@container" => "@index",
              "@id" => "https://json-schema.org/draft/2020-12/vocab/applicator#properties"
            },
            "required" => %{
              "@container" => "@set",
              "@id" => "https://json-schema.org/draft/2020-12/vocab/validation#required"
            },
            "title" => %{
              "@id" => "https://json-schema.org/draft/2020-12/vocab/meta-data#title"
            },
            "type" => %{
              "@id" => "https://json-schema.org/draft/2020-12/vocab/validation#type"
            }
          },
          "$id" => "urn:example:schema:is-equipment-affected",
          "$schema" => "https://json-schema.org/draft/2020-12/schema",
          "additionalProperties" => false,
          "properties" => %{
            "foo" => %{
              "default" => false,
              "type" => "boolean",
              "description" => "A description.",
              "title" => "A title"
            }
          },
          "required" => ["foo"],
          "type" => "object"
        },
        "modified_at" => 1_761_663_884_124,
        "modified_by" => "system",
        "name" => "Step 1"
      }

      result = JSON.LD.frame(input, frame)
      assert expected == result
    end
  end
end
