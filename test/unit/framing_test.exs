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
        "modified_at" => 1_761_663_884_124,
        "modified_by" => "system",
        "name" => "Step 1"
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

    test "user test 2" do
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

      input = user_test_input()
      expected = user_test_output()
      result = JSON.LD.frame(input, frame)
      assert expected == result
    end

    test "user test 3" do
      frame = %{
        "@context" => %{"@vocab" => "user:"},
        "@explicit" => false,
        "@type" => "user:ClassificationFacet",
        "user:account" => %{"@type" => %{"@default" => "user:Account"}},
        "user:classification_scheme" => %{
          "@type" => %{"@default" => "user:ClassificationScheme"}
        },
        "user:external_context" => %{
          "@context" => %{
            "$id" => "@id",
            "$schema" => %{"@id" => "https://json-schema.org/meta#schema", "@type" => "@id"},
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
            "title" => %{"@id" => "https://json-schema.org/draft/2020-12/vocab/meta-data#title"},
            "type" => %{"@id" => "https://json-schema.org/draft/2020-12/vocab/validation#type"}
          },
          "@explicit" => false
        }
      }

      input = user_test_input()
      expected = user_test_output()
      result = JSON.LD.frame(input, frame)
      assert expected == result
    end

    test "framing multilanguage labels preserves @value and @language" do
      input = %{
        "@context" => %{
          "ex" => "http://example.org/vocab#",
          "rdfs" => "http://www.w3.org/2000/01/rdf-schema#"
        },
        "@graph" => [
          %{
            "@id" => "http://example.org/resource1",
            "@type" => "ex:Item",
            "rdfs:label" => [
              %{"@value" => "Hello", "@language" => "en"},
              %{"@value" => "Hola", "@language" => "es"},
              %{"@value" => "Bonjour", "@language" => "fr"}
            ],
            "ex:description" => [
              %{"@value" => "An example item", "@language" => "en"},
              %{"@value" => "Un artículo de ejemplo", "@language" => "es"}
            ]
          },
          %{
            "@id" => "http://example.org/resource2",
            "@type" => "ex:Item",
            "rdfs:label" => %{"@value" => "World", "@language" => "en"}
          }
        ]
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/vocab#",
          "rdfs" => "http://www.w3.org/2000/01/rdf-schema#"
        },
        "@type" => "ex:Item"
      }

      result = JSON.LD.frame(input, frame)

      # Result should be an array wrapped in @graph or unwrapped items
      items =
        if is_list(result["@graph"]) do
          result["@graph"]
        else
          # Single item or multiple items
          if is_list(result) do
            result
          else
            [result]
          end
        end

      # Find resource1 in the results
      resource1 = Enum.find(items, fn item -> item["@id"] == "http://example.org/resource1" end)
      assert resource1 != nil, "resource1 should be in framed output"

      # Check that labels are preserved with @value and @language
      labels = List.wrap(resource1["rdfs:label"])
      assert length(labels) == 3, "Should have 3 language variants"

      # Verify each label has both @value and @language
      en_label = Enum.find(labels, fn l -> l["@language"] == "en" end)
      assert en_label != nil, "English label should exist"
      assert en_label["@value"] == "Hello", "English label @value should be 'Hello'"
      assert Map.has_key?(en_label, "@language"), "English label should have @language"

      es_label = Enum.find(labels, fn l -> l["@language"] == "es" end)
      assert es_label != nil, "Spanish label should exist"
      assert es_label["@value"] == "Hola", "Spanish label @value should be 'Hola'"
      assert Map.has_key?(es_label, "@language"), "Spanish label should have @language"

      fr_label = Enum.find(labels, fn l -> l["@language"] == "fr" end)
      assert fr_label != nil, "French label should exist"
      assert fr_label["@value"] == "Bonjour", "French label @value should be 'Bonjour'"
      assert Map.has_key?(fr_label, "@language"), "French label should have @language"

      # Check descriptions as well
      descriptions = List.wrap(resource1["ex:description"])
      assert length(descriptions) == 2, "Should have 2 description variants"

      en_desc = Enum.find(descriptions, fn d -> d["@language"] == "en" end)
      assert en_desc != nil, "English description should exist"
      assert en_desc["@value"] == "An example item"
      assert Map.has_key?(en_desc, "@language")

      es_desc = Enum.find(descriptions, fn d -> d["@language"] == "es" end)
      assert es_desc != nil, "Spanish description should exist"
      assert es_desc["@value"] == "Un artículo de ejemplo"
      assert Map.has_key?(es_desc, "@language")

      # Verify resource2 also maintains its language tag
      resource2 = Enum.find(items, fn item -> item["@id"] == "http://example.org/resource2" end)
      assert resource2 != nil, "resource2 should be in framed output"

      resource2_label = resource2["rdfs:label"]

      # It could be a single map or wrapped in a list
      resource2_label_map =
        if is_list(resource2_label), do: hd(resource2_label), else: resource2_label

      assert resource2_label_map["@value"] == "World"
      assert resource2_label_map["@language"] == "en"
    end
  end

  defp user_test_input() do
    %{
      "@context" => %{"@vocab" => "user:"},
      "@graph" => [
        %{
          "@id" => "_:b3110415",
          "https://json-schema.org/draft/2020-12/vocab/applicator#additionalProperties" => [
            %{"@value" => false}
          ],
          "https://json-schema.org/draft/2020-12/vocab/applicator#properties" => [
            %{"@id" => "_:b0"}
          ],
          "https://json-schema.org/draft/2020-12/vocab/meta-data#default" => [
            %{"@value" => false}
          ],
          "https://json-schema.org/draft/2020-12/vocab/meta-data#description" => [
            %{"@value" => "Indicates whether the equipment is affected."}
          ],
          "https://json-schema.org/draft/2020-12/vocab/meta-data#title" => [
            %{"@value" => "Is Equipment Affected"}
          ],
          "https://json-schema.org/draft/2020-12/vocab/validation#required" => [
            %{"@value" => "is_equipment_affected"}
          ],
          "https://json-schema.org/draft/2020-12/vocab/validation#type" => [
            %{"@value" => "boolean"},
            %{"@value" => "object"}
          ],
          "https://json-schema.org/meta#schema" => [
            %{"@id" => "https://json-schema.org/draft/2020-12/schema"}
          ]
        },
        %{
          "@id" => "_:b3970062",
          "user:index" => [%{"@value" => 1}],
          "user:key" => [%{"@value" => "phase"}],
          "user:value" => [%{"@value" => "SURF"}]
        },
        %{
          "@id" => "_:b3970574",
          "user:index" => [%{"@value" => 3}],
          "user:key" => [%{"@value" => "activity"}],
          "user:value" => [%{"@value" => "FOO"}]
        },
        %{
          "@id" => "_:b3971086",
          "user:index" => [%{"@value" => 4}],
          "user:key" => [%{"@value" => "step"}],
          "user:value" => [%{"@value" => "42"}]
        },
        %{
          "@id" => "urn:user:id:TEST",
          "@type" => ["user:ClassificationFacet"],
          "user:account" => [%{"@id" => "urn:user:id:Test"}],
          "user:classification_scheme" => [%{"@id" => "urn:user:id:CS1"}],
          "user:created_at" => [%{"@value" => 1_761_663_884_124}],
          "user:created_by" => [%{"@value" => "system"}],
          "user:external_context" => [%{"@id" => "_:b3110415"}],
          "user:modified_at" => [%{"@value" => 1_761_663_884_124}],
          "user:modified_by" => [%{"@value" => "system"}],
          "user:name" => [%{"@value" => "Step 1"}],
          "user:property" => [
            %{"@id" => "_:b3970062"},
            %{"@id" => "_:b3970574"},
            %{"@id" => "_:b3971086"}
          ]
        }
      ],
      "@id" => "urn:user:graph:default"
    }
  end

  defp user_test_output() do
    %{
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
        "$schema" => "https://json-schema.org/draft/2020-12/schema",
        "@context" => %{
          "$id" => "@id",
          "$schema" => %{
            "@id" => "https://json-schema.org/meta#schema",
            "@type" => "@id"
          },
          "@version" => 1.1,
          "@vocab" => "https://json-schema.org/draft/2020-12/vocab/core#",
          "additionalProperties" => %{
            "@id" => "https://json-schema.org/draft/2020-12/vocab/applicator#additionalProperties"
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
        "default" => false,
        "description" => "Indicates whether the equipment is affected.",
        "properties" => %{"@none" => %{}},
        "required" => ["is_equipment_affected"],
        "title" => "Is Equipment Affected",
        "type" => ["boolean", "object"]
      },
      "modified_at" => 1_761_663_884_124,
      "modified_by" => "system",
      "name" => "Step 1",
      "property" => [
        %{"index" => 1, "key" => "phase", "value" => "SURF"},
        %{"index" => 3, "key" => "activity", "value" => "FOO"},
        %{"index" => 4, "key" => "step", "value" => "42"}
      ]
    }
  end
end
