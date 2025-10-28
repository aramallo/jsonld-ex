defmodule JSON.LD.ScopedContextFramingTest do
  @moduledoc """
  Tests for scoped contexts (type-scoped contexts) in JSON-LD framing.

  Scoped contexts allow defining context rules that only apply within
  specific types or properties, providing fine-grained control over
  term definitions during framing and compaction.
  """

  use ExUnit.Case

  describe "Basic scoped context in frames" do
    test "type-scoped context applies to matched nodes" do
      input = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "Person" => "ex:Person",
          "name" => "ex:name"
        },
        "@graph" => [
          %{
            "@id" => "ex:alice",
            "@type" => "Person",
            "name" => "Alice",
            "ex:email" => "alice@example.org"
          }
        ]
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "name" => "ex:name",
          "Person" => %{
            "@id" => "ex:Person",
            "@context" => %{
              "contact" => "ex:email"
            }
          }
        },
        "@type" => "Person"
      }

      result = JSON.LD.frame(input, frame)

      # The scoped context should add the 'contact' term for ex:email
      assert result["@type"] == "Person"
      assert result["name"] == "Alice"
      assert result["contact"] == "alice@example.org"
      refute Map.has_key?(result, "email")
      refute Map.has_key?(result, "ex:email")
    end

    test "scoped context overrides outer context terms" do
      input = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "title" => "ex:title"
        },
        "@graph" => [
          %{
            "@id" => "ex:book1",
            "@type" => "ex:Book",
            "ex:title" => "JSON-LD Guide"
          }
        ]
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "Book" => %{
            "@id" => "ex:Book",
            "@context" => %{
              "title" => %{
                "@id" => "ex:title",
                "@container" => "@set"
              }
            }
          }
        },
        "@type" => "Book"
      }

      result = JSON.LD.frame(input, frame)

      # Scoped context should force title to be an array
      assert result["@type"] == "Book"
      assert is_list(result["title"])
      assert result["title"] == ["JSON-LD Guide"]
    end

    test "scoped context with @vocab applies within scope" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{
            "@id" => "ex:product1",
            "@type" => "ex:Product",
            "http://schema.org/name" => "Widget",
            "http://schema.org/price" => "19.99"
          }
        ]
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "Product" => %{
            "@id" => "ex:Product",
            "@context" => %{
              "@vocab" => "http://schema.org/"
            }
          }
        },
        "@type" => "Product"
      }

      result = JSON.LD.frame(input, frame)

      # Properties should be compacted using schema.org vocab
      assert result["name"] == "Widget"
      assert result["price"] == "19.99"
    end
  end

  describe "Nested scoped contexts" do
    test "multiple levels of type-scoped contexts" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{
            "@id" => "ex:org1",
            "@type" => "ex:Organization",
            "ex:hasMember" => %{"@id" => "ex:person1"}
          },
          %{
            "@id" => "ex:person1",
            "@type" => "ex:Person",
            "ex:name" => "Alice",
            "ex:hasRole" => %{"@id" => "ex:role1"}
          },
          %{
            "@id" => "ex:role1",
            "@type" => "ex:Role",
            "ex:title" => "Engineer"
          }
        ]
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "Organization" => %{
            "@id" => "ex:Organization",
            "@context" => %{
              "members" => %{"@id" => "ex:hasMember", "@type" => "@id"}
            }
          },
          "Person" => %{
            "@id" => "ex:Person",
            "@context" => %{
              "fullName" => "ex:name",
              "roles" => %{"@id" => "ex:hasRole", "@type" => "@id"}
            }
          },
          "Role" => %{
            "@id" => "ex:Role",
            "@context" => %{
              "jobTitle" => "ex:title"
            }
          }
        },
        "@type" => "Organization",
        "members" => %{
          "@type" => "Person",
          "roles" => %{
            "@type" => "Role"
          }
        }
      }

      result = JSON.LD.frame(input, frame)

      # Each level should apply its scoped context
      assert result["@type"] == "Organization"
      assert is_map(result["members"])
      assert result["members"]["@type"] == "Person"
      assert result["members"]["fullName"] == "Alice"
      assert is_map(result["members"]["roles"])
      assert result["members"]["roles"]["@type"] == "Role"
      assert result["members"]["roles"]["jobTitle"] == "Engineer"
    end

    test "scoped context in embedded objects" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{
            "@id" => "ex:article1",
            "@type" => "ex:Article",
            "ex:author" => %{
              "@id" => "ex:author1",
              "@type" => "ex:Author",
              "ex:givenName" => "John",
              "ex:familyName" => "Doe"
            }
          }
        ]
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "Article" => "ex:Article",
          "Author" => %{
            "@id" => "ex:Author",
            "@context" => %{
              "firstName" => "ex:givenName",
              "lastName" => "ex:familyName"
            }
          },
          "author" => %{"@id" => "ex:author", "@type" => "@id"}
        },
        "@type" => "Article",
        "author" => %{
          "@type" => "Author"
        }
      }

      result = JSON.LD.frame(input, frame)

      # Embedded author should use scoped context
      assert result["@type"] == "Article"
      assert result["author"]["@type"] == "Author"
      assert result["author"]["firstName"] == "John"
      assert result["author"]["lastName"] == "Doe"
      refute Map.has_key?(result["author"], "givenName")
      refute Map.has_key?(result["author"], "familyName")
    end
  end

  describe "Scoped context with framing options" do
    test "scoped context with @explicit framing" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@id" => "ex:item1",
        "@type" => "ex:Product",
        "ex:name" => "Widget",
        "ex:price" => "19.99",
        "ex:sku" => "WID-001",
        "ex:stock" => 42
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "Product" => %{
            "@id" => "ex:Product",
            "@context" => %{
              "productName" => "ex:name",
              "cost" => "ex:price"
            }
          }
        },
        "@type" => "Product",
        "@explicit" => true,
        "productName" => %{},
        "cost" => %{}
      }

      result = JSON.LD.frame(input, frame, explicit: true)

      # Only explicit properties with scoped context terms
      assert result["productName"] == "Widget"
      assert result["cost"] == "19.99"
      refute Map.has_key?(result, "sku")
      refute Map.has_key?(result, "stock")
      refute Map.has_key?(result, "ex:sku")
      refute Map.has_key?(result, "ex:stock")
    end

    test "scoped context with @embed directive" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{
            "@id" => "ex:post1",
            "@type" => "ex:BlogPost",
            "ex:author" => %{"@id" => "ex:author1"}
          },
          %{
            "@id" => "ex:author1",
            "@type" => "ex:Author",
            "ex:name" => "Jane Smith"
          }
        ]
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "BlogPost" => "ex:BlogPost",
          "author" => "ex:author",
          "Author" => %{
            "@id" => "ex:Author",
            "@context" => %{
              "authorName" => "ex:name"
            }
          }
        },
        "@type" => "BlogPost",
        "author" => %{
          "@type" => "Author",
          "@embed" => "@never"
        }
      }

      result = JSON.LD.frame(input, frame)

      # Author should not be embedded, just referenced
      # When @embed: @never is used, the value becomes a node reference
      assert result["@type"] == "BlogPost"
      assert is_map(result["author"])
      assert result["author"]["@id"] == "ex:author1"
      refute Map.has_key?(result["author"], "authorName")
      refute Map.has_key?(result["author"], "ex:name")
    end
  end

  describe "Multiple types with different scoped contexts" do
    test "different scoped contexts for different types" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{
            "@id" => "ex:person1",
            "@type" => "ex:Person",
            "ex:identifier" => "P123"
          },
          %{
            "@id" => "ex:product1",
            "@type" => "ex:Product",
            "ex:identifier" => "SKU456"
          }
        ]
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "Person" => %{
            "@id" => "ex:Person",
            "@context" => %{
              "personId" => "ex:identifier"
            }
          },
          "Product" => %{
            "@id" => "ex:Product",
            "@context" => %{
              "sku" => "ex:identifier"
            }
          }
        },
        "@type" => ["Person", "Product"]
      }

      result = JSON.LD.frame(input, frame)

      # Should return @graph with both objects using their respective scoped contexts
      assert is_list(result["@graph"])
      assert length(result["@graph"]) == 2

      person = Enum.find(result["@graph"], fn item -> item["@type"] == "Person" end)
      product = Enum.find(result["@graph"], fn item -> item["@type"] == "Product" end)

      assert person["personId"] == "P123"
      assert product["sku"] == "SKU456"
    end

    test "node matching multiple types uses appropriate scoped contexts" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@id" => "ex:item1",
        "@type" => ["ex:Document", "ex:Article"],
        "ex:title" => "JSON-LD Framing",
        "ex:created" => "2024-01-01"
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "Document" => %{
            "@id" => "ex:Document",
            "@context" => %{
              "docTitle" => "ex:title"
            }
          },
          "Article" => %{
            "@id" => "ex:Article",
            "@context" => %{
              "headline" => "ex:title"
            }
          }
        },
        "@type" => "Document"
      }

      result = JSON.LD.frame(input, frame)

      # Should use Document's scoped context since that's the frame type
      # Types are compacted using term definitions from context
      assert result["@type"] == ["Document", "Article"]
      # Document's scoped context should be applied
      assert result["docTitle"] == "JSON-LD Framing"
      assert result["ex:created"] == "2024-01-01"
    end
  end

  describe "Scoped context edge cases" do
    test "empty scoped context" do
      input = %{
        "@context" => %{"ex" => "http://example.org/", "name" => "ex:name"},
        "@id" => "ex:item1",
        "@type" => "ex:Thing",
        "name" => "Test"
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "name" => "ex:name",
          "Thing" => %{
            "@id" => "ex:Thing",
            "@context" => %{}
          }
        },
        "@type" => "Thing"
      }

      result = JSON.LD.frame(input, frame)

      # Empty scoped context shouldn't break anything
      # Outer context terms should still work
      assert result["@type"] == "Thing"
      assert result["name"] == "Test"
    end

    test "scoped context with @container: @set matches W3C test" do
      # This matches the structure of W3C test t0062
      input = %{
        "@context" => %{
          "@version" => 1.1,
          "@vocab" => "http://example.org/vocab#"
        },
        "@id" => "http://example.org/1",
        "@type" => "HumanMadeObject",
        "produced_by" => %{
          "@type" => "Production",
          "_label" => "Top Production",
          "part" => %{
            "@type" => "Production",
            "_label" => "Test Part"
          }
        }
      }

      frame = %{
        "@context" => %{
          "@version" => 1.1,
          "@vocab" => "http://example.org/vocab#",
          "Production" => %{
            "@context" => %{
              "part" => %{
                "@type" => "@id",
                "@container" => "@set"
              }
            }
          }
        },
        "@id" => "http://example.org/1"
      }

      result = JSON.LD.frame(input, frame)

      # The part property should be in an array due to @set container
      assert result["@type"] == "HumanMadeObject"
      assert result["produced_by"]["@type"] == "Production"
      assert is_list(result["produced_by"]["part"])
      assert length(result["produced_by"]["part"]) == 1
      assert hd(result["produced_by"]["part"])["@type"] == "Production"
    end

    test "scoped context with protected terms" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@id" => "ex:item1",
        "@type" => "ex:ProtectedType",
        "ex:name" => "Test",
        "ex:value" => "42"
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "ProtectedType" => %{
            "@id" => "ex:ProtectedType",
            "@context" => %{
              "@protected" => true,
              "name" => "ex:name"
            }
          }
        },
        "@type" => "ProtectedType"
      }

      result = JSON.LD.frame(input, frame)

      # Protected terms in scoped context should work
      assert result["@type"] == "ProtectedType"
      assert result["name"] == "Test"
    end

    test "scoped context doesn't leak to siblings" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{
            "@id" => "ex:special1",
            "@type" => "ex:Special",
            "ex:value" => "special"
          },
          %{
            "@id" => "ex:normal1",
            "@type" => "ex:Normal",
            "ex:value" => "normal"
          }
        ]
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "Special" => %{
            "@id" => "ex:Special",
            "@context" => %{
              "specialValue" => "ex:value"
            }
          },
          "Normal" => "ex:Normal"
        },
        "@type" => ["Special", "Normal"]
      }

      result = JSON.LD.frame(input, frame)

      assert is_list(result["@graph"])

      special = Enum.find(result["@graph"], fn item -> item["@type"] == "Special" end)
      normal = Enum.find(result["@graph"], fn item -> item["@type"] == "Normal" end)

      # Special should use scoped context
      assert special["specialValue"] == "special"
      refute Map.has_key?(special, "ex:value")

      # Normal should NOT use scoped context
      assert normal["ex:value"] == "normal"
      refute Map.has_key?(normal, "specialValue")
    end
  end

  describe "Property-scoped contexts in frames" do
    test "property-level scoped context" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@id" => "ex:event1",
        "@type" => "ex:Event",
        "ex:location" => %{
          "@id" => "ex:loc1",
          "ex:lat" => "40.7",
          "ex:long" => "-74.0",
          "ex:name" => "New York"
        }
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "Event" => "ex:Event",
          "location" => %{
            "@id" => "ex:location",
            "@context" => %{
              "latitude" => "ex:lat",
              "longitude" => "ex:long",
              "placeName" => "ex:name"
            }
          }
        },
        "@type" => "Event",
        "location" => %{}
      }

      result = JSON.LD.frame(input, frame)

      # Location should use its property-scoped context
      assert result["@type"] == "Event"
      assert result["location"]["latitude"] == "40.7"
      assert result["location"]["longitude"] == "-74.0"
      assert result["location"]["placeName"] == "New York"
    end
  end

  describe "Advanced scoped context features" do
    test "@propagate: true allows scoped context to affect nested nodes" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{
            "@id" => "ex:parent1",
            "@type" => "ex:Parent",
            "ex:name" => "Parent Name",
            "ex:child" => %{
              "@id" => "ex:child1",
              "@type" => "ex:Child",
              "ex:name" => "Child Name"
            }
          }
        ]
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "Parent" => %{
            "@id" => "ex:Parent",
            "@context" => %{
              "@propagate" => true,
              "name" => "ex:name"
            }
          }
        },
        "@type" => "Parent",
        "ex:child" => %{}
      }

      result = JSON.LD.frame(input, frame)

      # Parent should use scoped context
      assert result["@type"] == "Parent"
      assert result["name"] == "Parent Name"

      # With @propagate: true, child should also use parent's scoped context
      assert result["ex:child"]["@type"] == "ex:Child"
      assert result["ex:child"]["name"] == "Child Name"
      refute Map.has_key?(result["ex:child"], "ex:name")
    end

    test "@propagate: false prevents scoped context from affecting nested nodes" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{
            "@id" => "ex:parent1",
            "@type" => "ex:Parent",
            "ex:name" => "Parent Name",
            "ex:child" => %{
              "@id" => "ex:child1",
              "@type" => "ex:Child",
              "ex:name" => "Child Name"
            }
          }
        ]
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "Parent" => %{
            "@id" => "ex:Parent",
            "@context" => %{
              "@propagate" => false,
              "name" => "ex:name"
            }
          }
        },
        "@type" => "Parent",
        "ex:child" => %{}
      }

      result = JSON.LD.frame(input, frame)

      # Parent should use scoped context
      assert result["@type"] == "Parent"
      assert result["name"] == "Parent Name"

      # With @propagate: false, child should NOT use parent's scoped context
      assert result["ex:child"]["@type"] == "ex:Child"
      assert result["ex:child"]["ex:name"] == "Child Name"
      refute Map.has_key?(result["ex:child"], "name")
    end

    test "null scoped context resets to base context" do
      input = %{
        "@context" => %{"ex" => "http://example.org/", "name" => "ex:name"},
        "@graph" => [
          %{
            "@id" => "ex:item1",
            "@type" => "ex:Thing",
            "name" => "Test Item"
          }
        ]
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "name" => "ex:name",
          "Thing" => %{
            "@id" => "ex:Thing",
            "@context" => nil
          }
        },
        "@type" => "Thing"
      }

      result = JSON.LD.frame(input, frame)

      # null context should reset, so 'name' term is no longer defined
      # Data should appear with full IRI
      assert result["@type"] == "Thing"
      assert result["http://example.org/name"] == "Test Item"
      refute Map.has_key?(result, "name")
    end

    test "array scoped context merges multiple context sources" do
      input = %{
        "@context" => %{"ex" => "http://example.org/"},
        "@graph" => [
          %{
            "@id" => "ex:item1",
            "@type" => "ex:Product",
            "ex:name" => "Widget",
            "ex:price" => "19.99",
            "ex:sku" => "WID-001"
          }
        ]
      }

      frame = %{
        "@context" => %{
          "ex" => "http://example.org/",
          "Product" => %{
            "@id" => "ex:Product",
            "@context" => [
              %{"title" => "ex:name"},
              %{"cost" => "ex:price"},
              %{"itemCode" => "ex:sku"}
            ]
          }
        },
        "@type" => "Product"
      }

      result = JSON.LD.frame(input, frame)

      # Array of contexts should all be applied
      assert result["@type"] == "Product"
      assert result["title"] == "Widget"
      assert result["cost"] == "19.99"
      assert result["itemCode"] == "WID-001"
      refute Map.has_key?(result, "ex:name")
      refute Map.has_key?(result, "ex:price")
      refute Map.has_key?(result, "ex:sku")
    end
  end
end
