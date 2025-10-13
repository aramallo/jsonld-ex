defmodule JSON.LD.NodeIdentifierMap do
  @moduledoc false

  @type t() :: %__MODULE__{}

  defstruct tab: nil

  # Client API

  @doc """
  Creates a Blank Node identifier map using an anonymous `ets` table.
  """
  @spec new() :: :ets.tid()

  def new() do
    tab = :ets.new(:undefined, [:set, {:keypos, 1}])
    # Initialize counter at 0
    :ets.insert(tab, {:__counter__, 0})
    struct!(__MODULE__, tab: tab)
  end

  @doc """
  Deletes the blank node identifier map (deleting the underlying `ets` table)
  """
  @spec delete(t()) :: :ok

  def delete(%__MODULE__{tab: tab}) do
    _ = :ets.delete(tab)
    :ok
  end

  @doc """
  Generate Blank Node Identifier

  Details at <https://www.w3.org/TR/json-ld-api/#generate-blank-node-identifier>
  """
  @spec generate_blank_node_id(t(), String.t() | nil) :: String.t()

  def generate_blank_node_id(map, identifier \\ nil)

  def generate_blank_node_id(%__MODULE__{tab: tab}, nil) do
    blank_node_id(tab)
  end

  def generate_blank_node_id(%__MODULE__{tab: tab}, identifier) do
    case :ets.lookup(tab, identifier) do
      [] ->
        blank_node_id = blank_node_id(tab)
        :ets.insert(tab, {identifier, blank_node_id})
        blank_node_id

      [{^identifier, blank_node_id}] ->
        blank_node_id
    end
  end

  defp blank_node_id(tab) do
    # Atomically increment and get the counter from ETS
    counter = :ets.update_counter(tab, :__counter__, 1)
    "_:b#{counter - 1}"
  end
end
