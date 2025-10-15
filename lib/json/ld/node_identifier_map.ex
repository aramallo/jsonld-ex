defmodule JSON.LD.NodeIdentifierMap do
  @moduledoc """
  A virtual map over a pool of `ets` tables (one per online scheduler).
  Processes are mapped to a table by `phash2(self(), n_tables)` for low contention and good cache locality.

  Every virtual map is isolated

  Usage:

  """

  use GenServer

  # TODO add option to use the following options for counters:
  # - System.unique_integer([monotonic, positive]) - the most scalable and concurrent, but shared, which might complicate matching bnodes when testing
  # - :atomics - in most cases faster / more concurrent that ets counters
  # - :ets (counters) - the current implementation
  defstruct [:tab, :ref]

  @type t :: %__MODULE__{tab: :ets.tid(), ref: reference()}

  # persistent_term key
  @tables_key {__MODULE__, :tables}

  # ================================================================================================
  # API
  # ================================================================================================

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Creates a Blank Node identifier map re-using a table from the pool.
  """
  @spec new() :: t()

  def new() do
    tab = get_table()
    ref = make_ref()
    :ets.insert(tab, {{ref, :__counter__}, 0})
    struct!(__MODULE__, tab: tab, ref: ref)
  end

  @doc """
  Deletes the blank node identifier map (deleting all its entries from the ets table)
  """
  @spec delete(t()) :: :ok

  def delete(%__MODULE__{tab: tab, ref: ref}) do
    _ = :ets.match_delete(tab, {{ref, :_}, :_})
    :ok
  end

  @doc """
  Generate Blank Node Identifier

  Details at <https://www.w3.org/TR/json-ld-api/#generate-blank-node-identifier>
  """
  @spec generate_blank_node_id(t(), String.t() | nil) :: String.t()

  def generate_blank_node_id(map, identifier \\ nil)

  def generate_blank_node_id(%__MODULE__{tab: tab, ref: ref}, nil) do
    blank_node_id(tab, ref)
  end

  def generate_blank_node_id(%__MODULE__{tab: tab, ref: ref}, identifier) do
    case :ets.lookup(tab, {ref, identifier}) do
      [] ->
        blank_node_id = blank_node_id(tab, ref)
        :ets.insert(tab, {{ref, identifier}, blank_node_id})
        blank_node_id

      [{{^ref, ^identifier}, blank_node_id}] ->
        blank_node_id
    end
  end

  # ================================================================================================
  # GEN_SERVER CALLBACK HANDLERS
  # ================================================================================================

  @impl true
  def init(_) do
    schedulers = :erlang.system_info(:schedulers_online)

    tables =
      for _ <- 1..schedulers do
        :ets.new(:undefined, [
          :public,
          :ordered_set,
          {:keypos, 1},
          read_concurrency: true,
          write_concurrency: true
        ])
      end
      |> List.to_tuple()

    :persistent_term.put(@tables_key, %{tables: tables})

    {:ok, %{tables: tables}}
  end

  @impl true
  def terminate(_, %{tables: tables}) do
    _ = :persistent_term.erase(@tables_key)
    Enum.each(Tuple.to_list(tables), fn tab -> :ets.delete(tab) end)
    :ok
  end

  # ================================================================================================
  # PRIVATE
  # ================================================================================================

  @doc """
  Returns the ETS table identifier for an arbitrary pid (same mapping rule).
  """
  @spec get_table(pid()) :: :ets.tid()

  defp get_table(pid \\ self()) when is_pid(pid) do
    %{tables: tables} = :persistent_term.get(@tables_key)
    n = tuple_size(tables)
    idx = :erlang.phash2(pid, n) + 1
    elem(tables, idx - 1)
  end

  defp init_counter(tab, ref) do
    :ets.insert(tab, {{ref, :__counter__}, 0})
  end

  defp incr_counter(tab, ref) do
    :ets.update_counter(tab, {ref, :__counter__}, 1)
  end

  defp blank_node_id(tab, ref) do
    # Atomically increment and get the counter from ETS
    counter = incr_counter(tab, ref)
    "_:b#{counter - 1}"
  end
end
