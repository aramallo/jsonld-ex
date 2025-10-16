defmodule JSON.LD.NodeIdentifierMap do
  @moduledoc """
  A virtual map over a pool of `ets` tables (one per online scheduler).
  Processes are mapped to a table by `phash2(self(), n_tables)` for low contention and good cache locality.

  Every virtual map is isolated. Each map has a counter that can use one of the following options

  - `:system` -- System.unique_integer([monotonic, positive]) - the most scalable and concurrent, but shared, which might complicate matching bnodes when testing
  - `:atomics` - in most cases faster / more concurrent that ets counters
  - `:ets_counter` - the current implementation

  Usage:

  """

  use GenServer

  defstruct [:id, :tab, :counter_ref]

  @type t :: %__MODULE__{
          id: reference(),
          tab: :ets.tid(),
          counter_ref: :system | :ets_counter | {:atomics, reference()} | nil
        }

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

  ## Counter Option
  Each map has a counter. The type of counter to be used can be configured using the application
  environment option `blank_node_counter_type` or using the `counter_type` option when calling `new/1`.

  The supported counter types are:
  - `:ets_counter` — This is the default. The counter entry will be removed alongside the other map entries on `delete/1`. This is a zero-based counter.
  - `:atomics` — Uses the `atomics` module. In most cases, this is faster and more concurrent than `ets` counters. Atomics are not tied to the current process and are automatically garbage collected when no longer referenced. This is a zero-based counter.
  - `:system` — Uses `System.unique_integer([monotonic, positive])`. This is the most scalable and concurrent option. The only potential downside is that it is not a zero-based counter and it doesn't generate contiguous integers, which might complicate some use cases, particularly testing.
  """
  @spec new() :: t()
  @spec new(Keyword.t()) :: t()

  def new(opts \\ []) do
    counter_type =
      Keyword.get_lazy(opts, :counter_type, fn ->
        Application.get_env(:json_ld, :blank_node_counter_type, :ets_counter)
      end)

    unless counter_type in [:atomics, :ets_counter, :system] do
      raise ArgumentError,
        message:
          "invalid counter_type option, " <>
            "expected one of :atomics, :ets_counter or :system, got #{inspect(counter_type)}."
    end

    id = make_ref()
    tab = get_table()
    counter_ref = init_counter(id, tab, counter_type)
    struct!(__MODULE__, id: id, tab: tab, counter_ref: counter_ref)
  end

  @doc """
  Deletes the blank node identifier map (deleting all its entries from `ets`).
  """
  @spec delete(t()) :: :ok

  def delete(%__MODULE__{id: id, tab: tab}) do
    _ = :ets.match_delete(tab, {{id, :_}, :_})
    :ok
  end

  @doc """
  Generate Blank Node Identifier.
  Details at <https://www.w3.org/TR/json-ld-api/#generate-blank-node-identifier>.
  """
  @spec generate_blank_node_id(map :: t(), String.t() | nil) :: String.t()

  def generate_blank_node_id(map, identifier \\ nil)

  def generate_blank_node_id(%__MODULE__{} = map, nil) do
    blank_node_id(map)
  end

  def generate_blank_node_id(%__MODULE__{id: id, tab: tab} = map, identifier) do
    object = {id, identifier}

    case :ets.lookup(tab, object) do
      [] ->
        blank_node_id = blank_node_id(map)
        true = :ets.insert(tab, {object, blank_node_id})
        blank_node_id

      [{^object, blank_node_id}] ->
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

    # Store tables in persistent_term for efficient access
    :persistent_term.put(@tables_key, %{tables: tables})

    {:ok, %{tables: tables}}
  end

  @impl true
  def terminate(_, %{tables: tables}) do
    # if atomics was used it will be gc'ed when the process that owns this map dies
    _ = :persistent_term.erase(@tables_key)
    Enum.each(Tuple.to_list(tables), fn tab -> :ets.delete(tab) end)
    :ok
  end

  # ================================================================================================
  # PRIVATE
  # ================================================================================================

  # Returns the ETS table identifier for an arbitrary pid (same mapping rule).
  @spec get_table(pid()) :: :ets.tid()

  defp get_table(pid \\ self()) when is_pid(pid) do
    %{tables: tables} = :persistent_term.get(@tables_key)
    n = tuple_size(tables)
    idx = :erlang.phash2(pid, n) + 1
    elem(tables, idx - 1)
  end

  defp init_counter(_, _, :system) do
    :system
  end

  defp init_counter(_, _, :atomics) do
    ref = :atomics.new(1, [{:signed, false}])
    {:atomics, ref}
  end

  defp init_counter(id, tab, :ets_counter) do
    :ets.insert(tab, {{id, :__counter__}, 0})
    :ets_counter
  end

  @spec incr_counter(t()) :: non_neg_integer()

  defp(incr_counter(%__MODULE__{counter_ref: :system})) do
    System.unique_integer([:positive, :monotonic])
  end

  defp incr_counter(%__MODULE__{counter_ref: :ets_counter} = map) do
    :ets.update_counter(map.tab, {map.id, :__counter__}, 1)
  end

  defp incr_counter(%__MODULE__{counter_ref: {:atomics, ref}}) do
    # atomics start at zero
    :atomics.add_get(ref, 1, 1) - 1
  end

  defp blank_node_id(map) do
    counter = incr_counter(map)
    "_:b#{counter - 1}"
  end
end
