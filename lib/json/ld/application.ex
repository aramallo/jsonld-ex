defmodule JSON.LD.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      JSON.LD.NodeIdentifierMap
    ]

    opts = [strategy: :one_for_one, name: JSON.LD.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
