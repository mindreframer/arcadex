defmodule Arcadex.Error do
  @moduledoc """
  Error struct for ArcadeDB errors.

  Implements the Exception behaviour so it can be raised.
  """

  defexception [:status, :message, :detail]

  @type t :: %__MODULE__{
          status: integer() | nil,
          message: String.t() | nil,
          detail: String.t() | nil
        }

  @impl true
  @spec message(t()) :: String.t()
  def message(%__MODULE__{message: nil, detail: nil}), do: "Unknown error"
  def message(%__MODULE__{message: message, detail: nil}), do: message || "Unknown error"
  def message(%__MODULE__{message: nil, detail: detail}), do: detail || "Unknown error"
  def message(%__MODULE__{message: message, detail: detail}), do: "#{message}: #{detail}"
end
