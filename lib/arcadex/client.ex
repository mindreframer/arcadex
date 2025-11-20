defmodule Arcadex.Client do
  @moduledoc """
  HTTP client layer for ArcadeDB API.

  Uses Req for HTTP requests with Finch connection pooling.
  Handles authentication headers, session management, and response parsing.
  """

  alias Arcadex.{Conn, Error}

  @doc """
  POST request to ArcadeDB API.

  Sends JSON body with auth header and optional session header.
  Parses response and returns `{:ok, body}` or `{:error, %Arcadex.Error{}}`.

  ## Examples

      iex> Arcadex.Client.post(conn, "/api/v1/query/mydb", %{language: "sql", command: "SELECT 1"})
      {:ok, %{"result" => [%{"1" => 1}]}}

  """
  @spec post(Conn.t(), String.t(), map()) :: {:ok, map()} | {:error, Error.t()}
  def post(%Conn{} = conn, path, body) do
    url = "#{conn.base_url}#{path}"
    headers = build_headers(conn)

    case Req.post(url, json: body, headers: headers, finch: conn.finch_name, retry: false) do
      {:ok, %{status: 200, body: response_body}} ->
        {:ok, response_body}

      {:ok, %{status: status, body: %{"error" => error, "detail" => detail}}} ->
        {:error, %Error{status: status, message: error, detail: detail}}

      {:ok, %{status: status, body: %{"error" => error}}} ->
        {:error, %Error{status: status, message: error, detail: nil}}

      {:ok, %{status: status, body: response_body}} ->
        {:error,
         %Error{status: status, message: "HTTP #{status}", detail: inspect(response_body)}}

      {:error, %Req.TransportError{reason: reason}} ->
        {:error, %Error{message: "Connection failed", detail: inspect(reason)}}

      {:error, reason} ->
        {:error, %Error{message: "Request failed", detail: inspect(reason)}}
    end
  end

  @doc """
  GET request to ArcadeDB API.

  Sends request with auth header. Used for endpoints like database exists check.
  Returns `{:ok, body}` or `{:error, %Arcadex.Error{}}`.

  ## Examples

      iex> Arcadex.Client.get(conn, "/api/v1/exists/mydb")
      {:ok, %{"result" => true}}

  """
  @spec get(Conn.t(), String.t()) :: {:ok, map()} | {:error, Error.t()}
  def get(%Conn{} = conn, path) do
    url = "#{conn.base_url}#{path}"
    headers = [{"authorization", basic_auth(conn.auth)}]

    case Req.get(url, headers: headers, finch: conn.finch_name, retry: false) do
      {:ok, %{status: 200, body: response_body}} ->
        {:ok, response_body}

      {:ok, %{status: status, body: %{"error" => error, "detail" => detail}}} ->
        {:error, %Error{status: status, message: error, detail: detail}}

      {:ok, %{status: status, body: %{"error" => error}}} ->
        {:error, %Error{status: status, message: error, detail: nil}}

      {:ok, %{status: status, body: response_body}} ->
        {:error,
         %Error{status: status, message: "HTTP #{status}", detail: inspect(response_body)}}

      {:error, %Req.TransportError{reason: reason}} ->
        {:error, %Error{message: "Connection failed", detail: inspect(reason)}}

      {:error, reason} ->
        {:error, %Error{message: "Request failed", detail: inspect(reason)}}
    end
  end

  @doc false
  @spec build_headers(Conn.t()) :: [{String.t(), String.t()}]
  defp build_headers(%Conn{} = conn) do
    headers = [{"authorization", basic_auth(conn.auth)}]

    if conn.session_id do
      [{"arcadedb-session-id", conn.session_id} | headers]
    else
      headers
    end
  end

  @doc false
  @spec basic_auth({String.t(), String.t()}) :: String.t()
  defp basic_auth({user, pass}) do
    "Basic " <> Base.encode64("#{user}:#{pass}")
  end
end
