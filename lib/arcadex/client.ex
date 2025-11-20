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

    conn
    |> do_post(url, body, headers)
    |> handle_response()
  end

  defp do_post(conn, url, body, headers) do
    Req.post(url, json: body, headers: headers, finch: conn.finch_name, retry: false)
  end

  defp handle_response({:ok, %{status: 200, body: response_body, headers: resp_headers}}) do
    case get_header(resp_headers, "arcadedb-session-id") do
      nil -> {:ok, response_body}
      session_id when response_body == %{} -> {:ok, %{"result" => session_id}}
      _session_id -> {:ok, response_body}
    end
  end

  defp handle_response({:ok, %{status: 204, headers: resp_headers}}) do
    case get_header(resp_headers, "arcadedb-session-id") do
      nil -> {:ok, %{}}
      session_id -> {:ok, %{"result" => session_id}}
    end
  end

  defp handle_response({:ok, %{status: status, body: %{"error" => error, "detail" => detail}}}) do
    {:error, %Error{status: status, message: error, detail: detail}}
  end

  defp handle_response({:ok, %{status: status, body: %{"error" => error}}}) do
    {:error, %Error{status: status, message: error, detail: nil}}
  end

  defp handle_response({:ok, %{status: status, body: response_body}}) do
    {:error, %Error{status: status, message: "HTTP #{status}", detail: inspect(response_body)}}
  end

  defp handle_response({:error, %Req.TransportError{reason: reason}}) do
    {:error, %Error{message: "Connection failed", detail: inspect(reason)}}
  end

  defp handle_response({:error, reason}) do
    {:error, %Error{message: "Request failed", detail: inspect(reason)}}
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

  @doc false
  @spec get_header(map() | list(), String.t()) :: String.t() | nil
  defp get_header(headers, name) when is_map(headers) do
    # Req returns headers as a map with list values
    name_downcase = String.downcase(name)

    Enum.find_value(headers, fn {key, values} ->
      if String.downcase(key) == name_downcase do
        List.first(values)
      end
    end)
  end

  defp get_header(headers, name) when is_list(headers) do
    # Handle list of tuples format
    name_downcase = String.downcase(name)

    Enum.find_value(headers, fn {key, value} ->
      if String.downcase(key) == name_downcase, do: value
    end)
  end
end
