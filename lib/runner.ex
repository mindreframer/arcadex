defmodule Runner do
  @moduledoc false

  def sqlscript(db, script) do
    url = "http://localhost:2480/api/v1/command/#{db}"
    payload = %{language: "sqlscript", command: script}

    send_command(url, payload)
  end

  def sql(db, query) do
    url = "http://localhost:2480/api/v1/query/#{db}"
    payload = %{language: "sql", command: query}

    send_command(url, payload)
  end

  def send_command(url, payload) do
    Req.post!(url, json: payload, auth: {:basic, "root:playwithdata"})
  end
end
