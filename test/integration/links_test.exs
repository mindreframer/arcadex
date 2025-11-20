defmodule Arcadex.Integration.LinksTest do
  @moduledoc """
  Integration tests for LINK property operations against ArcadeDB.
  """
  use Arcadex.IntegrationCase, async: true

  describe "LINK properties" do
    @tag :fresh_db
    test "create LINK property", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Author")
      Arcadex.command!(conn, "CREATE PROPERTY Author.name STRING")

      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Book")
      Arcadex.command!(conn, "CREATE PROPERTY Book.title STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Book.author LINK OF Author")

      # Verify property exists
      [type] =
        Arcadex.query!(
          conn,
          "SELECT FROM schema:types WHERE name = 'Book'"
        )

      properties = type["properties"]
      author_prop = Enum.find(properties, &(&1["name"] == "author"))

      assert author_prop
      assert author_prop["type"] == "LINK"
    end

    @tag :fresh_db
    test "insert with LINK value", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Author")
      Arcadex.command!(conn, "CREATE PROPERTY Author.name STRING")

      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Book")
      Arcadex.command!(conn, "CREATE PROPERTY Book.title STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Book.author LINK OF Author")

      # Create author
      [author] =
        Arcadex.command!(
          conn,
          "INSERT INTO Author SET name = 'George Orwell'"
        )

      author_rid = author["@rid"]

      # Create book with link to author
      [book] =
        Arcadex.command!(
          conn,
          "INSERT INTO Book SET title = '1984', author = #{author_rid}"
        )

      assert book["title"] == "1984"
      assert book["author"] == author_rid
    end

    @tag :fresh_db
    test "expand LINK in projection", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Author")
      Arcadex.command!(conn, "CREATE PROPERTY Author.name STRING")

      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Book")
      Arcadex.command!(conn, "CREATE PROPERTY Book.title STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Book.author LINK OF Author")

      # Create author
      [author] =
        Arcadex.command!(
          conn,
          "INSERT INTO Author SET name = 'George Orwell'"
        )

      author_rid = author["@rid"]

      # Create book
      Arcadex.command!(
        conn,
        "INSERT INTO Book SET title = '1984', author = #{author_rid}"
      )

      # Query with expanded link
      [result] =
        Arcadex.query!(
          conn,
          "SELECT title, author.name as author_name FROM Book"
        )

      assert result["title"] == "1984"
      assert result["author_name"] == "George Orwell"
    end

    @tag :fresh_db
    test "navigate through multiple LINKs", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Country")
      Arcadex.command!(conn, "CREATE PROPERTY Country.name STRING")

      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Author")
      Arcadex.command!(conn, "CREATE PROPERTY Author.name STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Author.country LINK OF Country")

      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Book")
      Arcadex.command!(conn, "CREATE PROPERTY Book.title STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Book.author LINK OF Author")

      # Create country
      [country] =
        Arcadex.command!(
          conn,
          "INSERT INTO Country SET name = 'United Kingdom'"
        )

      country_rid = country["@rid"]

      # Create author with country
      [author] =
        Arcadex.command!(
          conn,
          "INSERT INTO Author SET name = 'George Orwell', country = #{country_rid}"
        )

      author_rid = author["@rid"]

      # Create book
      Arcadex.command!(
        conn,
        "INSERT INTO Book SET title = '1984', author = #{author_rid}"
      )

      # Navigate through multiple links
      [result] =
        Arcadex.query!(
          conn,
          "SELECT title, author.name as author_name, author.country.name as country_name FROM Book"
        )

      assert result["title"] == "1984"
      assert result["author_name"] == "George Orwell"
      assert result["country_name"] == "United Kingdom"
    end
  end

  describe "LINK list embedded" do
    @tag :fresh_db
    test "create embedded list with links", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Tag")
      Arcadex.command!(conn, "CREATE PROPERTY Tag.name STRING")

      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Article")
      Arcadex.command!(conn, "CREATE PROPERTY Article.title STRING")
      # Use LIST without type constraint - will store RIDs
      Arcadex.command!(conn, "CREATE PROPERTY Article.tags LIST")

      # Verify property exists
      [type] =
        Arcadex.query!(
          conn,
          "SELECT FROM schema:types WHERE name = 'Article'"
        )

      properties = type["properties"]
      tags_prop = Enum.find(properties, &(&1["name"] == "tags"))

      assert tags_prop
      assert tags_prop["type"] == "LIST"
    end

    @tag :fresh_db
    test "insert with list of links", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Tag")
      Arcadex.command!(conn, "CREATE PROPERTY Tag.name STRING")

      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Article")
      Arcadex.command!(conn, "CREATE PROPERTY Article.title STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Article.tags LIST")

      # Create tags
      [tag1] = Arcadex.command!(conn, "INSERT INTO Tag SET name = 'elixir'")
      [tag2] = Arcadex.command!(conn, "INSERT INTO Tag SET name = 'database'")
      [tag3] = Arcadex.command!(conn, "INSERT INTO Tag SET name = 'arcadedb'")

      tag1_rid = tag1["@rid"]
      tag2_rid = tag2["@rid"]
      tag3_rid = tag3["@rid"]

      # Create article with tags
      [article] =
        Arcadex.command!(
          conn,
          "INSERT INTO Article SET title = 'Getting Started', tags = [#{tag1_rid}, #{tag2_rid}, #{tag3_rid}]"
        )

      assert article["title"] == "Getting Started"
      assert length(article["tags"]) == 3
    end

    @tag :fresh_db
    test "expand list of links in projection", %{conn: conn} do
      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Tag")
      Arcadex.command!(conn, "CREATE PROPERTY Tag.name STRING")

      Arcadex.command!(conn, "CREATE DOCUMENT TYPE Article")
      Arcadex.command!(conn, "CREATE PROPERTY Article.title STRING")
      Arcadex.command!(conn, "CREATE PROPERTY Article.tags LIST")

      # Create tags
      [tag1] = Arcadex.command!(conn, "INSERT INTO Tag SET name = 'elixir'")
      [tag2] = Arcadex.command!(conn, "INSERT INTO Tag SET name = 'database'")

      tag1_rid = tag1["@rid"]
      tag2_rid = tag2["@rid"]

      # Create article
      Arcadex.command!(
        conn,
        "INSERT INTO Article SET title = 'Tutorial', tags = [#{tag1_rid}, #{tag2_rid}]"
      )

      # Query with expanded list of links
      [result] =
        Arcadex.query!(
          conn,
          "SELECT title, tags.name as tag_names FROM Article"
        )

      assert result["title"] == "Tutorial"
      assert is_list(result["tag_names"])
      assert "elixir" in result["tag_names"]
      assert "database" in result["tag_names"]
    end
  end
end
