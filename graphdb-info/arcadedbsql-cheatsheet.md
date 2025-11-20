# ArcadeDB SQL Cheat Sheet

## CRUD
[`INSERT`](https://docs.arcadedb.com/#SQL-Insert)
`INTO <type>|BUCKET:<bucket>|<index> [(<field>[,]*) VALUES (<expression>[,]*)[,]*]|`  
$~~~~~~~~~~~~~~~~~~~$`[SET <field> = <expression>|<command>[,]*]|[CONTENT {<json>}] [RETURN <expression>] [FROM <query>]`

[`SELECT`](https://docs.arcadedb.com/#SQL-Select)
`[<projections>] [FROM <target> [LET <assignment>*]] [WHERE <condition>*] [GROUP BY <field>*] [ORDER BY <fields>* [ASC|DESC]*]`  
$~~~~~~~~~~~$`[UNWIND <Field>*] [SKIP <number>] [LIMIT <max>] [TIMEOUT <ms> [<strategy>]]`

[`UPDATE`](https://docs.arcadedb.com/#SQL-Update)
`<type>|BUCKET:<bucket>|<RID> [SET|REMOVE <field> = <value>[,]*]|[CONTENT|MERGE <json>] [UPSERT] [RETURN <returning> [<expression>]]`  
$~~~~~~~~~~~$`[WHERE <condition>] [LIMIT <max>] [TIMEOUT <ms>]`

[`DELETE`](https://docs.arcadedb.com/#SQL-Delete)
`FROM <type>|BUCKET:<bucket>|<index> [RETURN <returning>] [WHERE <condition>*] [LIMIT <max>] [TIMEOUT <ms>]`

## Graphs
[`MATCH`](https://docs.arcadedb.com/#SQL-Match)
`<pattern> [,[NOT] <pattern>]* RETURN [DISTINCT] <expression> [AS <alias>] [,<expression> [AS <alias>]]*`  
$~~~~~~~~~~$`GROUP BY <expression>[,]* ORDER BY <expression>[,]* SKIP <number> LIMIT <max>`

[`TRAVERSE`](https://docs.arcadedb.com/#SQL-Traverse)
`[<type.]field>|*|any()|all() [FROM <target>] [MAXDEPTH <number>|WHILE <condition>] [LIMIT <max>] [STRATEGY <strategy>]`

[`CREATE VERTEX`](https://docs.arcadedb.com/#SQL-Create-Vertex)
`[<type>] [BUCKET <bucket>] [SET <field> = <expression>[,]*]`

[`CREATE EDGE`](https://docs.arcadedb.com/#SQL-Create-Edge)
`<type> [BUCKET <bucket>] [UPSERT] FROM <rid>|(<query>)|[<rid>]* TO <rid>|(<query>)|[<rid>]* [IF NOT EXISTS]`  
$~~~~~~~~~~~~~~~~~~~$`[SET <field> = <expression>[,]*]|CONTENT {<json>} [RETRY <retry> [WAIT <ms>]] [BATCH <batch-size>]`

## Types
[`CREATE <DOCUMENT|VERTEX|EDGE> TYPE`](https://docs.arcadedb.com/#SQL-Create-Type)
`<type> [IF NOT EXISTS] [EXTENDS <type>] [BUCKET <bucket>[,]*] [BUCKETS <number>]`

[`ALTER TYPE`](https://docs.arcadedb.com/#SQL-Alter-Type)
`<type> [<attribute> <value>] [CUSTOM <key> <value>]`

[`TRUNCATE TYPE`](https://docs.arcadedb.com/#SQL-Truncate-Type)
`<type> [POLYMORPHIC] [UNSAFE]`

[`DROP TYPE`](https://docs.arcadedb.com/#SQL-Drop-Type)
`<type> [UNSAFE] [IF EXISTS]`

## Buckets
[`CREATE BUCKET`](https://docs.arcadedb.com/#SQL-Create-Bucket)
`<bucket> [ID <bucket-id>]`

[`TRUNCATE BUCKET`](https://docs.arcadedb.com/#SQL-Truncate-Bucket)
`<bucket>`

[`DROP BUCKET`](https://docs.arcadedb.com/#SQL-Drop-Bucket)
`<bucket>|<bucket-id>`

## Properties
[`CREATE PROPERTY`](https://docs.arcadedb.com/#SQL-Create-Property)
`<type>.<property> <data-type> [<constraint>[,]*] [IF NOT EXISTS]`

[`ALTER PROPERTY`](https://docs.arcadedb.com/#SQL-Alter-Property)
`<type>.<property> <attribute> <value> [CUSTOM <custom-key> = <custom-value>]`

[`DROP PROPERTY`](https://docs.arcadedb.com/#SQL-Drop-Property)
`<type>.<property> [FORCE]`

## Indices
[`CREATE INDEX`](https://docs.arcadedb.com/#SQL-Create-Index)
`[<name>] [IF NOT EXISTS] [ON <type> (<property>[,]*)] <UNIQUE|NOTUNIQUE|FULL_TEXT> [<key-type>] [NULL_STRATEGY SKIP|ERROR]`

[`REBUILD INDEX`](https://docs.arcadedb.com/#_sql-rebuild-index)
`<name>`

[`DROP INDEX`](https://docs.arcadedb.com/#SQL-Drop-Index)
`<name> [IF EXISTS]`


## Database
[`ALTER DATABASE`](https://docs.arcadedb.com/#SQL-Alter-Database)
`<setting> <value>`

[`IMPORT DATABASE`](https://docs.arcadedb.com/#SQL-Import-Database)
`<url> [WITH (<setting-name> = <setting-value> [,])*]`

[`EXPORT DATABASE`](https://docs.arcadedb.com/#_sql-export-database)
`<url> [FORMAT JSONL|GRAPHML|GRAPHSON] [OVERWRITE TRUE|FALSE]`

[`BACKUP DATABASE`](https://docs.arcadedb.com/#SQL-Backup-Database)
`[<url>]`

[`ALIGN DATABASE`](https://docs.arcadedb.com/#_sql-align-database)

[`CHECK DATABASE`](https://docs.arcadedb.com/#_sql-check-database)
`[TYPE <type>[,]*] [BUCKET <bucket>[,]*] [FIX]`

## Analysis
[`EXPLAIN`](https://docs.arcadedb.com/#SQL-Explain)
`<command>`

[`PROFILE`](https://docs.arcadedb.com/#SQL-Profile)
`<command>`

## System
[`CONSOLE`](https://docs.arcadedb.com/#SQL-Console)
`.<logLevel> <expression>`

## [ArcadeDB SQL Reference](https://docs.arcadedb.com/#SQL)

<!-- Build: pandoc arcadedb-cheatsheet.md -f markdown -o arcadedb-sql.pdf -V geometry:margin=1.5cm,landscape -V pagestyle=empty -->
