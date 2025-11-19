import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import {
  createDatabase,
  command,
  query,
  uniqueDbName,
  cleanupDatabase
} from './helpers.js';

describe('Graph - Vertices and Edges', () => {
  const dbName = uniqueDbName('test_graph_basic');

  beforeAll(async () => {
    await cleanupDatabase(dbName);
    await createDatabase(dbName);
    await command(dbName, 'CREATE VERTEX TYPE Person');
    await command(dbName, 'CREATE PROPERTY Person.name STRING');
    await command(dbName, 'CREATE EDGE TYPE Knows');
    await command(dbName, 'CREATE PROPERTY Knows.since INTEGER');
    await command(dbName, 'CREATE EDGE TYPE Manages');
  });

  afterAll(async () => {
    await cleanupDatabase(dbName);
  });

  test('create vertex with SET', async () => {
    const result = await command(dbName, "CREATE VERTEX Person SET name = 'John'");
    expect(result.result).toBeDefined();
    expect(result.result.length).toBe(1);
  });

  test('create vertex with CONTENT', async () => {
    const result = await command(dbName, 'CREATE VERTEX Person CONTENT {"name": "Jane"}');
    expect(result.result).toBeDefined();
    expect(result.result[0].name).toBe('Jane');
  });

  test('create edge between vertices', async () => {
    const john = await command(dbName, "CREATE VERTEX Person SET name = 'EdgeJohn'");
    const jane = await command(dbName, "CREATE VERTEX Person SET name = 'EdgeJane'");

    const johnRid = john.result[0]['@rid'];
    const janeRid = jane.result[0]['@rid'];

    const result = await command(dbName, `CREATE EDGE Knows FROM ${johnRid} TO ${janeRid}`);
    expect(result.result).toBeDefined();
    expect(result.result.length).toBe(1);
  });

  test('create edge with properties', async () => {
    const alice = await command(dbName, "CREATE VERTEX Person SET name = 'PropAlice'");
    const bob = await command(dbName, "CREATE VERTEX Person SET name = 'PropBob'");

    const result = await command(dbName, `CREATE EDGE Knows FROM ${alice.result[0]['@rid']} TO ${bob.result[0]['@rid']} SET since = 2020`);
    expect(result.result).toBeDefined();
    expect(result.result[0].since).toBe(2020);
  });

  test('create edge from query', async () => {
    await command(dbName, "CREATE VERTEX Person SET name = 'QueryFrom'");
    await command(dbName, "CREATE VERTEX Person SET name = 'QueryTo'");

    const result = await command(dbName, `
      CREATE EDGE Knows
      FROM (SELECT FROM Person WHERE name = 'QueryFrom')
      TO (SELECT FROM Person WHERE name = 'QueryTo')
    `);
    expect(result.result).toBeDefined();
  });

  test('create multiple edges from one to many', async () => {
    const boss = await command(dbName, "CREATE VERTEX Person SET name = 'TheBoss'");
    const emp1 = await command(dbName, "CREATE VERTEX Person SET name = 'Emp1'");
    const emp2 = await command(dbName, "CREATE VERTEX Person SET name = 'Emp2'");

    const result = await command(dbName, `
      CREATE EDGE Manages FROM ${boss.result[0]['@rid']} TO [${emp1.result[0]['@rid']}, ${emp2.result[0]['@rid']}]
    `);
    expect(result.result).toBeDefined();
    expect(result.result.length).toBe(2);
  });

  test('traverse out() from vertex', async () => {
    // Create a simple graph
    const a = await command(dbName, "CREATE VERTEX Person SET name = 'OutA'");
    const b = await command(dbName, "CREATE VERTEX Person SET name = 'OutB'");
    await command(dbName, `CREATE EDGE Knows FROM ${a.result[0]['@rid']} TO ${b.result[0]['@rid']}`);

    const result = await query(dbName, "SELECT out('Knows').name as friends FROM Person WHERE name = 'OutA'");
    console.log(result);
    expect(result.result[0].friends).toContain('OutB');
  });

  test('traverse in() to vertex', async () => {
    const a = await command(dbName, "CREATE VERTEX Person SET name = 'InA'");
    const b = await command(dbName, "CREATE VERTEX Person SET name = 'InB'");
    await command(dbName, `CREATE EDGE Knows FROM ${a.result[0]['@rid']} TO ${b.result[0]['@rid']}`);

    const result = await query(dbName, "SELECT in('Knows').name as knownBy FROM Person WHERE name = 'InB'");
    expect(result.result[0].knownBy).toContain('InA');
  });

  test('traverse both() directions', async () => {
    const a = await command(dbName, "CREATE VERTEX Person SET name = 'BothA'");
    const b = await command(dbName, "CREATE VERTEX Person SET name = 'BothB'");
    const c = await command(dbName, "CREATE VERTEX Person SET name = 'BothC'");

    await command(dbName, `CREATE EDGE Knows FROM ${a.result[0]['@rid']} TO ${b.result[0]['@rid']}`);
    await command(dbName, `CREATE EDGE Knows FROM ${c.result[0]['@rid']} TO ${b.result[0]['@rid']}`);

    const result = await query(dbName, "SELECT both('Knows').name as connections FROM Person WHERE name = 'BothB'");
    expect(result.result[0].connections).toContain('BothA');
    expect(result.result[0].connections).toContain('BothC');
  });

  test('get outgoing edges with outE()', async () => {
    const a = await command(dbName, "CREATE VERTEX Person SET name = 'EdgeA'");
    const b = await command(dbName, "CREATE VERTEX Person SET name = 'EdgeB'");
    await command(dbName, `CREATE EDGE Knows FROM ${a.result[0]['@rid']} TO ${b.result[0]['@rid']} SET since = 2021`);

    const result = await query(dbName, "SELECT outE('Knows').since as years FROM Person WHERE name = 'EdgeA'");
    expect(result.result[0].years).toContain(2021);
  });

  test('multi-hop traversal', async () => {
    const a = await command(dbName, "CREATE VERTEX Person SET name = 'HopA'");
    const b = await command(dbName, "CREATE VERTEX Person SET name = 'HopB'");
    const c = await command(dbName, "CREATE VERTEX Person SET name = 'HopC'");

    await command(dbName, `CREATE EDGE Knows FROM ${a.result[0]['@rid']} TO ${b.result[0]['@rid']}`);
    await command(dbName, `CREATE EDGE Knows FROM ${b.result[0]['@rid']} TO ${c.result[0]['@rid']}`);

    const result = await query(dbName, "SELECT out('Knows').out('Knows').name as friendsOfFriends FROM Person WHERE name = 'HopA'");
    expect(result.result[0].friendsOfFriends).toContain('HopC');
  });
});
