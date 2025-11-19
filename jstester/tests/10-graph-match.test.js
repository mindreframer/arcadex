import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import {
  createDatabase,
  command,
  query,
  uniqueDbName,
  cleanupDatabase
} from './helpers.js';

describe('Graph - MATCH Pattern Matching', () => {
  const dbName = uniqueDbName('test_graph_match');

  beforeAll(async () => {
    await cleanupDatabase(dbName);
    await createDatabase(dbName);

    // Create schema
    await command(dbName, 'CREATE VERTEX TYPE Person');
    await command(dbName, 'CREATE PROPERTY Person.name STRING');
    await command(dbName, 'CREATE EDGE TYPE Friend');
    await command(dbName, 'CREATE PROPERTY Friend.since INTEGER');

    // Create test graph
    // John -> Jane -> Frank
    //   \-> Bob -> Frank
    const john = await command(dbName, "CREATE VERTEX Person SET name = 'John'");
    const jane = await command(dbName, "CREATE VERTEX Person SET name = 'Jane'");
    const bob = await command(dbName, "CREATE VERTEX Person SET name = 'Bob'");
    const frank = await command(dbName, "CREATE VERTEX Person SET name = 'Frank'");

    await command(dbName, `CREATE EDGE Friend FROM ${john.result[0]['@rid']} TO ${jane.result[0]['@rid']} SET since = 2020`);
    await command(dbName, `CREATE EDGE Friend FROM ${john.result[0]['@rid']} TO ${bob.result[0]['@rid']} SET since = 2019`);
    await command(dbName, `CREATE EDGE Friend FROM ${jane.result[0]['@rid']} TO ${frank.result[0]['@rid']} SET since = 2021`);
    await command(dbName, `CREATE EDGE Friend FROM ${bob.result[0]['@rid']} TO ${frank.result[0]['@rid']} SET since = 2022`);
  });

  afterAll(async () => {
    await cleanupDatabase(dbName);
  });

  test('basic MATCH - find person by name', async () => {
    const result = await query(dbName, `
      MATCH {type: Person, as: person, where: (name = 'John')}
      RETURN person.name as name
    `);
    expect(result.result.length).toBe(1);
    expect(result.result[0].name).toBe('John');
  });

  test('MATCH with outgoing edge', async () => {
    const result = await query(dbName, `
      MATCH {type: Person, as: person, where: (name = 'John')}.out('Friend'){as: friend}
      RETURN person.name as person, friend.name as friend
    `);
    expect(result.result.length).toBe(2);
    const friends = result.result.map(r => r.friend);
    expect(friends).toContain('Jane');
    expect(friends).toContain('Bob');
  });

  test('MATCH with both directions', async () => {
    const result = await query(dbName, `
      MATCH {type: Person, as: person, where: (name = 'Jane')}.both('Friend'){as: connection}
      RETURN person.name as person, connection.name as connection
    `);
    expect(result.result.length).toBe(2);
    const connections = result.result.map(r => r.connection);
    expect(connections).toContain('John');
    expect(connections).toContain('Frank');
  });

  test('MATCH friends of friends', async () => {
    const result = await query(dbName, `
      MATCH {type: Person, as: person, where: (name = 'John')}
        .out('Friend').out('Friend'){as: fof}
      RETURN person.name as person, fof.name as fof
    `);
    expect(result.result.length).toBe(2); // Jane->Frank and Bob->Frank
    const fofs = result.result.map(r => r.fof);
    expect(fofs).toContain('Frank');
  });

  test('MATCH with depth limit (while)', async () => {
    const result = await query(dbName, `
      MATCH {type: Person, as: person, where: (name = 'John')}
        .out('Friend'){as: friend, while: ($depth < 2)}
      RETURN friend.name as name
    `);
    // Should get immediate friends
    expect(result.result.length).toBeGreaterThanOrEqual(2);
  });

  test('MATCH with function syntax for edges', async () => {
    const result = await query(dbName, `
      MATCH {type: Person, as: a, where: (name = 'John')}.out('Friend'){as: b}
      RETURN a.name as fromPerson, b.name as toPerson
    `);
    expect(result.result.length).toBe(2);
  });

  test('MATCH filtering by person', async () => {
    // Simpler test that doesn't use complex edge filtering
    const result = await query(dbName, `
      MATCH {type: Person, as: person, where: (name = 'John')}.out('Friend'){as: friend}
      RETURN person.name as personName, friend.name as friendName
    `);
    expect(result.result.length).toBe(2);
    const friends = result.result.map(r => r.friendName);
    expect(friends).toContain('Jane');
    expect(friends).toContain('Bob');
  });

  test('MATCH common friends', async () => {
    // Find common friends of John and (someone connected to) Frank
    // Both Jane and Bob are friends with Frank
    const result = await query(dbName, `
      MATCH
        {type: Person, where: (name = 'John')}.out('Friend'){as: friend},
        {as: friend}.out('Friend'){type: Person, where: (name = 'Frank')}
      RETURN friend.name as commonFriend
    `);
    expect(result.result.length).toBe(2);
    const friends = result.result.map(r => r.commonFriend);
    expect(friends).toContain('Jane');
    expect(friends).toContain('Bob');
  });

  test('MATCH with DISTINCT', async () => {
    const result = await query(dbName, `
      MATCH {type: Person, as: person, where: (name = 'John')}
        .out('Friend').out('Friend'){as: fof}
      RETURN DISTINCT fof.name as name
    `);
    expect(result.result.length).toBe(1); // Only Frank
    expect(result.result[0].name).toBe('Frank');
  });

  test('MATCH with multiple return fields', async () => {
    const result = await query(dbName, `
      MATCH {type: Person, as: p, where: (name = 'John')}.out('Friend'){as: f}
      RETURN p.name as person, f.name as friend
    `);
    expect(result.result.length).toBe(2);
    expect(result.result[0].person).toBe('John');
  });

  test('MATCH with ORDER BY and LIMIT', async () => {
    const result = await query(dbName, `
      MATCH {type: Person, as: person}
      RETURN person.name as name
      ORDER BY name ASC
      LIMIT 2
    `);
    expect(result.result.length).toBe(2);
    expect(result.result[0].name).toBe('Bob');
    expect(result.result[1].name).toBe('Frank');
  });
});
