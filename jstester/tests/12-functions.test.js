import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import {
  createDatabase,
  command,
  query,
  uniqueDbName,
  cleanupDatabase
} from './helpers.js';

describe('SQL Functions', () => {
  const dbName = uniqueDbName('test_functions');

  beforeAll(async () => {
    await cleanupDatabase(dbName);
    await createDatabase(dbName);
    await command(dbName, 'CREATE DOCUMENT TYPE Item');
    await command(dbName, 'CREATE PROPERTY Item.name STRING');
    await command(dbName, 'CREATE PROPERTY Item.value INTEGER');
    await command(dbName, 'CREATE PROPERTY Item.tags LIST OF STRING');

    // Insert test data
    await command(dbName, "INSERT INTO Item SET name = 'A', value = 10, tags = ['one', 'two']");
    await command(dbName, "INSERT INTO Item SET name = 'B', value = 20, tags = ['two', 'three']");
    await command(dbName, "INSERT INTO Item SET name = 'C', value = 30, tags = ['one', 'three']");
    await command(dbName, "INSERT INTO Item SET name = 'D', value = 40");
  });

  afterAll(async () => {
    await cleanupDatabase(dbName);
  });

  // Aggregation functions
  test('count()', async () => {
    const result = await query(dbName, 'SELECT count(*) as cnt FROM Item');
    expect(result.result[0].cnt).toBe(4);
  });

  test('sum()', async () => {
    const result = await query(dbName, 'SELECT sum(value) as total FROM Item');
    expect(result.result[0].total).toBe(100);
  });

  test('avg()', async () => {
    const result = await query(dbName, 'SELECT avg(value) as average FROM Item');
    expect(result.result[0].average).toBe(25);
  });

  test('min()', async () => {
    const result = await query(dbName, 'SELECT min(value) as minimum FROM Item');
    expect(result.result[0].minimum).toBe(10);
  });

  test('max()', async () => {
    const result = await query(dbName, 'SELECT max(value) as maximum FROM Item');
    expect(result.result[0].maximum).toBe(40);
  });

  // Date functions
  test('sysdate()', async () => {
    const result = await query(dbName, 'SELECT sysdate() as now');
    expect(result.result[0].now).toBeDefined();
  });

  test('date() parsing', async () => {
    const result = await query(dbName, "SELECT date('2024-01-15', 'yyyy-MM-dd') as d");
    expect(result.result[0].d).toBeDefined();
  });

  // String/utility functions
  test('uuid()', async () => {
    const result = await query(dbName, 'SELECT uuid() as id');
    expect(result.result[0].id).toBeDefined();
    expect(result.result[0].id.length).toBeGreaterThan(30);
  });

  test('coalesce()', async () => {
    await command(dbName, 'INSERT INTO Item SET name = "NullTest", value = null');
    const result = await query(dbName, 'SELECT coalesce(value, 0) as val FROM Item WHERE name = "NullTest"');
    expect(result.result[0].val).toBe(0);
  });

  test('comparison expression', async () => {
    // Test basic conditional logic with comparison
    const result = await query(dbName, 'SELECT name, value FROM Item WHERE value > 25 ORDER BY name');
    expect(result.result.length).toBe(2); // C: 30, D: 40
    expect(result.result[0].name).toBe('C');
  });

  test('concat()', async () => {
    const result = await query(dbName, 'SELECT concat(name) as names FROM Item');
    expect(result.result[0].names).toContain('A');
  });

  // Collection functions
  test('first()', async () => {
    const result = await query(dbName, 'SELECT first(tags) as firstTag FROM Item WHERE name = "A"');
    expect(result.result[0].firstTag).toBe('one');
  });

  test('last()', async () => {
    const result = await query(dbName, 'SELECT last(tags) as lastTag FROM Item WHERE name = "A"');
    expect(result.result[0].lastTag).toBe('two');
  });

  test('list()', async () => {
    const result = await query(dbName, 'SELECT list(name) as names FROM Item');
    expect(result.result[0].names).toContain('A');
    expect(result.result[0].names).toContain('B');
  });

  test('set()', async () => {
    const result = await query(dbName, 'SELECT set(name) as names FROM Item');
    expect(result.result[0].names.length).toBe(5); // Including NullTest
  });

  // Math functions
  test('abs()', async () => {
    const result = await query(dbName, 'SELECT abs(-10) as val');
    expect(result.result[0].val).toBe(10);
  });

  test('sqrt()', async () => {
    const result = await query(dbName, 'SELECT sqrt(16.0) as val');
    expect(result.result[0].val).toBe(4);
  });

  test('pow()', async () => {
    const result = await query(dbName, 'SELECT pow(2, 3) as val');
    expect(result.result[0].val).toBe(8);
  });

  // Format function - skip as it has parsing issues in ArcadeDB
  // The format() function exists but has strict syntax requirements

  // expand() for unwinding
  test('expand() with list', async () => {
    const result = await query(dbName, 'SELECT expand(tags) FROM Item WHERE name = "A"');
    expect(result.result.length).toBe(2);
  });

  // map()
  test('map()', async () => {
    const result = await query(dbName, 'SELECT map("key1", "value1", "key2", "value2") as m');
    expect(result.result[0].m.key1).toBe('value1');
    expect(result.result[0].m.key2).toBe('value2');
  });
});
