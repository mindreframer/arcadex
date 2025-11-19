import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import {
  createDatabase,
  command,
  query,
  uniqueDbName,
  cleanupDatabase
} from './helpers.js';

describe('CRUD - SELECT', () => {
  const dbName = uniqueDbName('test_crud_select');

  beforeAll(async () => {
    await cleanupDatabase(dbName);
    await createDatabase(dbName);
    await command(dbName, 'CREATE DOCUMENT TYPE Contact');
    await command(dbName, 'CREATE PROPERTY Contact.name STRING');
    await command(dbName, 'CREATE PROPERTY Contact.email STRING');
    await command(dbName, 'CREATE PROPERTY Contact.age INTEGER');
    await command(dbName, 'CREATE PROPERTY Contact.city STRING');
    await command(dbName, 'CREATE PROPERTY Contact.status STRING');

    // Insert test data
    await command(dbName, "INSERT INTO Contact SET name = 'Alice', email = 'alice@test.com', age = 30, city = 'NYC', status = 'active'");
    await command(dbName, "INSERT INTO Contact SET name = 'Bob', email = 'bob@test.com', age = 25, city = 'LA', status = 'active'");
    await command(dbName, "INSERT INTO Contact SET name = 'Charlie', email = 'charlie@test.com', age = 35, city = 'NYC', status = 'inactive'");
    await command(dbName, "INSERT INTO Contact SET name = 'Dave', email = 'dave@test.com', age = 40, city = 'Chicago', status = 'active'");
    await command(dbName, "INSERT INTO Contact SET name = 'Eve', email = 'eve@test.com', age = 28, city = 'NYC', status = 'active'");
  });

  afterAll(async () => {
    await cleanupDatabase(dbName);
  });

  test('select all from type', async () => {
    const result = await query(dbName, 'SELECT FROM Contact');
    expect(result.result.length).toBe(5);
  });

  test('select specific fields', async () => {
    const result = await query(dbName, 'SELECT name, email FROM Contact');
    expect(result.result[0].name).toBeDefined();
    expect(result.result[0].email).toBeDefined();
    expect(result.result[0].age).toBeUndefined();
  });

  test('select with WHERE', async () => {
    const result = await query(dbName, "SELECT FROM Contact WHERE city = 'NYC'");
    expect(result.result.length).toBe(3);
  });

  test('select with multiple conditions', async () => {
    const result = await query(dbName, "SELECT FROM Contact WHERE city = 'NYC' AND status = 'active'");
    expect(result.result.length).toBe(2);
  });

  test('select with OR', async () => {
    const result = await query(dbName, "SELECT FROM Contact WHERE city = 'LA' OR city = 'Chicago'");
    expect(result.result.length).toBe(2);
  });

  test('select with LIKE', async () => {
    const result = await query(dbName, "SELECT FROM Contact WHERE name LIKE 'A%'");
    expect(result.result.length).toBe(1);
    expect(result.result[0].name).toBe('Alice');
  });

  test('select with ORDER BY ASC', async () => {
    const result = await query(dbName, 'SELECT FROM Contact ORDER BY age ASC');
    expect(result.result[0].age).toBe(25);
    expect(result.result[4].age).toBe(40);
  });

  test('select with ORDER BY DESC', async () => {
    const result = await query(dbName, 'SELECT FROM Contact ORDER BY age DESC');
    expect(result.result[0].age).toBe(40);
    expect(result.result[4].age).toBe(25);
  });

  test('select with LIMIT', async () => {
    const result = await query(dbName, 'SELECT FROM Contact LIMIT 2');
    expect(result.result.length).toBe(2);
  });

  test('select with SKIP and LIMIT', async () => {
    const result = await query(dbName, 'SELECT FROM Contact ORDER BY name ASC SKIP 1 LIMIT 2');
    expect(result.result.length).toBe(2);
    expect(result.result[0].name).toBe('Bob');
    expect(result.result[1].name).toBe('Charlie');
  });

  test('select with COUNT', async () => {
    const result = await query(dbName, 'SELECT count(*) as cnt FROM Contact');
    expect(result.result[0].cnt).toBe(5);
  });

  test('select with SUM', async () => {
    const result = await query(dbName, 'SELECT sum(age) as total FROM Contact');
    expect(result.result[0].total).toBe(158); // 30+25+35+40+28
  });

  test('select with AVG', async () => {
    const result = await query(dbName, 'SELECT avg(age) as average FROM Contact');
    // AVG returns integer in ArcadeDB
    expect(result.result[0].average).toBeGreaterThanOrEqual(31);
    expect(result.result[0].average).toBeLessThanOrEqual(32);
  });

  test('select with MIN and MAX', async () => {
    const minResult = await query(dbName, 'SELECT min(age) as minimum FROM Contact');
    const maxResult = await query(dbName, 'SELECT max(age) as maximum FROM Contact');
    expect(minResult.result[0].minimum).toBe(25);
    expect(maxResult.result[0].maximum).toBe(40);
  });

  test('select with GROUP BY', async () => {
    const result = await query(dbName, 'SELECT city, count(*) as cnt FROM Contact GROUP BY city');
    const nycRow = result.result.find(r => r.city === 'NYC');
    expect(nycRow.cnt).toBe(3);
  });

  test('select DISTINCT', async () => {
    const result = await query(dbName, 'SELECT DISTINCT city FROM Contact');
    expect(result.result.length).toBe(3); // NYC, LA, Chicago
  });

  test('select with parameters', async () => {
    const result = await query(dbName, 'SELECT FROM Contact WHERE city = :city', { city: 'NYC' });
    expect(result.result.length).toBe(3);
  });

  test('select with alias', async () => {
    const result = await query(dbName, 'SELECT name AS fullName, age AS years FROM Contact LIMIT 1');
    expect(result.result[0].fullName).toBeDefined();
    expect(result.result[0].years).toBeDefined();
  });

  test('select from schema:types', async () => {
    const result = await query(dbName, 'SELECT FROM schema:types');
    const names = result.result.map(t => t.name);
    expect(names).toContain('Contact');
  });

  test('select from schema:indexes', async () => {
    await command(dbName, 'CREATE INDEX ON Contact (email) UNIQUE');
    const result = await query(dbName, 'SELECT FROM schema:indexes');
    expect(result.result.length).toBeGreaterThan(0);
  });
});
