import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import {
  createDatabase,
  command,
  query,
  uniqueDbName,
  cleanupDatabase
} from './helpers.js';

describe('CRUD - INSERT', () => {
  const dbName = uniqueDbName('test_crud_insert');

  beforeAll(async () => {
    await cleanupDatabase(dbName);
    await createDatabase(dbName);
    await command(dbName, 'CREATE DOCUMENT TYPE Contact');
    await command(dbName, 'CREATE PROPERTY Contact.name STRING');
    await command(dbName, 'CREATE PROPERTY Contact.email STRING');
    await command(dbName, 'CREATE PROPERTY Contact.age INTEGER');
    await command(dbName, 'CREATE PROPERTY Contact.tags LIST OF STRING');
    await command(dbName, 'CREATE PROPERTY Contact.metadata MAP');
  });

  afterAll(async () => {
    await cleanupDatabase(dbName);
  });

  test('insert with SET syntax', async () => {
    const result = await command(dbName, "INSERT INTO Contact SET name = 'John', email = 'john@example.com'");
    expect(result.result).toBeDefined();
    expect(result.result.length).toBe(1);
  });

  test('insert with VALUES syntax', async () => {
    const result = await command(dbName, "INSERT INTO Contact (name, email) VALUES ('Jane', 'jane@example.com')");
    expect(result.result).toBeDefined();
    expect(result.result.length).toBe(1);
  });

  test('insert multiple records with VALUES', async () => {
    const result = await command(dbName, "INSERT INTO Contact (name, email) VALUES ('Alice', 'alice@example.com'), ('Bob', 'bob@example.com')");
    expect(result.result).toBeDefined();
    expect(result.result.length).toBe(2);
  });

  test('insert with JSON CONTENT', async () => {
    const result = await command(dbName, 'INSERT INTO Contact CONTENT {"name": "Charlie", "email": "charlie@example.com"}');
    expect(result.result).toBeDefined();
    expect(result.result.length).toBe(1);
  });

  test('insert with parameters', async () => {
    const result = await command(dbName, 'INSERT INTO Contact SET name = :name, email = :email', {
      name: 'Dave',
      email: 'dave@example.com'
    });
    expect(result.result).toBeDefined();
    expect(result.result.length).toBe(1);
  });

  test('insert with RETURN @this', async () => {
    const result = await command(dbName, "INSERT INTO Contact SET name = 'Eve', email = 'eve@example.com' RETURN @this");
    expect(result.result).toBeDefined();
    expect(result.result[0].name).toBe('Eve');
    expect(result.result[0].email).toBe('eve@example.com');
  });

  test('insert with RETURN @rid', async () => {
    const result = await command(dbName, "INSERT INTO Contact SET name = 'Frank', email = 'frank@example.com' RETURN @rid");
    expect(result.result).toBeDefined();
    expect(result.result[0]['@rid']).toBeDefined();
  });

  test('insert with list property', async () => {
    const result = await command(dbName, 'INSERT INTO Contact SET name = "Grace", tags = ["vip", "premium"]');
    expect(result.result).toBeDefined();

    const check = await query(dbName, 'SELECT FROM Contact WHERE name = "Grace"');
    expect(check.result[0].tags).toContain('vip');
    expect(check.result[0].tags).toContain('premium');
  });

  test('insert with map property', async () => {
    const result = await command(dbName, 'INSERT INTO Contact SET name = "Henry", metadata = {"source": "web", "campaign": "summer"}');
    expect(result.result).toBeDefined();

    const check = await query(dbName, 'SELECT FROM Contact WHERE name = "Henry"');
    expect(check.result[0].metadata.source).toBe('web');
  });

  test('insert from subquery', async () => {
    await command(dbName, 'CREATE DOCUMENT TYPE ArchivedContact');
    await command(dbName, "INSERT INTO Contact SET name = 'ToArchive', email = 'archive@test.com'");

    const result = await command(dbName, 'INSERT INTO ArchivedContact FROM SELECT * FROM Contact WHERE name = "ToArchive"');
    expect(result.result).toBeDefined();

    const check = await query(dbName, 'SELECT FROM ArchivedContact');
    expect(check.result.length).toBe(1);
    expect(check.result[0].name).toBe('ToArchive');
  });
});
