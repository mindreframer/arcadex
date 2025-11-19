import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import {
  createDatabase,
  command,
  query,
  uniqueDbName,
  cleanupDatabase
} from './helpers.js';

describe('Indexes', () => {
  const dbName = uniqueDbName('test_indexes');

  beforeAll(async () => {
    await cleanupDatabase(dbName);
    await createDatabase(dbName);
    await command(dbName, 'CREATE DOCUMENT TYPE Contact');
    await command(dbName, 'CREATE PROPERTY Contact.email STRING');
    await command(dbName, 'CREATE PROPERTY Contact.firstName STRING');
    await command(dbName, 'CREATE PROPERTY Contact.lastName STRING');
    await command(dbName, 'CREATE PROPERTY Contact.content STRING');
  });

  afterAll(async () => {
    await cleanupDatabase(dbName);
  });

  test('create unique index', async () => {
    const result = await command(dbName, 'CREATE INDEX ON Contact (email) UNIQUE');
    expect(result.result).toBeDefined();
  });

  test('create non-unique index', async () => {
    const result = await command(dbName, 'CREATE INDEX ON Contact (firstName) NOTUNIQUE');
    expect(result.result).toBeDefined();
  });

  test('create composite index', async () => {
    const result = await command(dbName, 'CREATE INDEX ON Contact (lastName, firstName) NOTUNIQUE');
    expect(result.result).toBeDefined();
  });

  test('create full-text index', async () => {
    const result = await command(dbName, 'CREATE INDEX ON Contact (content) FULL_TEXT');
    expect(result.result).toBeDefined();
  });

  test('verify indexes in schema', async () => {
    const result = await query(dbName, 'SELECT FROM schema:indexes');
    expect(result.result.length).toBeGreaterThanOrEqual(4);
  });

  test('unique index enforces uniqueness', async () => {
    await command(dbName, 'INSERT INTO Contact SET email = "test@example.com", firstName = "Test"');

    // Should fail due to duplicate
    const result = await command(dbName, 'INSERT INTO Contact SET email = "test@example.com", firstName = "Test2"');
    expect(result.error).toBeDefined();
  });

  test('drop index', async () => {
    await command(dbName, 'CREATE DOCUMENT TYPE TempType');
    await command(dbName, 'CREATE PROPERTY TempType.field1 STRING');
    await command(dbName, 'CREATE INDEX ON TempType (field1) UNIQUE');

    const result = await command(dbName, 'DROP INDEX `TempType[field1]`');
    expect(result.result).toBeDefined();
  });
});
