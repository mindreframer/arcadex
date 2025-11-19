import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import {
  createDatabase,
  dropDatabase,
  databaseExists,
  serverCommand,
  uniqueDbName,
  cleanupDatabase
} from './helpers.js';

describe('Database Management', () => {
  const dbName = uniqueDbName('test_db_mgmt');

  afterAll(async () => {
    await cleanupDatabase(dbName);
  });

  test('create database', async () => {
    const result = await createDatabase(dbName);
    expect(result.result).toBe('ok');
  });

  test('database exists returns true for existing db', async () => {
    const exists = await databaseExists(dbName);
    expect(exists).toBe(true);
  });

  test('database exists returns false for non-existing db', async () => {
    const exists = await databaseExists('nonexistent_db_12345');
    expect(exists).toBe(false);
  });

  test('list databases includes created db', async () => {
    const result = await serverCommand('list databases');
    expect(result.result).toContain(dbName);
  });

  test('drop database', async () => {
    const result = await dropDatabase(dbName);
    expect(result.result).toBe('ok');

    const exists = await databaseExists(dbName);
    expect(exists).toBe(false);
  });
});
