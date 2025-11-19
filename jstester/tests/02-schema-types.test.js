import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import {
  createDatabase,
  dropDatabase,
  command,
  query,
  uniqueDbName,
  cleanupDatabase
} from './helpers.js';

describe('Schema Types', () => {
  const dbName = uniqueDbName('test_schema_types');

  beforeAll(async () => {
    await cleanupDatabase(dbName);
    await createDatabase(dbName);
  });

  afterAll(async () => {
    await cleanupDatabase(dbName);
  });

  test('create document type', async () => {
    const result = await command(dbName, 'CREATE DOCUMENT TYPE Contact');
    expect(result.result).toBeDefined();
  });

  test('create vertex type', async () => {
    const result = await command(dbName, 'CREATE VERTEX TYPE Person');
    expect(result.result).toBeDefined();
  });

  test('create edge type', async () => {
    const result = await command(dbName, 'CREATE EDGE TYPE Knows');
    expect(result.result).toBeDefined();
  });

  test('create type with IF NOT EXISTS', async () => {
    // Should not error even if type exists
    const result = await command(dbName, 'CREATE VERTEX TYPE Person IF NOT EXISTS');
    expect(result.result).toBeDefined();
  });

  test('create type with inheritance', async () => {
    const result = await command(dbName, 'CREATE VERTEX TYPE Customer EXTENDS Person');
    expect(result.result).toBeDefined();
  });

  test('query schema:types shows created types', async () => {
    const result = await query(dbName, 'SELECT FROM schema:types');
    const typeNames = result.result.map(t => t.name);
    expect(typeNames).toContain('Contact');
    expect(typeNames).toContain('Person');
    expect(typeNames).toContain('Customer');
    expect(typeNames).toContain('Knows');
  });

  test('alter type - rename', async () => {
    await command(dbName, 'CREATE DOCUMENT TYPE OldName');
    const result = await command(dbName, 'ALTER TYPE OldName NAME NewName');
    expect(result.result).toBeDefined();

    const types = await query(dbName, 'SELECT FROM schema:types WHERE name = "NewName"');
    expect(types.result.length).toBe(1);
  });

  test('drop type', async () => {
    await command(dbName, 'CREATE DOCUMENT TYPE ToBeDropped');
    const result = await command(dbName, 'DROP TYPE ToBeDropped');
    expect(result.result).toBeDefined();

    const types = await query(dbName, 'SELECT FROM schema:types WHERE name = "ToBeDropped"');
    expect(types.result.length).toBe(0);
  });

  test('drop type IF EXISTS', async () => {
    // Should not error even if type doesn't exist
    const result = await command(dbName, 'DROP TYPE NonExistent IF EXISTS');
    expect(result.result).toBeDefined();
  });
});
