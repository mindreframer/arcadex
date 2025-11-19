import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import {
  createDatabase,
  command,
  query,
  uniqueDbName,
  cleanupDatabase
} from './helpers.js';

describe('Schema Properties', () => {
  const dbName = uniqueDbName('test_schema_props');

  beforeAll(async () => {
    await cleanupDatabase(dbName);
    await createDatabase(dbName);
    await command(dbName, 'CREATE DOCUMENT TYPE Contact');
    await command(dbName, 'CREATE DOCUMENT TYPE Product');
  });

  afterAll(async () => {
    await cleanupDatabase(dbName);
  });

  test('create string property', async () => {
    const result = await command(dbName, 'CREATE PROPERTY Contact.email STRING');
    expect(result.result).toBeDefined();
  });

  test('create property with mandatory constraint', async () => {
    const result = await command(dbName, 'CREATE PROPERTY Contact.firstName STRING (mandatory true)');
    expect(result.result).toBeDefined();
  });

  test('create property with notnull constraint', async () => {
    const result = await command(dbName, 'CREATE PROPERTY Contact.lastName STRING (notnull true)');
    expect(result.result).toBeDefined();
  });

  test('create property with default value', async () => {
    const result = await command(dbName, 'CREATE PROPERTY Contact.status STRING (default "active")');
    expect(result.result).toBeDefined();
  });

  test('create datetime property with default sysdate', async () => {
    const result = await command(dbName, 'CREATE PROPERTY Contact.createdAt DATETIME (default sysdate())');
    expect(result.result).toBeDefined();
  });

  test('create integer property', async () => {
    const result = await command(dbName, 'CREATE PROPERTY Contact.age INTEGER');
    expect(result.result).toBeDefined();
  });

  test('create decimal property with min/max', async () => {
    const result = await command(dbName, 'CREATE PROPERTY Product.price DECIMAL (min 0)');
    expect(result.result).toBeDefined();
  });

  test('create boolean property', async () => {
    const result = await command(dbName, 'CREATE PROPERTY Contact.active BOOLEAN');
    expect(result.result).toBeDefined();
  });

  test('create list property', async () => {
    const result = await command(dbName, 'CREATE PROPERTY Contact.tags LIST OF STRING');
    expect(result.result).toBeDefined();
  });

  test('create map property', async () => {
    const result = await command(dbName, 'CREATE PROPERTY Contact.metadata MAP');
    expect(result.result).toBeDefined();
  });

  test('create embedded property', async () => {
    const result = await command(dbName, 'CREATE PROPERTY Contact.address EMBEDDED');
    expect(result.result).toBeDefined();
  });

  test('create property IF NOT EXISTS', async () => {
    // Should not error if property already exists
    const result = await command(dbName, 'CREATE PROPERTY Contact.email IF NOT EXISTS STRING');
    expect(result.result).toBeDefined();
  });

  test('verify properties in schema', async () => {
    const result = await query(dbName, 'SELECT FROM schema:types WHERE name = "Contact"');
    expect(result.result.length).toBe(1);
    const props = result.result[0].properties;
    // Properties are stored as array in ArcadeDB
    const propNames = Array.isArray(props)
      ? props.map(p => p.name)
      : Object.values(props).map(p => p.name);
    expect(propNames).toContain('email');
    expect(propNames).toContain('firstName');
    expect(propNames).toContain('tags');
  });
});
