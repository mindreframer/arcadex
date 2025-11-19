import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import {
  createDatabase,
  command,
  query,
  uniqueDbName,
  cleanupDatabase
} from './helpers.js';

describe('CRUD - UPDATE', () => {
  const dbName = uniqueDbName('test_crud_update');

  beforeAll(async () => {
    await cleanupDatabase(dbName);
    await createDatabase(dbName);
    await command(dbName, 'CREATE DOCUMENT TYPE Contact');
    await command(dbName, 'CREATE PROPERTY Contact.name STRING');
    await command(dbName, 'CREATE PROPERTY Contact.email STRING');
    await command(dbName, 'CREATE PROPERTY Contact.status STRING');
    await command(dbName, 'CREATE PROPERTY Contact.tags LIST OF STRING');
    await command(dbName, 'CREATE PROPERTY Contact.score INTEGER');
  });

  afterAll(async () => {
    await cleanupDatabase(dbName);
  });

  test('update single field', async () => {
    await command(dbName, "INSERT INTO Contact SET name = 'John', status = 'pending'");

    const result = await command(dbName, "UPDATE Contact SET status = 'active' WHERE name = 'John'");
    expect(result.result).toBeDefined();

    const check = await query(dbName, "SELECT FROM Contact WHERE name = 'John'");
    expect(check.result[0].status).toBe('active');
  });

  test('update multiple fields', async () => {
    await command(dbName, "INSERT INTO Contact SET name = 'Jane', status = 'pending', score = 0");

    await command(dbName, "UPDATE Contact SET status = 'active', score = 100 WHERE name = 'Jane'");

    const check = await query(dbName, "SELECT FROM Contact WHERE name = 'Jane'");
    expect(check.result[0].status).toBe('active');
    expect(check.result[0].score).toBe(100);
  });

  test('update with REMOVE field', async () => {
    await command(dbName, "INSERT INTO Contact SET name = 'RemoveTest', email = 'test@test.com', status = 'temp'");

    await command(dbName, "UPDATE Contact REMOVE status WHERE name = 'RemoveTest'");

    const check = await query(dbName, "SELECT FROM Contact WHERE name = 'RemoveTest'");
    expect(check.result[0].status).toBeUndefined();
  });

  test('update with list append', async () => {
    await command(dbName, "INSERT INTO Contact SET name = 'ListTest', tags = ['initial']");

    await command(dbName, "UPDATE Contact SET tags += 'new-tag' WHERE name = 'ListTest'");

    const check = await query(dbName, "SELECT FROM Contact WHERE name = 'ListTest'");
    expect(check.result[0].tags).toContain('initial');
    expect(check.result[0].tags).toContain('new-tag');
  });

  test('update with REMOVE from list', async () => {
    await command(dbName, "INSERT INTO Contact SET name = 'ListRemoveTest', tags = ['keep', 'remove']");

    await command(dbName, "UPDATE Contact REMOVE tags = 'remove' WHERE name = 'ListRemoveTest'");

    const check = await query(dbName, "SELECT FROM Contact WHERE name = 'ListRemoveTest'");
    expect(check.result[0].tags).toContain('keep');
    expect(check.result[0].tags).not.toContain('remove');
  });

  test('update with LIMIT', async () => {
    await command(dbName, "INSERT INTO Contact SET name = 'Batch1', status = 'pending'");
    await command(dbName, "INSERT INTO Contact SET name = 'Batch2', status = 'pending'");
    await command(dbName, "INSERT INTO Contact SET name = 'Batch3', status = 'pending'");

    await command(dbName, "UPDATE Contact SET status = 'processed' WHERE status = 'pending' LIMIT 1");

    const check = await query(dbName, "SELECT FROM Contact WHERE status = 'processed' AND name LIKE 'Batch%'");
    expect(check.result.length).toBe(1);
  });

  test('update with UPSERT', async () => {
    // Create unique index for UPSERT to work correctly
    await command(dbName, 'CREATE INDEX ON Contact (email) UNIQUE');

    // First upsert - should insert
    await command(dbName, "UPDATE Contact SET name = 'Upsert1', email = 'upsert@test.com' UPSERT WHERE email = 'upsert@test.com'");

    let check = await query(dbName, "SELECT FROM Contact WHERE email = 'upsert@test.com'");
    expect(check.result.length).toBe(1);

    // Second upsert - should update
    check = await command(dbName, "UPDATE Contact SET name = 'Upsert1-Updated', email = 'upsert@test.com' UPSERT WHERE email = 'upsert@test.com'");


    check = await query(dbName, "SELECT FROM Contact WHERE email = 'upsert@test.com'");
    expect(check.result.length).toBe(1);
    expect(check.result[0].name).toBe('Upsert1-Updated');
  });

  test('update with RETURN AFTER', async () => {
    await command(dbName, "INSERT INTO Contact SET name = 'ReturnTest', status = 'old'");

    const result = await command(dbName, "UPDATE Contact SET status = 'new' RETURN AFTER WHERE name = 'ReturnTest'");
    expect(result.result[0].status).toBe('new');
  });

  test('update with parameters', async () => {
    await command(dbName, "INSERT INTO Contact SET name = 'ParamTest', status = 'pending'");

    await command(dbName, 'UPDATE Contact SET status = :status WHERE name = :name', {
      status: 'active',
      name: 'ParamTest'
    });

    const check = await query(dbName, "SELECT FROM Contact WHERE name = 'ParamTest'");
    expect(check.result[0].status).toBe('active');
  });
});
