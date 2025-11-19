import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import {
  createDatabase,
  command,
  query,
  uniqueDbName,
  cleanupDatabase
} from './helpers.js';

describe('CRUD - DELETE', () => {
  const dbName = uniqueDbName('test_crud_delete');

  beforeAll(async () => {
    await cleanupDatabase(dbName);
    await createDatabase(dbName);
    await command(dbName, 'CREATE DOCUMENT TYPE Contact');
    await command(dbName, 'CREATE PROPERTY Contact.name STRING');
    await command(dbName, 'CREATE PROPERTY Contact.status STRING');
  });

  afterAll(async () => {
    await cleanupDatabase(dbName);
  });

  test('delete with WHERE', async () => {
    await command(dbName, "INSERT INTO Contact SET name = 'ToDelete', status = 'temp'");

    const result = await command(dbName, "DELETE FROM Contact WHERE name = 'ToDelete'");
    expect(result.result).toBeDefined();

    const check = await query(dbName, "SELECT FROM Contact WHERE name = 'ToDelete'");
    expect(check.result.length).toBe(0);
  });

  test('delete with LIMIT', async () => {
    await command(dbName, "INSERT INTO Contact SET name = 'Del1', status = 'temp'");
    await command(dbName, "INSERT INTO Contact SET name = 'Del2', status = 'temp'");
    await command(dbName, "INSERT INTO Contact SET name = 'Del3', status = 'temp'");

    await command(dbName, "DELETE FROM Contact WHERE status = 'temp' LIMIT 1");

    const check = await query(dbName, "SELECT FROM Contact WHERE status = 'temp'");
    expect(check.result.length).toBe(2);
  });

  test('delete multiple records', async () => {
    await command(dbName, "INSERT INTO Contact SET name = 'Multi1', status = 'bulk'");
    await command(dbName, "INSERT INTO Contact SET name = 'Multi2', status = 'bulk'");
    await command(dbName, "INSERT INTO Contact SET name = 'Multi3', status = 'bulk'");

    await command(dbName, "DELETE FROM Contact WHERE status = 'bulk'");

    const check = await query(dbName, "SELECT FROM Contact WHERE status = 'bulk'");
    expect(check.result.length).toBe(0);
  });

  test('delete with parameters', async () => {
    await command(dbName, "INSERT INTO Contact SET name = 'ParamDel', status = 'param-test'");

    await command(dbName, 'DELETE FROM Contact WHERE name = :name', { name: 'ParamDel' });

    const check = await query(dbName, "SELECT FROM Contact WHERE name = 'ParamDel'");
    expect(check.result.length).toBe(0);
  });

  test('delete all from type', async () => {
    await command(dbName, 'CREATE DOCUMENT TYPE TempType');
    await command(dbName, "INSERT INTO TempType SET name = 'temp1'");
    await command(dbName, "INSERT INTO TempType SET name = 'temp2'");

    await command(dbName, 'DELETE FROM TempType');

    const check = await query(dbName, 'SELECT FROM TempType');
    expect(check.result.length).toBe(0);
  });
});
