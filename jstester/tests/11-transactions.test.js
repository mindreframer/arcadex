import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import {
  createDatabase,
  command,
  query,
  beginTransaction,
  commitTransaction,
  rollbackTransaction,
  commandWithSession,
  uniqueDbName,
  cleanupDatabase
} from './helpers.js';

describe('Transactions', () => {
  const dbName = uniqueDbName('test_transactions');

  beforeAll(async () => {
    await cleanupDatabase(dbName);
    await createDatabase(dbName);
    await command(dbName, 'CREATE DOCUMENT TYPE Account');
    await command(dbName, 'CREATE PROPERTY Account.name STRING');
    await command(dbName, 'CREATE PROPERTY Account.balance INTEGER');
  });

  afterAll(async () => {
    await cleanupDatabase(dbName);
  });

  test('begin and commit transaction', async () => {
    const sessionId = await beginTransaction(dbName);
    expect(sessionId).toBeDefined();
    expect(sessionId).toContain('AS-');

    await commandWithSession(dbName, sessionId, "INSERT INTO Account SET name = 'TxCommit', balance = 100");

    const committed = await commitTransaction(dbName, sessionId);
    expect(committed).toBe(true);

    // Verify data persisted
    const check = await query(dbName, "SELECT FROM Account WHERE name = 'TxCommit'");
    expect(check.result.length).toBe(1);
    expect(check.result[0].balance).toBe(100);
  });

  test('begin and rollback transaction', async () => {
    const sessionId = await beginTransaction(dbName);
    expect(sessionId).toBeDefined();

    await commandWithSession(dbName, sessionId, "INSERT INTO Account SET name = 'TxRollback', balance = 200");

    const rolledback = await rollbackTransaction(dbName, sessionId);
    expect(rolledback).toBe(true);

    // Verify data was NOT persisted
    const check = await query(dbName, "SELECT FROM Account WHERE name = 'TxRollback'");
    expect(check.result.length).toBe(0);
  });

  test('multiple operations in single transaction', async () => {
    const sessionId = await beginTransaction(dbName);

    await commandWithSession(dbName, sessionId, "INSERT INTO Account SET name = 'Multi1', balance = 100");
    await commandWithSession(dbName, sessionId, "INSERT INTO Account SET name = 'Multi2', balance = 200");
    await commandWithSession(dbName, sessionId, "UPDATE Account SET balance = 150 WHERE name = 'Multi1'");

    await commitTransaction(dbName, sessionId);

    const check1 = await query(dbName, "SELECT FROM Account WHERE name = 'Multi1'");
    expect(check1.result[0].balance).toBe(150);

    const check2 = await query(dbName, "SELECT FROM Account WHERE name = 'Multi2'");
    expect(check2.result[0].balance).toBe(200);
  });

  test('transaction isolation - changes not visible until commit', async () => {
    const sessionId = await beginTransaction(dbName);

    await commandWithSession(dbName, sessionId, "INSERT INTO Account SET name = 'Isolated', balance = 500");

    // Query without session should NOT see uncommitted data
    const beforeCommit = await query(dbName, "SELECT FROM Account WHERE name = 'Isolated'");
    expect(beforeCommit.result.length).toBe(0);

    await commitTransaction(dbName, sessionId);

    // After commit, data should be visible
    const afterCommit = await query(dbName, "SELECT FROM Account WHERE name = 'Isolated'");
    expect(afterCommit.result.length).toBe(1);
  });
});
