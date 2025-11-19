import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import {
  createDatabase,
  command,
  query,
  beginTransaction,
  commitTransaction,
  rollbackTransaction,
  commandWithSession,
  queryWithSession,
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

  test('query within transaction - read then update', async () => {
    // Setup: create account with initial balance
    await command(dbName, "INSERT INTO Account SET name = 'ReadThenUpdate', balance = 1000");

    const sessionId = await beginTransaction(dbName);

    // Query current balance within transaction
    const current = await queryWithSession(dbName, sessionId, "SELECT balance FROM Account WHERE name = 'ReadThenUpdate'");
    const currentBalance = current.result[0].balance;
    expect(currentBalance).toBe(1000);

    // Update based on queried value
    const newBalance = currentBalance - 200;
    await commandWithSession(dbName, sessionId, `UPDATE Account SET balance = ${newBalance} WHERE name = 'ReadThenUpdate'`);

    // Query again within same transaction - should see updated value
    const updated = await queryWithSession(dbName, sessionId, "SELECT balance FROM Account WHERE name = 'ReadThenUpdate'");
    expect(updated.result[0].balance).toBe(800);

    await commitTransaction(dbName, sessionId);

    // Verify after commit
    const final = await query(dbName, "SELECT balance FROM Account WHERE name = 'ReadThenUpdate'");
    expect(final.result[0].balance).toBe(800);
  });

  test('query within transaction - transfer between accounts', async () => {
    // Setup: create two accounts
    await command(dbName, "INSERT INTO Account SET name = 'Sender', balance = 500");
    await command(dbName, "INSERT INTO Account SET name = 'Receiver', balance = 100");

    const sessionId = await beginTransaction(dbName);

    // Read both balances
    const sender = await queryWithSession(dbName, sessionId, "SELECT balance FROM Account WHERE name = 'Sender'");
    const receiver = await queryWithSession(dbName, sessionId, "SELECT balance FROM Account WHERE name = 'Receiver'");

    const transferAmount = 150;
    const newSenderBalance = sender.result[0].balance - transferAmount;
    const newReceiverBalance = receiver.result[0].balance + transferAmount;

    // Perform transfer
    await commandWithSession(dbName, sessionId, `UPDATE Account SET balance = ${newSenderBalance} WHERE name = 'Sender'`);
    await commandWithSession(dbName, sessionId, `UPDATE Account SET balance = ${newReceiverBalance} WHERE name = 'Receiver'`);

    await commitTransaction(dbName, sessionId);

    // Verify final balances
    const finalSender = await query(dbName, "SELECT balance FROM Account WHERE name = 'Sender'");
    const finalReceiver = await query(dbName, "SELECT balance FROM Account WHERE name = 'Receiver'");

    expect(finalSender.result[0].balance).toBe(350);
    expect(finalReceiver.result[0].balance).toBe(250);
  });

  test('query within transaction sees own uncommitted changes', async () => {
    const sessionId = await beginTransaction(dbName);

    // Insert within transaction
    await commandWithSession(dbName, sessionId, "INSERT INTO Account SET name = 'TxVisible', balance = 999");

    // Query within same session should see the insert
    const withinTx = await queryWithSession(dbName, sessionId, "SELECT FROM Account WHERE name = 'TxVisible'");
    expect(withinTx.result.length).toBe(1);
    expect(withinTx.result[0].balance).toBe(999);

    await rollbackTransaction(dbName, sessionId);

    // After rollback, should not exist
    const afterRollback = await query(dbName, "SELECT FROM Account WHERE name = 'TxVisible'");
    expect(afterRollback.result.length).toBe(0);
  });
});
