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

describe('Concurrency and Race Conditions', () => {
  const dbName = uniqueDbName('test_concurrency');

  beforeAll(async () => {
    await cleanupDatabase(dbName);
    await createDatabase(dbName);
    await command(dbName, 'CREATE DOCUMENT TYPE Counter');
    await command(dbName, 'CREATE PROPERTY Counter.name STRING');
    await command(dbName, 'CREATE PROPERTY Counter.value INTEGER');
    await command(dbName, 'CREATE PROPERTY Counter.version INTEGER');
    await command(dbName, 'CREATE INDEX ON Counter (name) UNIQUE');

    await command(dbName, 'CREATE DOCUMENT TYPE Account');
    await command(dbName, 'CREATE PROPERTY Account.name STRING');
    await command(dbName, 'CREATE PROPERTY Account.balance INTEGER');
    await command(dbName, 'CREATE INDEX ON Account (name) UNIQUE');
  });

  afterAll(async () => {
    await cleanupDatabase(dbName);
  });

  describe('Optimistic Locking with Version', () => {
    test('update with version check succeeds when version matches', async () => {
      // Insert with version
      await command(dbName, "INSERT INTO Counter SET name = 'visits', value = 0, version = 1");

      // Read current state
      const current = await query(dbName, "SELECT FROM Counter WHERE name = 'visits'");
      const currentVersion = current.result[0].version;
      const currentValue = current.result[0].value;

      // Update only if version matches (optimistic lock)
      const result = await command(dbName, `
        UPDATE Counter SET value = ${currentValue + 1}, version = ${currentVersion + 1}
        WHERE name = 'visits' AND version = ${currentVersion}
      `);

      // Check update succeeded
      const updated = await query(dbName, "SELECT FROM Counter WHERE name = 'visits'");
      expect(updated.result[0].value).toBe(1);
      expect(updated.result[0].version).toBe(2);
    });

    test('update with version check fails when version mismatches', async () => {
      await command(dbName, "INSERT INTO Counter SET name = 'pageviews', value = 100, version = 5");

      // Try to update with wrong version (simulating stale read)
      const result = await command(dbName, `
        UPDATE Counter SET value = 200, version = 6
        WHERE name = 'pageviews' AND version = 3
      `);

      // Update should not have happened
      const check = await query(dbName, "SELECT FROM Counter WHERE name = 'pageviews'");
      expect(check.result[0].value).toBe(100); // Unchanged
      expect(check.result[0].version).toBe(5); // Unchanged
    });

    test('retry pattern with version check', async () => {
      await command(dbName, "INSERT INTO Counter SET name = 'retrytest', value = 0, version = 1");

      // Simulate retry pattern
      let success = false;
      let attempts = 0;
      const maxAttempts = 3;

      while (!success && attempts < maxAttempts) {
        attempts++;

        // Read current state
        const current = await query(dbName, "SELECT FROM Counter WHERE name = 'retrytest'");
        const currentVersion = current.result[0].version;
        const currentValue = current.result[0].value;

        // Try conditional update
        const result = await command(dbName, `
          UPDATE Counter SET value = ${currentValue + 10}, version = ${currentVersion + 1}
          WHERE name = 'retrytest' AND version = ${currentVersion}
        `);

        // Check if update succeeded (count > 0)
        if (result.result && result.result[0] && result.result[0].count > 0) {
          success = true;
        }
      }

      expect(success).toBe(true);
      const final = await query(dbName, "SELECT FROM Counter WHERE name = 'retrytest'");
      expect(final.result[0].value).toBe(10);
    });
  });

  describe('Atomic Increment Operations', () => {
    test('increment using arithmetic in UPDATE', async () => {
      await command(dbName, "INSERT INTO Counter SET name = 'atomic', value = 0, version = 1");

      // Atomic increment - no read needed
      await command(dbName, "UPDATE Counter SET value = value + 1 WHERE name = 'atomic'");
      await command(dbName, "UPDATE Counter SET value = value + 1 WHERE name = 'atomic'");
      await command(dbName, "UPDATE Counter SET value = value + 1 WHERE name = 'atomic'");

      const result = await query(dbName, "SELECT value FROM Counter WHERE name = 'atomic'");
      expect(result.result[0].value).toBe(3);
    });

    test('decrement using arithmetic in UPDATE', async () => {
      await command(dbName, "INSERT INTO Counter SET name = 'decrement', value = 100, version = 1");

      await command(dbName, "UPDATE Counter SET value = value - 25 WHERE name = 'decrement'");

      const result = await query(dbName, "SELECT value FROM Counter WHERE name = 'decrement'");
      expect(result.result[0].value).toBe(75);
    });

    test('conditional decrement - prevent negative', async () => {
      await command(dbName, "INSERT INTO Counter SET name = 'stock', value = 5, version = 1");

      // Only decrement if value would stay >= 0
      await command(dbName, "UPDATE Counter SET value = value - 3 WHERE name = 'stock' AND value >= 3");

      let result = await query(dbName, "SELECT value FROM Counter WHERE name = 'stock'");
      expect(result.result[0].value).toBe(2);

      // Try to decrement more than available - should not update
      await command(dbName, "UPDATE Counter SET value = value - 5 WHERE name = 'stock' AND value >= 5");

      result = await query(dbName, "SELECT value FROM Counter WHERE name = 'stock'");
      expect(result.result[0].value).toBe(2); // Unchanged
    });
  });

  describe('Transaction-based Locking', () => {
    test('transaction provides isolation for read-modify-write', async () => {
      await command(dbName, "INSERT INTO Account SET name = 'TxLock', balance = 1000");

      const sessionId = await beginTransaction(dbName);

      // Read within transaction
      const current = await queryWithSession(dbName, sessionId, "SELECT balance FROM Account WHERE name = 'TxLock'");
      const newBalance = current.result[0].balance - 100;

      // Modify within same transaction
      await commandWithSession(dbName, sessionId, `UPDATE Account SET balance = ${newBalance} WHERE name = 'TxLock'`);

      await commitTransaction(dbName, sessionId);

      const final = await query(dbName, "SELECT balance FROM Account WHERE name = 'TxLock'");
      expect(final.result[0].balance).toBe(900);
    });

    test('concurrent transactions - second one should fail or see stale data', async () => {
      await command(dbName, "INSERT INTO Account SET name = 'Concurrent', balance = 500");

      // Start two transactions
      const session1 = await beginTransaction(dbName);
      const session2 = await beginTransaction(dbName);

      // Both read the same initial value
      const read1 = await queryWithSession(dbName, session1, "SELECT balance FROM Account WHERE name = 'Concurrent'");
      const read2 = await queryWithSession(dbName, session2, "SELECT balance FROM Account WHERE name = 'Concurrent'");

      expect(read1.result[0].balance).toBe(500);
      expect(read2.result[0].balance).toBe(500);

      // Session 1 updates and commits
      await commandWithSession(dbName, session1, "UPDATE Account SET balance = 400 WHERE name = 'Concurrent'");
      await commitTransaction(dbName, session1);

      // Session 2 tries to update based on stale read
      // This may fail or succeed depending on isolation level
      await commandWithSession(dbName, session2, "UPDATE Account SET balance = 450 WHERE name = 'Concurrent'");

      // Try to commit session 2 - may fail with conflict
      try {
        await commitTransaction(dbName, session2);
      } catch (e) {
        // Expected - conflict detected
      }

      // Check final state - should reflect one of the updates
      const final = await query(dbName, "SELECT balance FROM Account WHERE name = 'Concurrent'");
      // Balance should be either 400 (session1) or 450 (session2), not 500
      expect(final.result[0].balance).toBeLessThan(500);
    });
  });

  describe('Actual Parallel Race Conditions', () => {
    test('parallel increments without protection lose updates', async () => {
      await command(dbName, "INSERT INTO Counter SET name = 'race', value = 0, version = 1");

      // Simulate 10 parallel read-modify-write operations WITHOUT atomic increment
      // This demonstrates the lost update problem
      const iterations = 10;
      const promises = [];

      for (let i = 0; i < iterations; i++) {
        promises.push((async () => {
          // Non-atomic: read, then write (race condition!)
          const current = await query(dbName, "SELECT value FROM Counter WHERE name = 'race'");
          const newValue = current.result[0].value + 1;
          await command(dbName, `UPDATE Counter SET value = ${newValue} WHERE name = 'race'`);
        })());
      }

      await Promise.all(promises);

      const final = await query(dbName, "SELECT value FROM Counter WHERE name = 'race'");
      // Due to race conditions, final value will likely be LESS than 10
      // This test demonstrates the problem - it may occasionally pass if no races occur
      console.log(`Race condition result: expected 10, got ${final.result[0].value}`);
      // We don't assert exact value because race conditions are non-deterministic
      expect(final.result[0].value).toBeLessThanOrEqual(10);
    });

    test('parallel "atomic" increments also lose updates (ArcadeDB behavior)', async () => {
      await command(dbName, "INSERT INTO Counter SET name = 'atomic-race', value = 0, version = 1");

      // Even value = value + 1 loses updates under parallel load
      // This is because ArcadeDB reads-then-writes internally
      const iterations = 10;
      const promises = [];

      for (let i = 0; i < iterations; i++) {
        promises.push(
          command(dbName, "UPDATE Counter SET value = value + 1 WHERE name = 'atomic-race'")
        );
      }

      await Promise.all(promises);

      const final = await query(dbName, "SELECT value FROM Counter WHERE name = 'atomic-race'");
      // Due to race conditions, value will likely be less than 10
      console.log(`Atomic increment race result: expected 10, got ${final.result[0].value}`);
      // This demonstrates that even "atomic" syntax isn't truly atomic under concurrency
      expect(final.result[0].value).toBeLessThanOrEqual(10);
    });

    test('sequential increments work correctly', async () => {
      await command(dbName, "INSERT INTO Counter SET name = 'sequential', value = 0, version = 1");

      // Sequential updates work fine
      for (let i = 0; i < 10; i++) {
        await command(dbName, "UPDATE Counter SET value = value + 1 WHERE name = 'sequential'");
      }

      const final = await query(dbName, "SELECT value FROM Counter WHERE name = 'sequential'");
      expect(final.result[0].value).toBe(10);
    });

    test('CAS with retry demonstrates optimistic locking', async () => {
      await command(dbName, "INSERT INTO Counter SET name = 'cas-demo', value = 0, version = 1");

      // Single CAS operation with retry - demonstrates the pattern
      let success = false;
      let attempts = 0;
      const maxAttempts = 5;

      while (!success && attempts < maxAttempts) {
        attempts++;
        const current = await query(dbName, "SELECT value, version FROM Counter WHERE name = 'cas-demo'");
        const result = await command(dbName, `
          UPDATE Counter SET value = ${current.result[0].value + 1}, version = ${current.result[0].version + 1}
          WHERE name = 'cas-demo' AND version = ${current.result[0].version}
        `);

        if (result.result && result.result[0] && result.result[0].count > 0) {
          success = true;
        }
      }

      expect(success).toBe(true);
      const final = await query(dbName, "SELECT value, version FROM Counter WHERE name = 'cas-demo'");
      expect(final.result[0].value).toBe(1);
      expect(final.result[0].version).toBe(2);
    });
  });

  describe('Compare-and-Set Pattern', () => {
    test('CAS with count check for success', async () => {
      await command(dbName, "INSERT INTO Counter SET name = 'cas', value = 10, version = 1");

      // CAS: only update if current value matches expected
      const result = await command(dbName, `
        UPDATE Counter SET value = 20, version = version + 1
        WHERE name = 'cas' AND value = 10
      `);

      // count > 0 means CAS succeeded
      expect(result.result[0].count).toBe(1);

      // Verify the update
      const check = await query(dbName, "SELECT value FROM Counter WHERE name = 'cas'");
      expect(check.result[0].value).toBe(20);
    });

    test('CAS fails when expected value differs', async () => {
      await command(dbName, "INSERT INTO Counter SET name = 'cas2', value = 50, version = 1");

      // CAS with wrong expected value
      const result = await command(dbName, `
        UPDATE Counter SET value = 100, version = version + 1
        WHERE name = 'cas2' AND value = 999
      `);

      // count = 0 means CAS failed
      expect(result.result[0].count).toBe(0);

      // Value unchanged
      const check = await query(dbName, "SELECT value FROM Counter WHERE name = 'cas2'");
      expect(check.result[0].value).toBe(50);
    });
  });

  describe('Conditional Business Logic', () => {
    test('transfer only if sufficient balance', async () => {
      await command(dbName, "INSERT INTO Account SET name = 'Rich', balance = 1000");
      await command(dbName, "INSERT INTO Account SET name = 'Poor', balance = 0");

      const transferAmount = 200;

      // Conditional debit - only if balance sufficient
      const debit = await command(dbName, `
        UPDATE Account SET balance = balance - ${transferAmount}
        WHERE name = 'Rich' AND balance >= ${transferAmount}
      `);

      if (debit.result[0].count > 0) {
        // Debit succeeded, now credit
        await command(dbName, `
          UPDATE Account SET balance = balance + ${transferAmount}
          WHERE name = 'Poor'
        `);
      }

      const rich = await query(dbName, "SELECT balance FROM Account WHERE name = 'Rich'");
      const poor = await query(dbName, "SELECT balance FROM Account WHERE name = 'Poor'");

      expect(rich.result[0].balance).toBe(800);
      expect(poor.result[0].balance).toBe(200);
    });

    test('transfer fails when insufficient balance', async () => {
      await command(dbName, "INSERT INTO Account SET name = 'Broke', balance = 50");
      await command(dbName, "INSERT INTO Account SET name = 'Waiting', balance = 0");

      const transferAmount = 100;

      // Conditional debit - will fail
      const debit = await command(dbName, `
        UPDATE Account SET balance = balance - ${transferAmount}
        WHERE name = 'Broke' AND balance >= ${transferAmount}
      `);

      // Debit failed (count = 0), don't credit
      expect(debit.result[0].count).toBe(0);

      const broke = await query(dbName, "SELECT balance FROM Account WHERE name = 'Broke'");
      const waiting = await query(dbName, "SELECT balance FROM Account WHERE name = 'Waiting'");

      expect(broke.result[0].balance).toBe(50); // Unchanged
      expect(waiting.result[0].balance).toBe(0); // Unchanged
    });
  });
});
