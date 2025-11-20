// Schema Migration System for ArcadeDB
// Similar to Ecto/ActiveRecord migrations

import { readdir } from 'fs/promises';
import { join, basename } from 'path';

const BASE_URL = 'http://localhost:2480';
const AUTH = 'Basic ' + btoa('root:playwithdata');

// ============================================================================
// HTTP Helpers
// ============================================================================

async function command(database, sql, params = null) {
  const body = { language: 'sql', command: sql };
  if (params) body.params = params;

  const res = await fetch(`${BASE_URL}/api/v1/command/${database}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': AUTH
    },
    body: JSON.stringify(body)
  });
  const json = await res.json();
  if (json.error) {
    throw new Error(json.error);
  }
  return json;
}

async function query(database, sql, params = null) {
  const body = { language: 'sql', command: sql };
  if (params) body.params = params;

  const res = await fetch(`${BASE_URL}/api/v1/query/${database}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': AUTH
    },
    body: JSON.stringify(body)
  });
  const json = await res.json();
  if (json.error) {
    throw new Error(json.error);
  }
  return json;
}

async function databaseExists(name) {
  const res = await fetch(`${BASE_URL}/api/v1/exists/${name}`, {
    headers: { 'Authorization': AUTH }
  });
  const data = await res.json();
  return data.result;
}

// ============================================================================
// Migrator Class
// ============================================================================

class Migrator {
  constructor(database, migrationsPath = './migrations') {
    this.db = database;
    this.migrationsPath = migrationsPath;
  }

  // Check database exists
  async checkDatabase() {
    const exists = await databaseExists(this.db);
    if (!exists) {
      throw new Error(`Database '${this.db}' does not exist`);
    }
  }

  // Ensure migrations table exists
  async ensureMigrationsTable() {
    await this.checkDatabase();

    // Check if _migrations type exists
    const typeCheck = await query(this.db,
      "SELECT FROM schema:types WHERE name = '_migrations'"
    );

    if (typeCheck.result.length === 0) {
      // Create fresh
      await command(this.db, 'CREATE DOCUMENT TYPE _migrations');
      await command(this.db, 'CREATE PROPERTY _migrations.version STRING');
      await command(this.db, 'CREATE PROPERTY _migrations.name STRING');
      await command(this.db, 'CREATE PROPERTY _migrations.appliedAt DATETIME');
      await command(this.db, 'CREATE INDEX ON _migrations (version) UNIQUE');
    }
  }

  // Get list of applied migrations
  async getAppliedMigrations() {
    const result = await query(this.db, 'SELECT version, name, appliedAt FROM _migrations ORDER BY version');
    return result.result || [];
  }

  // Get list of migration files
  async getMigrationFiles() {
    const files = await readdir(this.migrationsPath);

    // Filter and sort migration files (format: YYYYMMDDHHMMSS_name.js)
    return files
      .filter(f => f.endsWith('.js') && /^\d{14}_/.test(f))
      .sort()
      .map(f => ({
        filename: f,
        version: f.split('_')[0],
        name: f.replace(/^\d{14}_/, '').replace('.js', '')
      }));
  }

  // Get pending migrations
  async getPendingMigrations() {
    const applied = await this.getAppliedMigrations();
    const appliedVersions = new Set(applied.map(m => m.version));

    const files = await this.getMigrationFiles();
    return files.filter(f => !appliedVersions.has(f.version));
  }

  // Run a single migration up
  async runUp(migration) {
    const modulePath = join(this.migrationsPath, migration.filename);
    const module = await import(modulePath);

    if (typeof module.up !== 'function') {
      throw new Error(`Migration ${migration.filename} has no up() function`);
    }

    console.log(`  Applying: ${migration.version}_${migration.name}`);

    // Run the migration
    await module.up(this.db, command, query);

    // Record as applied
    await command(this.db, `
      INSERT INTO _migrations SET
        version = :version,
        name = :name,
        appliedAt = sysdate()
    `, {
      version: migration.version,
      name: migration.name
    });
  }

  // Run a single migration down
  async runDown(migration) {
    const modulePath = join(this.migrationsPath, migration.filename);
    const module = await import(modulePath);

    if (typeof module.down !== 'function') {
      throw new Error(`Migration ${migration.filename} has no down() function`);
    }

    console.log(`  Rolling back: ${migration.version}_${migration.name}`);

    // Run the rollback
    await module.down(this.db, command, query);

    // Remove from applied
    await command(this.db, 'DELETE FROM _migrations WHERE version = :version', {
      version: migration.version
    });
  }

  // Migrate to latest
  async migrate() {
    await this.ensureMigrationsTable();

    const pending = await this.getPendingMigrations();

    if (pending.length === 0) {
      console.log('No pending migrations.');
      return { applied: 0 };
    }

    console.log(`Found ${pending.length} pending migration(s):\n`);

    for (const migration of pending) {
      await this.runUp(migration);
    }

    console.log(`\nApplied ${pending.length} migration(s).`);
    return { applied: pending.length };
  }

  // Rollback last N migrations
  async rollback(steps = 1) {
    await this.ensureMigrationsTable();

    const applied = await this.getAppliedMigrations();

    if (applied.length === 0) {
      console.log('No migrations to rollback.');
      return { rolledBack: 0 };
    }

    const files = await this.getMigrationFiles();
    const fileMap = new Map(files.map(f => [f.version, f]));

    // Get last N applied migrations (in reverse order)
    const toRollback = applied
      .reverse()
      .slice(0, steps)
      .map(m => fileMap.get(m.version))
      .filter(Boolean);

    if (toRollback.length === 0) {
      console.log('No matching migration files found.');
      return { rolledBack: 0 };
    }

    console.log(`Rolling back ${toRollback.length} migration(s):\n`);

    for (const migration of toRollback) {
      await this.runDown(migration);
    }

    console.log(`\nRolled back ${toRollback.length} migration(s).`);
    return { rolledBack: toRollback.length };
  }

  // Reset database (rollback all, then migrate)
  async reset() {
    await this.ensureMigrationsTable();

    const applied = await this.getAppliedMigrations();

    if (applied.length > 0) {
      console.log('Rolling back all migrations...\n');
      await this.rollback(applied.length);
    }

    console.log('\nApplying all migrations...\n');
    return this.migrate();
  }

  // Show migration status
  async status() {
    await this.ensureMigrationsTable();

    const applied = await this.getAppliedMigrations();
    const appliedMap = new Map(applied.map(m => [m.version, m]));

    const files = await this.getMigrationFiles();

    console.log('Migration Status:\n');
    console.log('Status     Version         Name');
    console.log('-'.repeat(60));

    for (const file of files) {
      const appliedMigration = appliedMap.get(file.version);
      const status = appliedMigration ? 'applied' : 'pending';
      const date = appliedMigration
        ? new Date(appliedMigration.appliedAt).toISOString().split('T')[0]
        : '          ';

      console.log(`${status.padEnd(10)} ${file.version}  ${file.name}`);
    }

    const pendingCount = files.length - applied.length;
    console.log(`\nTotal: ${files.length} migrations, ${applied.length} applied, ${pendingCount} pending`);
  }
}

// ============================================================================
// CLI
// ============================================================================

async function main() {
  const args = process.argv.slice(2);

  if (args.length < 2) {
    console.log('Usage: bun migrator.js <database> <command> [options]');
    console.log('\nCommands:');
    console.log('  migrate           Apply pending migrations');
    console.log('  rollback [N]      Rollback last N migrations (default: 1)');
    console.log('  reset             Rollback all, then migrate');
    console.log('  status            Show migration status');
    console.log('  create <name>     Create a new migration file');
    console.log('\nExamples:');
    console.log('  bun migrator.js language_cms migrate');
    console.log('  bun migrator.js language_cms rollback 2');
    console.log('  bun migrator.js language_cms create add_user_preferences');
    process.exit(1);
  }

  const dbName = args[0];
  const cmd = args[1];
  const migrationsPath = join(import.meta.dir, '.');

  // Handle create command separately (doesn't need DB connection)
  if (cmd === 'create') {
    const name = args[2];
    if (!name) {
      console.error('Error: Migration name required');
      process.exit(1);
    }

    const timestamp = new Date().toISOString()
      .replace(/[-:T]/g, '')
      .slice(0, 14);

    const filename = `${timestamp}_${name}.js`;
    const filepath = join(migrationsPath, filename);

    const template = `// Migration: ${name}
// Created: ${new Date().toISOString()}

export async function up(db, command, query) {
  // Apply migration
  // Example:
  // await command(db, 'CREATE DOCUMENT TYPE MyType IF NOT EXISTS');
  // await command(db, 'CREATE PROPERTY MyType.name STRING IF NOT EXISTS');
}

export async function down(db, command, query) {
  // Rollback migration
  // Example:
  // await command(db, 'DROP TYPE MyType IF EXISTS');
}
`;

    await Bun.write(filepath, template);
    console.log(`Created: ${filename}`);
    process.exit(0);
  }

  const migrator = new Migrator(dbName, migrationsPath);

  switch (cmd) {
    case 'migrate':
      await migrator.migrate();
      break;

    case 'rollback':
      const steps = parseInt(args[2]) || 1;
      await migrator.rollback(steps);
      break;

    case 'reset':
      await migrator.reset();
      break;

    case 'status':
      await migrator.status();
      break;

    default:
      console.error(`Unknown command: ${cmd}`);
      process.exit(1);
  }
}

// Export for programmatic use
export { Migrator, command, query };

// Run CLI if executed directly
if (import.meta.main) {
  main().catch(err => {
    console.error('Error:', err.message);
    process.exit(1);
  });
}
