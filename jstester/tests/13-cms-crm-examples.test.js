import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import {
  createDatabase,
  command,
  query,
  uniqueDbName,
  cleanupDatabase
} from './helpers.js';

describe('CMS/CRM Examples', () => {
  const dbName = uniqueDbName('test_cms_crm');

  beforeAll(async () => {
    await cleanupDatabase(dbName);
    await createDatabase(dbName);
  });

  afterAll(async () => {
    await cleanupDatabase(dbName);
  });

  describe('Contact Management', () => {
    beforeAll(async () => {
      // Create Contact schema
      await command(dbName, 'CREATE DOCUMENT TYPE Contact');
      await command(dbName, 'CREATE PROPERTY Contact.email STRING (mandatory true, notnull true)');
      await command(dbName, 'CREATE PROPERTY Contact.firstName STRING');
      await command(dbName, 'CREATE PROPERTY Contact.lastName STRING');
      await command(dbName, 'CREATE PROPERTY Contact.phone STRING');
      await command(dbName, 'CREATE PROPERTY Contact.status STRING (default "lead")');
      await command(dbName, 'CREATE PROPERTY Contact.createdAt DATETIME (default sysdate())');
      await command(dbName, 'CREATE INDEX ON Contact (email) UNIQUE');
    });

    test('insert contact', async () => {
      const result = await command(dbName, `
        INSERT INTO Contact SET
          firstName = 'John',
          lastName = 'Doe',
          email = 'john@example.com',
          status = 'customer'
      `);
      expect(result.result.length).toBe(1);
    });

    test('find contacts by status', async () => {
      await command(dbName, "INSERT INTO Contact SET firstName = 'Lead1', email = 'lead1@test.com', status = 'lead'");
      await command(dbName, "INSERT INTO Contact SET firstName = 'Lead2', email = 'lead2@test.com', status = 'lead'");

      const result = await query(dbName, "SELECT FROM Contact WHERE status = 'lead' ORDER BY firstName");
      expect(result.result.length).toBe(2);
    });

    test('update contact status', async () => {
      await command(dbName, "UPDATE Contact SET status = 'customer' WHERE email = 'lead1@test.com'");

      const check = await query(dbName, "SELECT FROM Contact WHERE email = 'lead1@test.com'");
      expect(check.result[0].status).toBe('customer');
    });

    test('search contacts with LIKE', async () => {
      const result = await query(dbName, "SELECT FROM Contact WHERE firstName LIKE 'Jo%' OR lastName LIKE 'Jo%'");
      expect(result.result.length).toBeGreaterThanOrEqual(1);
    });

    test('unique email constraint', async () => {
      const result = await command(dbName, "INSERT INTO Contact SET firstName = 'Dup', email = 'john@example.com'");
      expect(result.error).toBeDefined();
    });
  });

  describe('Organization Hierarchy', () => {
    beforeAll(async () => {
      // Create graph schema
      await command(dbName, 'CREATE VERTEX TYPE Organization');
      await command(dbName, 'CREATE PROPERTY Organization.name STRING');

      await command(dbName, 'CREATE VERTEX TYPE Department');
      await command(dbName, 'CREATE PROPERTY Department.name STRING');

      await command(dbName, 'CREATE VERTEX TYPE Employee');
      await command(dbName, 'CREATE PROPERTY Employee.name STRING');
      await command(dbName, 'CREATE PROPERTY Employee.title STRING');

      await command(dbName, 'CREATE EDGE TYPE BelongsTo');
      await command(dbName, 'CREATE EDGE TYPE Manages');
      await command(dbName, 'CREATE EDGE TYPE WorksIn');
    });

    test('create org structure', async () => {
      const org = await command(dbName, "CREATE VERTEX Organization SET name = 'Acme Corp'");
      const sales = await command(dbName, "CREATE VERTEX Department SET name = 'Sales'");
      const john = await command(dbName, "CREATE VERTEX Employee SET name = 'John', title = 'Manager'");
      const jane = await command(dbName, "CREATE VERTEX Employee SET name = 'Jane', title = 'Rep'");

      // Link department to org
      await command(dbName, `CREATE EDGE BelongsTo FROM ${sales.result[0]['@rid']} TO ${org.result[0]['@rid']}`);

      // Link employees to department
      await command(dbName, `CREATE EDGE WorksIn FROM ${john.result[0]['@rid']} TO ${sales.result[0]['@rid']}`);
      await command(dbName, `CREATE EDGE WorksIn FROM ${jane.result[0]['@rid']} TO ${sales.result[0]['@rid']}`);

      // Manager relationship
      await command(dbName, `CREATE EDGE Manages FROM ${john.result[0]['@rid']} TO ${jane.result[0]['@rid']}`);

      // Verify structure
      const employees = await query(dbName, 'SELECT FROM Employee');
      expect(employees.result.length).toBe(2);
    });

    test('find employees in department via MATCH', async () => {
      const result = await query(dbName, `
        MATCH {type: Employee, as: emp}.out('WorksIn'){type: Department, where: (name = 'Sales')}
        RETURN emp.name as name, emp.title as title
      `);
      expect(result.result.length).toBe(2);
    });

    test('find manager chain', async () => {
      const result = await query(dbName, `
        MATCH {type: Employee, where: (name = 'Jane')}.in('Manages'){as: mgr}
        RETURN mgr.name as manager
      `);
      expect(result.result.length).toBe(1);
      expect(result.result[0].manager).toBe('John');
    });

    test('find department for employee', async () => {
      const result = await query(dbName, `
        MATCH {type: Employee, where: (name = 'John')}.out('WorksIn'){as: dept}
        RETURN dept.name as department
      `);
      expect(result.result[0].department).toBe('Sales');
    });
  });

  describe('Content Management', () => {
    beforeAll(async () => {
      await command(dbName, 'CREATE DOCUMENT TYPE Article');
      await command(dbName, 'CREATE PROPERTY Article.title STRING (mandatory true)');
      await command(dbName, 'CREATE PROPERTY Article.slug STRING (mandatory true)');
      await command(dbName, 'CREATE PROPERTY Article.content STRING');
      await command(dbName, 'CREATE PROPERTY Article.status STRING (default "draft")');
      await command(dbName, 'CREATE PROPERTY Article.publishedAt DATETIME');
      await command(dbName, 'CREATE PROPERTY Article.tags LIST OF STRING');
      await command(dbName, 'CREATE INDEX ON Article (slug) UNIQUE');
      await command(dbName, 'CREATE INDEX ON Article (tags BY ITEM) NOTUNIQUE');
    });

    test('insert article', async () => {
      const result = await command(dbName, `
        INSERT INTO Article SET
          title = 'Getting Started',
          slug = 'getting-started',
          content = 'Welcome to our CMS...',
          status = 'published',
          publishedAt = sysdate(),
          tags = ['tutorial', 'beginner']
      `);
      expect(result.result.length).toBe(1);
    });

    test('find by tag', async () => {
      await command(dbName, `
        INSERT INTO Article SET
          title = 'Advanced Topics',
          slug = 'advanced-topics',
          content = 'Deep dive...',
          status = 'published',
          tags = ['tutorial', 'advanced']
      `);

      const result = await query(dbName, "SELECT FROM Article WHERE tags CONTAINS 'tutorial' AND status = 'published'");
      expect(result.result.length).toBe(2);
    });

    test('find by multiple tags', async () => {
      const result = await query(dbName, "SELECT FROM Article WHERE tags CONTAINS 'beginner'");
      expect(result.result.length).toBe(1);
      expect(result.result[0].title).toBe('Getting Started');
    });

    test('update article status', async () => {
      await command(dbName, `
        INSERT INTO Article SET
          title = 'Draft Article',
          slug = 'draft-article',
          status = 'draft',
          tags = ['wip']
      `);

      await command(dbName, "UPDATE Article SET status = 'published', publishedAt = sysdate() WHERE slug = 'draft-article'");

      const check = await query(dbName, "SELECT FROM Article WHERE slug = 'draft-article'");
      expect(check.result[0].status).toBe('published');
    });

    test('unique slug constraint', async () => {
      const result = await command(dbName, "INSERT INTO Article SET title = 'Dup', slug = 'getting-started'");
      expect(result.error).toBeDefined();
    });
  });
});
