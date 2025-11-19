// Course Importer for Language Learning CMS
// Imports JSONL files into ArcadeDB

import { readFileSync } from 'fs';
import {
  LanguageCMS,
  createSchema,
  createDatabase,
  dropDatabase,
  databaseExists,
  command,
  query
} from './language-cms.js';

const BASE_URL = 'http://localhost:2480';
const AUTH = 'Basic ' + btoa('root:playwithdata');

// ============================================================================
// Importer Class
// ============================================================================

class CourseImporter {
  constructor(database) {
    this.db = database;
    this.cms = new LanguageCMS(database);

    // ID to RID mappings
    this.courseMap = new Map();   // course id -> @rid
    this.trackMap = new Map();    // track id -> @rid
    this.deckMap = new Map();     // deck id -> @rid
    this.cardMap = new Map();     // card id -> @rid (base cards by base_card id)
    this.hostCardMap = new Map(); // card id -> @rid (host cards by their id)

    // Stats
    this.stats = {
      courses: 0,
      tracks: 0,
      decks: 0,
      cards: 0,
      errors: []
    };
  }

  async importFile(filePath) {
    console.log(`\nImporting from: ${filePath}`);

    const content = readFileSync(filePath, 'utf-8');
    const lines = content.trim().split('\n');

    console.log(`Found ${lines.length} records to import\n`);

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;

      try {
        const record = JSON.parse(line);
        await this.processRecord(record, i + 1);
      } catch (e) {
        this.stats.errors.push(`Line ${i + 1}: ${e.message}`);
        console.error(`Error on line ${i + 1}:`, e.message);
      }

      // Progress indicator
      if ((i + 1) % 100 === 0) {
        console.log(`Processed ${i + 1}/${lines.length} records...`);
      }
    }

    return this.stats;
  }

  async processRecord(record, lineNum) {
    const { op, type, payload } = record;

    if (op !== 'insert') {
      console.warn(`Line ${lineNum}: Skipping non-insert operation: ${op}`);
      return;
    }

    switch (type) {
      case 'course':
        await this.importCourse(payload);
        break;
      case 'track':
        await this.importTrack(payload);
        break;
      case 'deck':
        await this.importDeck(payload);
        break;
      case 'card':
        await this.importCard(payload);
        break;
      default:
        console.warn(`Line ${lineNum}: Unknown type: ${type}`);
    }
  }

  async importCourse(payload) {
    // Parse lang_combo: "en_us.si.sl" -> baseLang: "en_us", hostCountry: "si", hostLang: "sl"
    const [baseLang, hostCountry, hostLang] = payload.lang_combo.split('.');

    // Create base course with uid
    const baseCourse = await command(this.db, `
      INSERT INTO BaseCourse SET
        uid = :uid,
        name = :name,
        summary = :summary,
        lang = :lang,
        version = :version,
        createdAt = sysdate(),
        updatedAt = sysdate()
    `, {
      uid: payload.id,
      name: payload.name,
      summary: `Imported from ${payload.base_folder}`,
      lang: baseLang.replace('_', '-'),
      version: payload.version || 1
    });

    this.courseMap.set(payload.id, {
      baseRid: baseCourse.result[0]['@rid'],
      baseLang,
      hostCountry: hostCountry.toUpperCase(),
      hostLang
    });

    // Create host course with uid
    const hostCourse = await command(this.db, `
      INSERT INTO HostCourse SET
        uid = :uid,
        baseCourse = :baseCourseRid,
        hostCountry = :hostCountry,
        hostLang = :hostLang,
        name = :name,
        summary = :summary,
        createdAt = sysdate(),
        updatedAt = sysdate()
    `, {
      uid: payload.id, // same as base for now
      baseCourseRid: baseCourse.result[0]['@rid'],
      hostCountry: hostCountry.toUpperCase(),
      hostLang,
      name: payload.name,
      summary: `Imported from ${payload.base_folder}`
    });

    this.courseMap.get(payload.id).hostRid = hostCourse.result[0]['@rid'];

    this.stats.courses++;
    console.log(`  Course: ${payload.name} (${payload.id})`);
  }

  async importTrack(payload) {
    const courseData = this.courseMap.get(payload.course);
    if (!courseData) {
      throw new Error(`Course not found: ${payload.course}`);
    }

    // Create base track with uid
    // Note: LINK fields need RID directly in SQL, not via parameters
    const baseTrack = await command(this.db, `
      INSERT INTO BaseTrack SET
        uid = :uid,
        name = :name,
        lang = :lang,
        course = ${courseData.baseRid},
        \`order\` = :order,
        createdAt = sysdate(),
        updatedAt = sysdate()
    `, {
      uid: payload.id,
      name: payload.name,
      lang: courseData.baseLang.replace('_', '-'),
      order: payload.position || 0
    });

    this.trackMap.set(payload.id, {
      baseRid: baseTrack.result[0]['@rid']
    });

    // Create host track with uid
    const hostTrack = await command(this.db, `
      INSERT INTO HostTrack SET
        uid = :uid,
        baseTrack = ${baseTrack.result[0]['@rid']},
        hostCourse = ${courseData.hostRid},
        hostCountry = :hostCountry,
        hostLang = :hostLang,
        name = :name,
        createdAt = sysdate(),
        updatedAt = sysdate()
    `, {
      uid: payload.id,
      hostCountry: courseData.hostCountry,
      hostLang: courseData.hostLang,
      name: payload.name
    });

    this.trackMap.get(payload.id).hostRid = hostTrack.result[0]['@rid'];

    this.stats.tracks++;
  }

  async importDeck(payload) {
    const trackData = this.trackMap.get(payload.track);
    if (!trackData) {
      throw new Error(`Track not found: ${payload.track}`);
    }

    // Get course data for host info
    const trackId = payload.track;
    const courseId = trackId.split('.').slice(0, -1).join('.') ||
      Array.from(this.courseMap.keys()).find(k => trackId.startsWith(k));
    const courseData = this.courseMap.get(courseId) ||
      this.courseMap.values().next().value;

    // Create base deck with uid
    const baseDeck = await command(this.db, `
      INSERT INTO BaseDeck SET
        uid = :uid,
        name = :name,
        lang = :lang,
        track = ${trackData.baseRid},
        \`order\` = :order,
        createdAt = sysdate(),
        updatedAt = sysdate()
    `, {
      uid: payload.id,
      name: payload.name,
      lang: courseData.baseLang.replace('_', '-'),
      order: payload.position || 0
    });

    this.deckMap.set(payload.id, {
      baseRid: baseDeck.result[0]['@rid'],
      courseData
    });

    // Create host deck with uid
    const hostDeck = await command(this.db, `
      INSERT INTO HostDeck SET
        uid = :uid,
        baseDeck = ${baseDeck.result[0]['@rid']},
        hostTrack = ${trackData.hostRid},
        hostCountry = :hostCountry,
        hostLang = :hostLang,
        name = :name,
        createdAt = sysdate(),
        updatedAt = sysdate()
    `, {
      uid: payload.id,
      hostCountry: courseData.hostCountry,
      hostLang: courseData.hostLang,
      name: payload.name
    });

    this.deckMap.get(payload.id).hostRid = hostDeck.result[0]['@rid'];

    this.stats.decks++;
  }

  async importCard(payload) {
    const deckData = this.deckMap.get(payload.deck);
    if (!deckData) {
      throw new Error(`Deck not found: ${payload.deck}`);
    }

    // Check if base card already exists (by base_card id)
    let baseCardRid = this.cardMap.get(payload.base_card);

    if (!baseCardRid) {
      // Create new base card with uid
      // Note: LINK fields need RID directly in SQL, not via parameters
      const baseCard = await command(this.db, `
        INSERT INTO BaseCard SET
          uid = :uid,
          text = :text,
          deck = ${deckData.baseRid},
          countryAffinity = :countryAffinity,
          \`order\` = :order,
          cloze_text = :clozeText,
          createdAt = sysdate(),
          updatedAt = sysdate()
      `, {
        uid: payload.base_card,
        text: payload.text,
        countryAffinity: null,
        order: payload.position || 0,
        clozeText: payload.cloze_text || null
      });
      baseCardRid = baseCard.result[0]['@rid'];
      this.cardMap.set(payload.base_card, baseCardRid);
    }

    // Create host card with uid
    // Note: LINK fields (baseCard, hostDeck) need RID directly in SQL
    const result = await command(this.db, `
      INSERT INTO HostCard SET
        uid = :uid,
        baseCard = ${baseCardRid},
        hostDeck = ${deckData.hostRid},
        hostCountry = :hostCountry,
        hostLang = :hostLang,
        translation = :translation,
        explanation1 = :explanation1,
        explanation2 = :explanation2,
        explanation3 = :explanation3,
        createdAt = sysdate(),
        updatedAt = sysdate()
    `, {
      uid: payload.id,
      hostCountry: deckData.courseData.hostCountry,
      hostLang: deckData.courseData.hostLang,
      translation: payload.translation || null,
      // Remap explanation fields
      explanation1: payload.explain_short || null,  // lv1 - short
      explanation2: payload.explain || null,        // lv2 - medium
      explanation3: payload.explain_long || null    // lv3 - long
    });

    this.hostCardMap.set(payload.id, result.result[0]['@rid']);

    this.stats.cards++;
  }
}

// ============================================================================
// Main
// ============================================================================

async function main() {
  const args = process.argv.slice(2);

  if (args.length === 0) {
    console.log('Usage: bun run import-course.js <jsonl-file> [database-name]');
    console.log('Example: bun run import-course.js ./kids-en_us-si-sl-final.jsonl language_cms_import');
    process.exit(1);
  }

  const filePath = args[0];
  const dbName = args[1] || 'language_cms_import';

  console.log('='.repeat(60));
  console.log('Language CMS Course Importer');
  console.log('='.repeat(60));

  // Setup database
  console.log(`\nSetting up database: ${dbName}`);
  if (await databaseExists(dbName)) {
    console.log('Dropping existing database...');
    await dropDatabase(dbName);
  }
  await createDatabase(dbName);
  await createSchema(dbName);

  // Add cloze_text property to BaseCard
  await command(dbName, 'CREATE PROPERTY BaseCard.cloze_text STRING IF NOT EXISTS');

  // Import
  const importer = new CourseImporter(dbName);
  const stats = await importer.importFile(filePath);

  // Report
  console.log('\n' + '='.repeat(60));
  console.log('Import Complete');
  console.log('='.repeat(60));
  console.log(`Courses: ${stats.courses}`);
  console.log(`Tracks:  ${stats.tracks}`);
  console.log(`Decks:   ${stats.decks}`);
  console.log(`Cards:   ${stats.cards}`);

  if (stats.errors.length > 0) {
    console.log(`\nErrors (${stats.errors.length}):`);
    stats.errors.slice(0, 10).forEach(e => console.log(`  - ${e}`));
    if (stats.errors.length > 10) {
      console.log(`  ... and ${stats.errors.length - 10} more`);
    }
  }

  // Verify
  console.log('\nVerification:');
  const courseCount = await query(dbName, 'SELECT count(*) as cnt FROM BaseCourse');
  const trackCount = await query(dbName, 'SELECT count(*) as cnt FROM BaseTrack');
  const deckCount = await query(dbName, 'SELECT count(*) as cnt FROM BaseDeck');
  const baseCardCount = await query(dbName, 'SELECT count(*) as cnt FROM BaseCard');
  const hostCardCount = await query(dbName, 'SELECT count(*) as cnt FROM HostCard');

  console.log(`  BaseCourse: ${courseCount.result[0].cnt}`);
  console.log(`  BaseTrack:  ${trackCount.result[0].cnt}`);
  console.log(`  BaseDeck:   ${deckCount.result[0].cnt}`);
  console.log(`  BaseCard:   ${baseCardCount.result[0].cnt}`);
  console.log(`  HostCard:   ${hostCardCount.result[0].cnt}`);

  // Sample query
  console.log('\nSample card:');
  const sample = await query(dbName, 'SELECT FROM HostCard LIMIT 1');
  if (sample.result[0]) {
    const hostCard = sample.result[0];
    const baseCard = await query(dbName, 'SELECT FROM BaseCard WHERE @rid = :rid', { rid: hostCard.baseCard });
    console.log(`  Text: ${baseCard.result[0]?.text || 'N/A'}`);
    console.log(`  Translation: ${hostCard.translation || 'N/A'}`);
    console.log(`  Explanation (short): ${(hostCard.explanation1 || '').substring(0, 100)}...`);
  }
}

main().catch(console.error);
