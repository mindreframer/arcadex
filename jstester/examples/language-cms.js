// Language Learning CMS Data Model
// Uses Document types with LINK properties for hierarchy

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
  return res.json();
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
  return res.json();
}

async function serverCommand(cmd) {
  const res = await fetch(`${BASE_URL}/api/v1/server`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': AUTH
    },
    body: JSON.stringify({ command: cmd })
  });
  return res.json();
}

// ============================================================================
// Schema Setup
// ============================================================================

async function createSchema(db) {
  // --- BASE TYPES ---

  // BaseCourse
  await command(db, 'CREATE DOCUMENT TYPE BaseCourse IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseCourse.uid STRING IF NOT EXISTS'); // external ID
  await command(db, 'CREATE PROPERTY BaseCourse.name STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseCourse.summary STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseCourse.lang STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseCourse.version INTEGER IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseCourse.createdAt DATETIME IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseCourse.updatedAt DATETIME IF NOT EXISTS');

  // BaseTrack
  await command(db, 'CREATE DOCUMENT TYPE BaseTrack IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseTrack.uid STRING IF NOT EXISTS'); // external ID
  await command(db, 'CREATE PROPERTY BaseTrack.name STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseTrack.lang STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseTrack.course LINK IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseTrack.order INTEGER IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseTrack.createdAt DATETIME IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseTrack.updatedAt DATETIME IF NOT EXISTS');

  // BaseDeck
  await command(db, 'CREATE DOCUMENT TYPE BaseDeck IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseDeck.uid STRING IF NOT EXISTS'); // external ID
  await command(db, 'CREATE PROPERTY BaseDeck.name STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseDeck.lang STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseDeck.track LINK IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseDeck.order INTEGER IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseDeck.createdAt DATETIME IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseDeck.updatedAt DATETIME IF NOT EXISTS');

  // BaseCard
  await command(db, 'CREATE DOCUMENT TYPE BaseCard IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseCard.uid STRING IF NOT EXISTS'); // external ID
  await command(db, 'CREATE PROPERTY BaseCard.text STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseCard.countryAffinity STRING IF NOT EXISTS'); // null = all countries
  await command(db, 'CREATE PROPERTY BaseCard.deck LINK IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseCard.order INTEGER IF NOT EXISTS');
  // Agent-added fields
  await command(db, 'CREATE PROPERTY BaseCard.pronunciation STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseCard.words LIST IF NOT EXISTS'); // list of words
  await command(db, 'CREATE PROPERTY BaseCard.wordTypes LIST IF NOT EXISTS'); // list of types for each word
  await command(db, 'CREATE PROPERTY BaseCard.createdAt DATETIME IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY BaseCard.updatedAt DATETIME IF NOT EXISTS');

  // --- HOST TYPES ---

  // HostCourse
  await command(db, 'CREATE DOCUMENT TYPE HostCourse IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostCourse.uid STRING IF NOT EXISTS'); // external ID
  await command(db, 'CREATE PROPERTY HostCourse.baseCourse LINK IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostCourse.hostCountry STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostCourse.hostLang STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostCourse.name STRING IF NOT EXISTS'); // translated
  await command(db, 'CREATE PROPERTY HostCourse.summary STRING IF NOT EXISTS'); // translated
  await command(db, 'CREATE PROPERTY HostCourse.createdAt DATETIME IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostCourse.updatedAt DATETIME IF NOT EXISTS');

  // HostTrack
  await command(db, 'CREATE DOCUMENT TYPE HostTrack IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostTrack.uid STRING IF NOT EXISTS'); // external ID
  await command(db, 'CREATE PROPERTY HostTrack.baseTrack LINK IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostTrack.hostCourse LINK IF NOT EXISTS'); // parent host course
  await command(db, 'CREATE PROPERTY HostTrack.hostCountry STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostTrack.hostLang STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostTrack.name STRING IF NOT EXISTS'); // translated
  await command(db, 'CREATE PROPERTY HostTrack.createdAt DATETIME IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostTrack.updatedAt DATETIME IF NOT EXISTS');

  // HostDeck
  await command(db, 'CREATE DOCUMENT TYPE HostDeck IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostDeck.uid STRING IF NOT EXISTS'); // external ID
  await command(db, 'CREATE PROPERTY HostDeck.baseDeck LINK IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostDeck.hostTrack LINK IF NOT EXISTS'); // parent host track
  await command(db, 'CREATE PROPERTY HostDeck.hostCountry STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostDeck.hostLang STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostDeck.name STRING IF NOT EXISTS'); // translated
  await command(db, 'CREATE PROPERTY HostDeck.createdAt DATETIME IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostDeck.updatedAt DATETIME IF NOT EXISTS');

  // HostCard
  await command(db, 'CREATE DOCUMENT TYPE HostCard IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostCard.uid STRING IF NOT EXISTS'); // external ID
  await command(db, 'CREATE PROPERTY HostCard.baseCard LINK IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostCard.hostDeck LINK IF NOT EXISTS'); // parent host deck
  await command(db, 'CREATE PROPERTY HostCard.hostCountry STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostCard.hostLang STRING IF NOT EXISTS');
  // Agent-added fields
  await command(db, 'CREATE PROPERTY HostCard.translation STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostCard.explanation1 STRING IF NOT EXISTS'); // level 1
  await command(db, 'CREATE PROPERTY HostCard.explanation2 STRING IF NOT EXISTS'); // level 2
  await command(db, 'CREATE PROPERTY HostCard.explanation3 STRING IF NOT EXISTS'); // level 3
  await command(db, 'CREATE PROPERTY HostCard.createdAt DATETIME IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY HostCard.updatedAt DATETIME IF NOT EXISTS');

  // --- TTS SETTINGS ---

  // TTSSettings - reusable voice configurations
  await command(db, 'CREATE DOCUMENT TYPE TTSSettings IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY TTSSettings.name STRING IF NOT EXISTS'); // friendly name
  await command(db, 'CREATE PROPERTY TTSSettings.provider STRING IF NOT EXISTS'); // google, aws, azure, elevenlabs
  await command(db, 'CREATE PROPERTY TTSSettings.engine STRING IF NOT EXISTS'); // standard, neural, wavenet
  await command(db, 'CREATE PROPERTY TTSSettings.voice STRING IF NOT EXISTS'); // provider-specific voice ID
  await command(db, 'CREATE PROPERTY TTSSettings.options MAP IF NOT EXISTS'); // provider-specific options
  await command(db, 'CREATE PROPERTY TTSSettings.createdAt DATETIME IF NOT EXISTS');

  // --- TTS AUDIO ---

  // TTSAudio - points to BaseCard and TTSSettings
  await command(db, 'CREATE DOCUMENT TYPE TTSAudio IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY TTSAudio.baseCard LINK IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY TTSAudio.settings LINK IF NOT EXISTS'); // link to TTSSettings
  await command(db, 'CREATE PROPERTY TTSAudio.fileUrl STRING IF NOT EXISTS');
  await command(db, 'CREATE PROPERTY TTSAudio.duration FLOAT IF NOT EXISTS'); // in seconds
  await command(db, 'CREATE PROPERTY TTSAudio.createdAt DATETIME IF NOT EXISTS');

  // --- INDEXES ---

  // UID indexes (external IDs)
  await command(db, 'CREATE INDEX IF NOT EXISTS ON BaseCourse (uid) UNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON BaseTrack (uid) UNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON BaseDeck (uid) UNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON BaseCard (uid) UNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON HostCourse (uid) UNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON HostTrack (uid) UNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON HostDeck (uid) UNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON HostCard (uid) UNIQUE');

  // Unique constraints
  await command(db, 'CREATE INDEX IF NOT EXISTS ON BaseCourse (name, lang, version) UNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON HostCourse (baseCourse, hostCountry, hostLang) NOTUNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON HostCard (baseCard, hostCountry, hostLang) NOTUNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON TTSSettings (name) UNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON TTSAudio (baseCard) NOTUNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON TTSAudio (settings) NOTUNIQUE');

  // Lookup indexes
  await command(db, 'CREATE INDEX IF NOT EXISTS ON BaseTrack (course) NOTUNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON BaseDeck (track) NOTUNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON BaseCard (deck) NOTUNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON HostTrack (hostCourse) NOTUNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON HostDeck (hostTrack) NOTUNIQUE');
  await command(db, 'CREATE INDEX IF NOT EXISTS ON HostCard (hostDeck) NOTUNIQUE');

  console.log('Schema created successfully');
}

// ============================================================================
// CMS Operations
// ============================================================================

class LanguageCMS {
  constructor(database) {
    this.db = database;
  }

  // --- BASE COURSE ---

  async createBaseCourse({ name, summary, lang, version = 1 }) {
    const result = await command(this.db, `
      INSERT INTO BaseCourse SET
        name = :name,
        summary = :summary,
        lang = :lang,
        version = :version,
        createdAt = sysdate(),
        updatedAt = sysdate()
    `, { name, summary, lang, version });
    return result.result[0];
  }

  async getBaseCourse(rid) {
    const result = await query(this.db, 'SELECT FROM BaseCourse WHERE @rid = :rid', { rid });
    return result.result[0];
  }

  async updateBaseCourse(rid, fields) {
    const sets = Object.keys(fields).map(k => `${k} = :${k}`).join(', ');
    const result = await command(this.db, `
      UPDATE BaseCourse SET ${sets}, updatedAt = sysdate() WHERE @rid = :rid
    `, { ...fields, rid });
    return result.result[0];
  }

  async listBaseCourses(lang = null) {
    if (lang) {
      const result = await query(this.db, 'SELECT FROM BaseCourse WHERE lang = :lang ORDER BY name', { lang });
      return result.result;
    }
    const result = await query(this.db, 'SELECT FROM BaseCourse ORDER BY name');
    return result.result;
  }

  // --- BASE TRACK ---

  async createBaseTrack({ name, lang, courseRid, order = 0 }) {
    const result = await command(this.db, `
      INSERT INTO BaseTrack SET
        name = :name,
        lang = :lang,
        course = :courseRid,
        \`order\` = :order,
        createdAt = sysdate(),
        updatedAt = sysdate()
    `, { name, lang, courseRid, order });
    return result.result[0];
  }

  async getTracksForCourse(courseRid) {
    const result = await query(this.db, `
      SELECT FROM BaseTrack WHERE course = :courseRid ORDER BY \`order\`
    `, { courseRid });
    return result.result;
  }

  // --- BASE DECK ---

  async createBaseDeck({ name, lang, trackRid, order = 0 }) {
    const result = await command(this.db, `
      INSERT INTO BaseDeck SET
        name = :name,
        lang = :lang,
        track = :trackRid,
        \`order\` = :order,
        createdAt = sysdate(),
        updatedAt = sysdate()
    `, { name, lang, trackRid, order });
    return result.result[0];
  }

  async getDecksForTrack(trackRid) {
    const result = await query(this.db, `
      SELECT FROM BaseDeck WHERE track = :trackRid ORDER BY \`order\`
    `, { trackRid });
    return result.result;
  }

  // --- BASE CARD ---

  async createBaseCard({ text, deckRid, countryAffinity = null, order = 0 }) {
    const result = await command(this.db, `
      INSERT INTO BaseCard SET
        text = :text,
        deck = :deckRid,
        countryAffinity = :countryAffinity,
        \`order\` = :order,
        createdAt = sysdate(),
        updatedAt = sysdate()
    `, { text, deckRid, countryAffinity, order });
    return result.result[0];
  }

  async updateBaseCardWithAgentData(cardRid, { pronunciation, words, wordTypes }) {
    const result = await command(this.db, `
      UPDATE BaseCard SET
        pronunciation = :pronunciation,
        words = :words,
        wordTypes = :wordTypes,
        updatedAt = sysdate()
      WHERE @rid = :cardRid
    `, { cardRid, pronunciation, words, wordTypes });
    return result.result[0];
  }

  async getCardsForDeck(deckRid, countryAffinity = null) {
    if (countryAffinity) {
      const result = await query(this.db, `
        SELECT FROM BaseCard
        WHERE deck = :deckRid AND (countryAffinity IS NULL OR countryAffinity = :countryAffinity)
        ORDER BY \`order\`
      `, { deckRid, countryAffinity });
      return result.result;
    }
    const result = await query(this.db, `
      SELECT FROM BaseCard WHERE deck = :deckRid ORDER BY \`order\`
    `, { deckRid });
    return result.result;
  }

  // --- HOST COURSE ---

  async createHostCourse({ baseCourseRid, hostCountry, hostLang, name, summary }) {
    const result = await command(this.db, `
      INSERT INTO HostCourse SET
        baseCourse = :baseCourseRid,
        hostCountry = :hostCountry,
        hostLang = :hostLang,
        name = :name,
        summary = :summary,
        createdAt = sysdate(),
        updatedAt = sysdate()
    `, { baseCourseRid, hostCountry, hostLang, name, summary });
    return result.result[0];
  }

  async getHostCoursesForBase(baseCourseRid) {
    const result = await query(this.db, `
      SELECT FROM HostCourse WHERE baseCourse = :baseCourseRid
    `, { baseCourseRid });
    return result.result;
  }

  async getHostCourse(baseCourseRid, hostCountry, hostLang) {
    const result = await query(this.db, `
      SELECT FROM HostCourse
      WHERE baseCourse = :baseCourseRid AND hostCountry = :hostCountry AND hostLang = :hostLang
    `, { baseCourseRid, hostCountry, hostLang });
    return result.result[0];
  }

  // --- HOST TRACK ---

  async createHostTrack({ baseTrackRid, hostCourseRid, hostCountry, hostLang, name }) {
    const result = await command(this.db, `
      INSERT INTO HostTrack SET
        baseTrack = :baseTrackRid,
        hostCourse = :hostCourseRid,
        hostCountry = :hostCountry,
        hostLang = :hostLang,
        name = :name,
        createdAt = sysdate(),
        updatedAt = sysdate()
    `, { baseTrackRid, hostCourseRid, hostCountry, hostLang, name });
    return result.result[0];
  }

  async getHostTracksForCourse(hostCourseRid) {
    const result = await query(this.db, `
      SELECT FROM HostTrack WHERE hostCourse = :hostCourseRid ORDER BY baseTrack.\`order\`
    `, { hostCourseRid });
    return result.result;
  }

  // --- HOST DECK ---

  async createHostDeck({ baseDeckRid, hostTrackRid, hostCountry, hostLang, name }) {
    const result = await command(this.db, `
      INSERT INTO HostDeck SET
        baseDeck = :baseDeckRid,
        hostTrack = :hostTrackRid,
        hostCountry = :hostCountry,
        hostLang = :hostLang,
        name = :name,
        createdAt = sysdate(),
        updatedAt = sysdate()
    `, { baseDeckRid, hostTrackRid, hostCountry, hostLang, name });
    return result.result[0];
  }

  async getHostDecksForTrack(hostTrackRid) {
    const result = await query(this.db, `
      SELECT FROM HostDeck WHERE hostTrack = :hostTrackRid ORDER BY baseDeck.\`order\`
    `, { hostTrackRid });
    return result.result;
  }

  // --- HOST CARD ---

  async createHostCard({ baseCardRid, hostDeckRid, hostCountry, hostLang }) {
    const result = await command(this.db, `
      INSERT INTO HostCard SET
        baseCard = :baseCardRid,
        hostDeck = :hostDeckRid,
        hostCountry = :hostCountry,
        hostLang = :hostLang,
        createdAt = sysdate(),
        updatedAt = sysdate()
    `, { baseCardRid, hostDeckRid, hostCountry, hostLang });
    return result.result[0];
  }

  async updateHostCardWithAgentData(cardRid, { translation, explanation1, explanation2, explanation3 }) {
    const result = await command(this.db, `
      UPDATE HostCard SET
        translation = :translation,
        explanation1 = :explanation1,
        explanation2 = :explanation2,
        explanation3 = :explanation3,
        updatedAt = sysdate()
      WHERE @rid = :cardRid
    `, { cardRid, translation, explanation1, explanation2, explanation3 });
    return result.result[0];
  }

  async getHostCardsForDeck(hostDeckRid) {
    const result = await query(this.db, `
      SELECT FROM HostCard WHERE hostDeck = :hostDeckRid ORDER BY baseCard.\`order\`
    `, { hostDeckRid });
    return result.result;
  }

  async getHostCardWithBase(hostCardRid) {
    // Get host card with expanded base card data
    const result = await query(this.db, `
      SELECT *, baseCard.text as baseText, baseCard.pronunciation, baseCard.words, baseCard.wordTypes
      FROM HostCard WHERE @rid = :hostCardRid
    `, { hostCardRid });
    return result.result[0];
  }

  // --- TTS SETTINGS ---

  async createTTSSettings({ name, provider, engine, voice, options = {} }) {
    const result = await command(this.db, `
      INSERT INTO TTSSettings SET
        name = :name,
        provider = :provider,
        engine = :engine,
        voice = :voice,
        options = :options,
        createdAt = sysdate()
    `, { name, provider, engine, voice, options });
    return result.result[0];
  }

  async getTTSSettings(name) {
    const result = await query(this.db, `
      SELECT FROM TTSSettings WHERE name = :name
    `, { name });
    return result.result[0];
  }

  async listTTSSettings() {
    const result = await query(this.db, 'SELECT FROM TTSSettings ORDER BY name');
    return result.result;
  }

  async updateTTSSettings(rid, fields) {
    const sets = Object.keys(fields).map(k => `${k} = :${k}`).join(', ');
    const result = await command(this.db, `
      UPDATE TTSSettings SET ${sets} WHERE @rid = :rid
    `, { ...fields, rid });
    return result.result[0];
  }

  // --- TTS AUDIO ---

  async createTTSAudio({ baseCardRid, settingsRid, fileUrl, duration }) {
    const result = await command(this.db, `
      INSERT INTO TTSAudio SET
        baseCard = :baseCardRid,
        settings = :settingsRid,
        fileUrl = :fileUrl,
        duration = :duration,
        createdAt = sysdate()
    `, { baseCardRid, settingsRid, fileUrl, duration });
    return result.result[0];
  }

  async getTTSForCard(baseCardRid) {
    const result = await query(this.db, `
      SELECT FROM TTSAudio WHERE baseCard = :baseCardRid
    `, { baseCardRid });

    // Expand settings for each audio
    const audios = [];
    for (const audio of result.result) {
      const settingsResult = await query(this.db, `
        SELECT FROM TTSSettings WHERE @rid = :settingsRid
      `, { settingsRid: audio.settings });

      audios.push({
        ...audio,
        settingsData: settingsResult.result[0] || null
      });
    }
    return audios;
  }

  async getTTSBySettings(settingsRid) {
    const result = await query(this.db, `
      SELECT FROM TTSAudio WHERE settings = :settingsRid
    `, { settingsRid });
    return result.result;
  }

  // --- BULK OPERATIONS ---

  async createHostHierarchy(baseCourseRid, hostCountry, hostLang, translations) {
    // Create complete host hierarchy from base course
    // translations = { courseName, courseSummary, tracks: [{ name, decks: [{ name }] }] }

    const baseCourse = await this.getBaseCourse(baseCourseRid);
    if (!baseCourse) throw new Error('Base course not found');

    // Create host course
    const hostCourse = await this.createHostCourse({
      baseCourseRid,
      hostCountry,
      hostLang,
      name: translations.courseName,
      summary: translations.courseSummary
    });

    // Get base tracks
    const baseTracks = await this.getTracksForCourse(baseCourseRid);

    for (let i = 0; i < baseTracks.length; i++) {
      const baseTrack = baseTracks[i];
      const trackTranslation = translations.tracks?.[i] || {};

      // Create host track
      const hostTrack = await this.createHostTrack({
        baseTrackRid: baseTrack['@rid'],
        hostCourseRid: hostCourse['@rid'],
        hostCountry,
        hostLang,
        name: trackTranslation.name || baseTrack.name
      });

      // Get base decks
      const baseDecks = await this.getDecksForTrack(baseTrack['@rid']);

      for (let j = 0; j < baseDecks.length; j++) {
        const baseDeck = baseDecks[j];
        const deckTranslation = trackTranslation.decks?.[j] || {};

        // Create host deck
        const hostDeck = await this.createHostDeck({
          baseDeckRid: baseDeck['@rid'],
          hostTrackRid: hostTrack['@rid'],
          hostCountry,
          hostLang,
          name: deckTranslation.name || baseDeck.name
        });

        // Get base cards (filtered by country affinity)
        const baseCards = await this.getCardsForDeck(baseDeck['@rid'], hostCountry);

        for (const baseCard of baseCards) {
          // Create host card
          await this.createHostCard({
            baseCardRid: baseCard['@rid'],
            hostDeckRid: hostDeck['@rid'],
            hostCountry,
            hostLang
          });
        }
      }
    }

    return hostCourse;
  }

  // --- QUERIES ---

  async getFullCardData(hostCardRid) {
    // Get complete card data including base card, TTS, and translations
    // First get host card
    const hostResult = await query(this.db, `
      SELECT FROM HostCard WHERE @rid = :hostCardRid
    `, { hostCardRid });

    const hostCard = hostResult.result[0];
    if (!hostCard) return null;

    // Get base card data
    const baseResult = await query(this.db, `
      SELECT FROM BaseCard WHERE @rid = :baseCardRid
    `, { baseCardRid: hostCard.baseCard });

    const baseCard = baseResult.result[0];

    // Get TTS audio files
    const tts = await this.getTTSForCard(hostCard.baseCard);

    return {
      hostCardRid: hostCard['@rid'],
      hostCountry: hostCard.hostCountry,
      hostLang: hostCard.hostLang,
      translation: hostCard.translation,
      explanation1: hostCard.explanation1,
      explanation2: hostCard.explanation2,
      explanation3: hostCard.explanation3,
      baseCardRid: baseCard?.['@rid'] || null,
      text: baseCard?.text || null,
      pronunciation: baseCard?.pronunciation || null,
      words: baseCard?.words || null,
      wordTypes: baseCard?.wordTypes || null,
      audio: tts
    };
  }

  async searchCards(hostLang, searchText) {
    // Get all host cards for the language
    const hostResult = await query(this.db, `
      SELECT FROM HostCard WHERE hostLang = :hostLang
    `, { hostLang });

    const results = [];
    for (const hostCard of hostResult.result) {
      // Get base card text
      const baseResult = await query(this.db, `
        SELECT text FROM BaseCard WHERE @rid = :baseCardRid
      `, { baseCardRid: hostCard.baseCard });

      const baseText = baseResult.result[0]?.text || '';
      const translation = hostCard.translation || '';

      // Filter by search text
      if (translation.toLowerCase().includes(searchText.toLowerCase()) ||
          baseText.toLowerCase().includes(searchText.toLowerCase())) {
        results.push({
          hostCardRid: hostCard['@rid'],
          translation: hostCard.translation,
          text: baseText
        });
      }
    }

    return results;
  }
}

// ============================================================================
// Database Management
// ============================================================================

async function createDatabase(name) {
  return serverCommand(`create database ${name}`);
}

async function dropDatabase(name) {
  return serverCommand(`drop database ${name}`);
}

async function databaseExists(name) {
  const res = await fetch(`${BASE_URL}/api/v1/exists/${name}`, {
    headers: { 'Authorization': AUTH }
  });
  const data = await res.json();
  return data.result;
}

// ============================================================================
// Exports
// ============================================================================

export {
  LanguageCMS,
  createSchema,
  createDatabase,
  dropDatabase,
  databaseExists,
  command,
  query
};

// ============================================================================
// Example Usage (run with: bun run language-cms.js)
// ============================================================================

async function main() {
  const dbName = 'language_cms_demo';

  // Setup
  console.log('Setting up database...');
  if (await databaseExists(dbName)) {
    await dropDatabase(dbName);
  }
  await createDatabase(dbName);
  await createSchema(dbName);

  const cms = new LanguageCMS(dbName);

  // Create base content (Spanish course)
  console.log('\nCreating base course...');
  const baseCourse = await cms.createBaseCourse({
    name: 'Spanish Basics',
    summary: 'Learn fundamental Spanish vocabulary and phrases',
    lang: 'es',
    version: 1
  });
  console.log('Created base course:', baseCourse['@rid']);

  // Create track
  const baseTrack = await cms.createBaseTrack({
    name: 'Greetings',
    lang: 'es',
    courseRid: baseCourse['@rid'],
    order: 1
  });
  console.log('Created base track:', baseTrack['@rid']);

  // Create deck
  const baseDeck = await cms.createBaseDeck({
    name: 'Common Greetings',
    lang: 'es',
    trackRid: baseTrack['@rid'],
    order: 1
  });
  console.log('Created base deck:', baseDeck['@rid']);

  // Create cards
  const card1 = await cms.createBaseCard({
    text: 'Hola',
    deckRid: baseDeck['@rid'],
    order: 1
  });
  const card2 = await cms.createBaseCard({
    text: 'Buenos días',
    deckRid: baseDeck['@rid'],
    order: 2
  });
  const card3 = await cms.createBaseCard({
    text: '¿Cómo estás?',
    deckRid: baseDeck['@rid'],
    countryAffinity: 'ES', // Spain only
    order: 3
  });
  console.log('Created base cards:', card1['@rid'], card2['@rid'], card3['@rid']);

  // Add agent data to base cards
  await cms.updateBaseCardWithAgentData(card1['@rid'], {
    pronunciation: 'OH-lah',
    words: ['Hola'],
    wordTypes: ['interjection']
  });
  await cms.updateBaseCardWithAgentData(card2['@rid'], {
    pronunciation: 'BWEH-nohs DEE-ahs',
    words: ['Buenos', 'días'],
    wordTypes: ['adjective', 'noun']
  });

  // Create TTS settings (reusable voice configurations)
  const mariaTTS = await cms.createTTSSettings({
    name: 'Maria - Spanish Female',
    provider: 'google',
    engine: 'wavenet',
    voice: 'es-ES-Wavenet-A',
    options: {
      speakingRate: 0.9,
      pitch: 0,
      audioEncoding: 'MP3'
    }
  });

  const carlosTTS = await cms.createTTSSettings({
    name: 'Carlos - Spanish Male',
    provider: 'google',
    engine: 'wavenet',
    voice: 'es-ES-Wavenet-B',
    options: {
      speakingRate: 1.0,
      pitch: -2,
      audioEncoding: 'MP3'
    }
  });
  console.log('Created TTS settings:', mariaTTS['@rid'], carlosTTS['@rid']);

  // Add TTS audio using settings
  await cms.createTTSAudio({
    baseCardRid: card1['@rid'],
    settingsRid: mariaTTS['@rid'],
    fileUrl: 'https://storage.example.com/tts/hola-maria.mp3',
    duration: 0.8
  });
  await cms.createTTSAudio({
    baseCardRid: card1['@rid'],
    settingsRid: carlosTTS['@rid'],
    fileUrl: 'https://storage.example.com/tts/hola-carlos.mp3',
    duration: 0.9
  });
  console.log('Added TTS audio');

  // Create host content (English translations for US)
  console.log('\nCreating host course for US English...');
  const hostCourse = await cms.createHostHierarchy(
    baseCourse['@rid'],
    'US',
    'en',
    {
      courseName: 'Spanish Basics',
      courseSummary: 'Learn fundamental Spanish vocabulary and phrases',
      tracks: [{
        name: 'Greetings',
        decks: [{
          name: 'Common Greetings'
        }]
      }]
    }
  );
  console.log('Created host course:', hostCourse['@rid']);

  // Get host cards and add translations
  const hostTracks = await cms.getHostTracksForCourse(hostCourse['@rid']);
  const hostDecks = await cms.getHostDecksForTrack(hostTracks[0]['@rid']);
  const hostCards = await cms.getHostCardsForDeck(hostDecks[0]['@rid']);

  console.log(`\nFound ${hostCards.length} host cards (card3 excluded due to country affinity)`);

  // Add agent translations
  for (const hostCard of hostCards) {
    const baseCard = await cms.getCardsForDeck(baseDeck['@rid']).then(
      cards => cards.find(c => c['@rid'] === hostCard.baseCard)
    );

    if (baseCard.text === 'Hola') {
      await cms.updateHostCardWithAgentData(hostCard['@rid'], {
        translation: 'Hello',
        explanation1: 'A common greeting',
        explanation2: 'Used any time of day to greet someone',
        explanation3: 'The most basic and universal Spanish greeting, appropriate in both formal and informal situations'
      });
    } else if (baseCard.text === 'Buenos días') {
      await cms.updateHostCardWithAgentData(hostCard['@rid'], {
        translation: 'Good morning',
        explanation1: 'Morning greeting',
        explanation2: 'Used from dawn until noon',
        explanation3: 'A polite greeting used in the morning hours, literally meaning "good days"'
      });
    }
  }
  console.log('Added translations to host cards');

  // Query full card data
  console.log('\n--- Full Card Data ---');
  const fullCard = await cms.getFullCardData(hostCards[0]['@rid']);
  console.log(JSON.stringify(fullCard, null, 2));

  // Search cards
  console.log('\n--- Search Results for "morning" ---');
  const searchResults = await cms.searchCards('en', 'morning');
  console.log(searchResults);

  console.log('\nDemo complete!');
}

// Run if executed directly
if (import.meta.main) {
  main().catch(console.error);
}
