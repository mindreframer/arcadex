// Migration: initial_schema
// Created: 2024-11-19
// Sets up the base Language CMS schema

export async function up(db, command, query) {
  // --- BASE TYPES ---

  // BaseCourse
  await command(db, 'CREATE DOCUMENT TYPE BaseCourse');
  await command(db, 'CREATE PROPERTY BaseCourse.uid STRING');
  await command(db, 'CREATE PROPERTY BaseCourse.name STRING');
  await command(db, 'CREATE PROPERTY BaseCourse.summary STRING');
  await command(db, 'CREATE PROPERTY BaseCourse.lang STRING');
  await command(db, 'CREATE PROPERTY BaseCourse.version INTEGER');
  await command(db, 'CREATE PROPERTY BaseCourse.createdAt DATETIME');
  await command(db, 'CREATE PROPERTY BaseCourse.updatedAt DATETIME');

  // BaseTrack
  await command(db, 'CREATE DOCUMENT TYPE BaseTrack');
  await command(db, 'CREATE PROPERTY BaseTrack.uid STRING');
  await command(db, 'CREATE PROPERTY BaseTrack.name STRING');
  await command(db, 'CREATE PROPERTY BaseTrack.lang STRING');
  await command(db, 'CREATE PROPERTY BaseTrack.course LINK');
  await command(db, 'CREATE PROPERTY BaseTrack.order INTEGER');
  await command(db, 'CREATE PROPERTY BaseTrack.createdAt DATETIME');
  await command(db, 'CREATE PROPERTY BaseTrack.updatedAt DATETIME');

  // BaseDeck
  await command(db, 'CREATE DOCUMENT TYPE BaseDeck');
  await command(db, 'CREATE PROPERTY BaseDeck.uid STRING');
  await command(db, 'CREATE PROPERTY BaseDeck.name STRING');
  await command(db, 'CREATE PROPERTY BaseDeck.lang STRING');
  await command(db, 'CREATE PROPERTY BaseDeck.track LINK');
  await command(db, 'CREATE PROPERTY BaseDeck.order INTEGER');
  await command(db, 'CREATE PROPERTY BaseDeck.createdAt DATETIME');
  await command(db, 'CREATE PROPERTY BaseDeck.updatedAt DATETIME');

  // BaseCard
  await command(db, 'CREATE DOCUMENT TYPE BaseCard');
  await command(db, 'CREATE PROPERTY BaseCard.uid STRING');
  await command(db, 'CREATE PROPERTY BaseCard.text STRING');
  await command(db, 'CREATE PROPERTY BaseCard.countryAffinity STRING');
  await command(db, 'CREATE PROPERTY BaseCard.deck LINK');
  await command(db, 'CREATE PROPERTY BaseCard.order INTEGER');
  await command(db, 'CREATE PROPERTY BaseCard.pronunciation STRING');
  await command(db, 'CREATE PROPERTY BaseCard.words LIST');
  await command(db, 'CREATE PROPERTY BaseCard.wordTypes LIST');
  await command(db, 'CREATE PROPERTY BaseCard.cloze_text STRING');
  await command(db, 'CREATE PROPERTY BaseCard.createdAt DATETIME');
  await command(db, 'CREATE PROPERTY BaseCard.updatedAt DATETIME');

  // --- HOST TYPES ---

  // HostCourse
  await command(db, 'CREATE DOCUMENT TYPE HostCourse');
  await command(db, 'CREATE PROPERTY HostCourse.uid STRING');
  await command(db, 'CREATE PROPERTY HostCourse.baseCourse LINK');
  await command(db, 'CREATE PROPERTY HostCourse.hostCountry STRING');
  await command(db, 'CREATE PROPERTY HostCourse.hostLang STRING');
  await command(db, 'CREATE PROPERTY HostCourse.name STRING');
  await command(db, 'CREATE PROPERTY HostCourse.summary STRING');
  await command(db, 'CREATE PROPERTY HostCourse.createdAt DATETIME');
  await command(db, 'CREATE PROPERTY HostCourse.updatedAt DATETIME');

  // HostTrack
  await command(db, 'CREATE DOCUMENT TYPE HostTrack');
  await command(db, 'CREATE PROPERTY HostTrack.uid STRING');
  await command(db, 'CREATE PROPERTY HostTrack.baseTrack LINK');
  await command(db, 'CREATE PROPERTY HostTrack.hostCourse LINK');
  await command(db, 'CREATE PROPERTY HostTrack.hostCountry STRING');
  await command(db, 'CREATE PROPERTY HostTrack.hostLang STRING');
  await command(db, 'CREATE PROPERTY HostTrack.name STRING');
  await command(db, 'CREATE PROPERTY HostTrack.createdAt DATETIME');
  await command(db, 'CREATE PROPERTY HostTrack.updatedAt DATETIME');

  // HostDeck
  await command(db, 'CREATE DOCUMENT TYPE HostDeck');
  await command(db, 'CREATE PROPERTY HostDeck.uid STRING');
  await command(db, 'CREATE PROPERTY HostDeck.baseDeck LINK');
  await command(db, 'CREATE PROPERTY HostDeck.hostTrack LINK');
  await command(db, 'CREATE PROPERTY HostDeck.hostCountry STRING');
  await command(db, 'CREATE PROPERTY HostDeck.hostLang STRING');
  await command(db, 'CREATE PROPERTY HostDeck.name STRING');
  await command(db, 'CREATE PROPERTY HostDeck.createdAt DATETIME');
  await command(db, 'CREATE PROPERTY HostDeck.updatedAt DATETIME');

  // HostCard
  await command(db, 'CREATE DOCUMENT TYPE HostCard');
  await command(db, 'CREATE PROPERTY HostCard.uid STRING');
  await command(db, 'CREATE PROPERTY HostCard.baseCard LINK');
  await command(db, 'CREATE PROPERTY HostCard.hostDeck LINK');
  await command(db, 'CREATE PROPERTY HostCard.hostCountry STRING');
  await command(db, 'CREATE PROPERTY HostCard.hostLang STRING');
  await command(db, 'CREATE PROPERTY HostCard.translation STRING');
  await command(db, 'CREATE PROPERTY HostCard.explanation1 STRING');
  await command(db, 'CREATE PROPERTY HostCard.explanation2 STRING');
  await command(db, 'CREATE PROPERTY HostCard.explanation3 STRING');
  await command(db, 'CREATE PROPERTY HostCard.createdAt DATETIME');
  await command(db, 'CREATE PROPERTY HostCard.updatedAt DATETIME');

  // --- INDEXES ---

  // UID indexes (external IDs)
  await command(db, 'CREATE INDEX ON BaseCourse (uid) UNIQUE');
  await command(db, 'CREATE INDEX ON BaseTrack (uid) UNIQUE');
  await command(db, 'CREATE INDEX ON BaseDeck (uid) UNIQUE');
  await command(db, 'CREATE INDEX ON BaseCard (uid) UNIQUE');
  await command(db, 'CREATE INDEX ON HostCourse (uid) UNIQUE');
  await command(db, 'CREATE INDEX ON HostTrack (uid) UNIQUE');
  await command(db, 'CREATE INDEX ON HostDeck (uid) UNIQUE');
  await command(db, 'CREATE INDEX ON HostCard (uid) UNIQUE');

  // Unique constraints
  await command(db, 'CREATE INDEX ON BaseCourse (name, lang, version) UNIQUE');
  await command(db, 'CREATE INDEX ON HostCourse (baseCourse, hostCountry, hostLang) NOTUNIQUE');
  await command(db, 'CREATE INDEX ON HostCard (baseCard, hostCountry, hostLang) NOTUNIQUE');

  // Lookup indexes
  await command(db, 'CREATE INDEX ON BaseTrack (course) NOTUNIQUE');
  await command(db, 'CREATE INDEX ON BaseDeck (track) NOTUNIQUE');
  await command(db, 'CREATE INDEX ON BaseCard (deck) NOTUNIQUE');
  await command(db, 'CREATE INDEX ON HostTrack (hostCourse) NOTUNIQUE');
  await command(db, 'CREATE INDEX ON HostDeck (hostTrack) NOTUNIQUE');
  await command(db, 'CREATE INDEX ON HostCard (hostDeck) NOTUNIQUE');

  console.log('    Created 8 document types with indexes');
}

export async function down(db, command, query) {
  // Drop in reverse order of dependencies
  await command(db, 'DROP TYPE HostCard IF EXISTS');
  await command(db, 'DROP TYPE HostDeck IF EXISTS');
  await command(db, 'DROP TYPE HostTrack IF EXISTS');
  await command(db, 'DROP TYPE HostCourse IF EXISTS');
  await command(db, 'DROP TYPE BaseCard IF EXISTS');
  await command(db, 'DROP TYPE BaseDeck IF EXISTS');
  await command(db, 'DROP TYPE BaseTrack IF EXISTS');
  await command(db, 'DROP TYPE BaseCourse IF EXISTS');

  console.log('    Dropped 8 document types');
}
