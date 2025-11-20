// Migration: add_tts_settings
// Created: 2024-11-19
// Adds TTS settings and audio types

export async function up(db, command, query) {
  // TTSSettings - reusable voice configurations
  await command(db, 'CREATE DOCUMENT TYPE TTSSettings');
  await command(db, 'CREATE PROPERTY TTSSettings.name STRING');
  await command(db, 'CREATE PROPERTY TTSSettings.provider STRING');
  await command(db, 'CREATE PROPERTY TTSSettings.engine STRING');
  await command(db, 'CREATE PROPERTY TTSSettings.voice STRING');
  await command(db, 'CREATE PROPERTY TTSSettings.options MAP');
  await command(db, 'CREATE PROPERTY TTSSettings.createdAt DATETIME');

  // TTSAudio - points to BaseCard and TTSSettings
  await command(db, 'CREATE DOCUMENT TYPE TTSAudio');
  await command(db, 'CREATE PROPERTY TTSAudio.baseCard LINK');
  await command(db, 'CREATE PROPERTY TTSAudio.settings LINK');
  await command(db, 'CREATE PROPERTY TTSAudio.fileUrl STRING');
  await command(db, 'CREATE PROPERTY TTSAudio.duration FLOAT');
  await command(db, 'CREATE PROPERTY TTSAudio.createdAt DATETIME');

  // Indexes
  await command(db, 'CREATE INDEX ON TTSSettings (name) UNIQUE');
  await command(db, 'CREATE INDEX ON TTSAudio (baseCard) NOTUNIQUE');
  await command(db, 'CREATE INDEX ON TTSAudio (settings) NOTUNIQUE');

  console.log('    Created TTSSettings and TTSAudio types');
}

export async function down(db, command, query) {
  await command(db, 'DROP TYPE TTSAudio IF EXISTS');
  await command(db, 'DROP TYPE TTSSettings IF EXISTS');

  console.log('    Dropped TTSSettings and TTSAudio types');
}
