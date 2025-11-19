// Test fetching deck with all cards in a single request
import { LanguageCMS } from './language-cms.js';

const cms = new LanguageCMS('language_cms_import');

async function main() {
  console.log('=== Fetching Deck with Cards (Single SQL Query with Nested Projections) ===\n');

  const deckUid = 'kids-en_us-si-sl.00-pre-a1.01-greetings';

  console.time('Fetch time');
  const deck = await cms.getDeckWithCards(deckUid);
  console.timeEnd('Fetch time');

  if (!deck) {
    console.error('Deck not found:', deckUid);
    process.exit(1);
  }

  console.log('\n--- Deck Info ---');
  console.log('Name:', deck.name);
  console.log('UID:', deck.uid);
  console.log('RID:', deck['@rid']);
  console.log('Host Country:', deck.hostCountry);
  console.log('Host Language:', deck.hostLang);
  console.log('Total Cards:', deck.cards.length);

  console.log('\n--- First 5 Cards ---');
  deck.cards.slice(0, 5).forEach((card, i) => {
    console.log(`\n[${i + 1}] ${card.text}`);
    console.log(`    Translation: ${card.translation}`);
    console.log(`    Host Card UID: ${card.uid}`);
    console.log(`    Base Card UID: ${card.baseUid}`);
    if (card.clozeText) {
      console.log(`    Cloze: ${card.clozeText}`);
    }
  });

  console.log('\n--- SQL Query Used (Nested Projections) ---');
  const sqlQuery = `
SELECT
  @rid, uid, name, hostCountry, hostLang,
  (SELECT
    @rid as hostCardRid,
    uid,
    translation,
    explanation1,
    explanation2,
    explanation3,
    baseCard.uid as baseUid,
    baseCard.text as text,
    baseCard.cloze_text as clozeText,
    baseCard.pronunciation as pronunciation,
    baseCard.\`order\` as \`order\`
   FROM HostCard
   WHERE hostDeck = $parent.$current
   ORDER BY baseCard.\`order\`) as cards
FROM HostDeck
WHERE uid = '${deckUid}'
`;
  console.log(sqlQuery);
}

main().catch(console.error);
