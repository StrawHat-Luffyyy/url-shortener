const ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
export const CODE_LENGTH = 7;
const SPACE = 62n ** BigInt(CODE_LENGTH); // ≈ 3.5 trillion codes

// Coprime with 62^7 (= 2^7 · 31^7), so id ↦ id·MULTIPLIER mod 62^7 is a
// bijection: distinct ids always give distinct codes. Chosen near 0.618·62^7 so
// consecutive ids land far apart and codes don't reveal the sequence. This hides
// the order from casual enumeration; it is not encryption.
const MULTIPLIER = 2176477521739n;

export function codeForId(id: number): string {
  if (!Number.isSafeInteger(id) || id < 0 || BigInt(id) >= SPACE) throw new RangeError(`id out of range: ${id}`);
  let n = (BigInt(id) * MULTIPLIER) % SPACE;
  let code = "";
  for (let i = 0; i < CODE_LENGTH; i++) {
    code = ALPHABET[Number(n % 62n)]! + code;
    n /= 62n;
  }
  return code;
}
