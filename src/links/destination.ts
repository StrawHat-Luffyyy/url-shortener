export const MAX_URL_LENGTH = 2048;

export type DestinationError = "INVALID_URL" | "UNSUPPORTED_SCHEME" | "URL_HAS_CREDENTIALS" | "URL_TOO_LONG";

export type Destination =
  | { ok: true; url: string; normalized: string }
  | { ok: false; reason: DestinationError };

// Validates a destination URL (FR-1) and normalizes it (FR-7).
// `url` is what visitors are redirected to: the WHATWG serialization, which
// already lowercases scheme/host, drops default ports and punycodes the host.
// `normalized` additionally drops the fragment and sorts query parameters; it is
// used for matching (blocklists, dedup), never as the redirect target.
export function parseDestination(input: string): Destination {
  if (input.length > MAX_URL_LENGTH) return { ok: false, reason: "URL_TOO_LONG" };

  let parsed: URL;
  try {
    parsed = new URL(input);
  } catch {
    return { ok: false, reason: "INVALID_URL" };
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    return { ok: false, reason: "UNSUPPORTED_SCHEME" };
  }
  // `https://paypal.com@evil.example` is a classic phishing disguise.
  if (parsed.username || parsed.password) return { ok: false, reason: "URL_HAS_CREDENTIALS" };

  const url = parsed.href;
  if (url.length > MAX_URL_LENGTH) return { ok: false, reason: "URL_TOO_LONG" };

  parsed.hash = "";
  parsed.search = sortQuery(parsed.search);
  return { ok: true, url, normalized: parsed.href };
}

// Stable sort of the raw `name=value` pairs by name. Working on raw pairs avoids
// URLSearchParams re-encoding values ("+" vs "%20"), and the stable sort keeps
// repeated names in their original relative order, so the meaning is unchanged.
function sortQuery(search: string): string {
  if (search.length <= 1) return search;
  const pairs = search.slice(1).split("&");
  const name = (pair: string) => pair.split("=", 1)[0]!;
  pairs.sort((a, b) => (name(a) < name(b) ? -1 : name(a) > name(b) ? 1 : 0));
  return `?${pairs.join("&")}`;
}
