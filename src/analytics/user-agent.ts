export type ClickLabel = "human" | "suspected_bot" | "known_crawler";

// Self-identified crawlers, monitors, headless browsers and HTTP libraries.
const KNOWN_CRAWLER =
  /bot\b|bot\/|crawl|spider|slurp|facebookexternalhit|embedly|preview|headless|phantomjs|curl\/|wget\/|python-requests|python-urllib|aiohttp|httpx|go-http-client|java\/|okhttp|axios\/|node-fetch|undici|libwww|scrapy|httpclient/i;

// The cheap rules layer of bot detection (PRD §4.2): it runs first and only
// catches traffic that announces itself. A missing user agent is a strong bot
// signal but not proof, so it is only suspected. The Phase 5 model labels the
// rest; until then everything else counts as human.
export function classifyUserAgent(ua: string | null): { label: ClickLabel; browser: string; device: string } {
  if (!ua) return { label: "suspected_bot", browser: "Unknown", device: "unknown" };
  if (KNOWN_CRAWLER.test(ua)) return { label: "known_crawler", browser: "Bot", device: "bot" };
  return { label: "human", browser: browserFamily(ua), device: deviceType(ua) };
}

function browserFamily(ua: string): string {
  if (/Edg(e|A|iOS)?\//.test(ua)) return "Edge";
  if (/OPR\/|Opera/.test(ua)) return "Opera";
  if (/SamsungBrowser\//.test(ua)) return "Samsung Internet";
  if (/Firefox\/|FxiOS\//.test(ua)) return "Firefox";
  if (/Chrome\/|CriOS\//.test(ua)) return "Chrome";
  if (/Safari\//.test(ua) && /Version\//.test(ua)) return "Safari";
  return "Other";
}

function deviceType(ua: string): string {
  if (/iPad|Tablet/.test(ua) || (/Android/.test(ua) && !/Mobile/.test(ua))) return "tablet";
  if (/Mobi|iPhone|iPod|Android/.test(ua)) return "mobile";
  return "desktop";
}
