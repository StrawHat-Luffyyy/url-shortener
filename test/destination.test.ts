import { describe, expect, it } from "vitest";
import { parseDestination } from "../src/links/destination";

describe("parseDestination", () => {
  it.each([
    ["lowercases scheme and host", "HTTPS://Example.COM/Path", "https://example.com/Path"],
    ["strips the default https port", "https://example.com:443/a", "https://example.com/a"],
    ["strips the default http port", "http://example.com:80/a", "http://example.com/a"],
    ["keeps a non-default port", "https://example.com:8443/a", "https://example.com:8443/a"],
    ["strips the fragment", "https://example.com/a#section", "https://example.com/a"],
    ["converts an IDN host to punycode", "https://bücher.example/", "https://xn--bcher-kva.example/"],
    ["sorts query parameters by name", "https://example.com/?b=2&a=1", "https://example.com/?a=1&b=2"],
    [
      "keeps the order of repeated parameters",
      "https://example.com/?z=0&k=2&a=1&k=1",
      "https://example.com/?a=1&k=2&k=1&z=0",
    ],
    [
      "does not re-encode query values",
      "https://example.com/?q=a+b&p=%2F",
      "https://example.com/?p=%2F&q=a+b",
    ],
    ["adds the root path", "https://example.com", "https://example.com/"],
  ])("%s", (_, input, normalized) => {
    expect(parseDestination(input)).toMatchObject({ ok: true, normalized });
  });

  it("redirect target keeps fragment and query order but is serialized", () => {
    expect(parseDestination("HTTPS://Example.com:443/a?b=2&a=1#frag")).toEqual({
      ok: true,
      url: "https://example.com/a?b=2&a=1#frag",
      normalized: "https://example.com/a?a=1&b=2",
    });
  });

  it.each([
    ["not a URL", "not a url", "INVALID_URL"],
    ["an empty string", "", "INVALID_URL"],
    ["ftp", "ftp://example.com/file", "UNSUPPORTED_SCHEME"],
    ["javascript:", "javascript:alert(1)", "UNSUPPORTED_SCHEME"],
    ["data:", "data:text/html,hi", "UNSUPPORTED_SCHEME"],
    ["embedded credentials", "https://paypal.com@evil.example/", "URL_HAS_CREDENTIALS"],
    ["user and password", "https://u:p@example.com/", "URL_HAS_CREDENTIALS"],
    ["over 2048 characters", `https://example.com/${"a".repeat(2029)}`, "URL_TOO_LONG"],
  ])("rejects %s", (_, input, reason) => {
    expect(parseDestination(input)).toEqual({ ok: false, reason });
  });

  it("accepts exactly 2048 characters", () => {
    const url = `https://example.com/${"a".repeat(2028)}`;
    expect(url).toHaveLength(2048);
    expect(parseDestination(url)).toMatchObject({ ok: true });
  });
});
