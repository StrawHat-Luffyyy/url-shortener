import type { Context } from "hono";
import type { ContentfulStatusCode } from "hono/utils/http-status";

// Every error response uses this envelope: { error: { code, message } }.
export type ErrorCode = "UNAUTHORIZED" | "NOT_FOUND" | "VALIDATION_FAILED" | "INTERNAL";

export class ApiError extends Error {
  constructor(
    readonly status: ContentfulStatusCode,
    readonly code: ErrorCode,
    message: string,
  ) {
    super(message);
  }
}

export function errorResponse(c: Context, status: ContentfulStatusCode, code: ErrorCode, message: string) {
  return c.json({ error: { code, message } }, status);
}
