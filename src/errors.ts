import type { Context } from "hono";
import type { ContentfulStatusCode } from "hono/utils/http-status";
import { z } from "zod";
import type { DestinationError } from "./links/destination";

// Every error response uses this envelope: { error: { code, message } }.
export type ErrorCode =
  | "UNAUTHORIZED"
  | "NOT_FOUND"
  | "VALIDATION_FAILED"
  | "INTERNAL"
  | DestinationError
  | "LINK_GONE"
  | "ALIAS_TAKEN"
  | "ALIAS_RESERVED"
  | "IDEMPOTENCY_KEY_REUSED";

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

export async function parseBody<T extends z.ZodType>(req: Request, schema: T): Promise<z.infer<T>> {
  const parsed = schema.safeParse(await req.json().catch(() => undefined));
  if (!parsed.success) throw new ApiError(400, "VALIDATION_FAILED", z.prettifyError(parsed.error));
  return parsed.data;
}
