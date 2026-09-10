import Fastify from "fastify";
import { describe, expect, it } from "vitest";
import { SESSION_REPLAY_BODY_LIMIT_BYTES } from "./bodyLimit.js";

describe("session replay body limit", () => {
  it("accepts replay payloads larger than the global API limit", async () => {
    const fastify = Fastify({ bodyLimit: 10 * 1024 * 1024 });
    fastify.post("/session-replay/record/:siteId", { bodyLimit: SESSION_REPLAY_BODY_LIMIT_BYTES }, async () => ({
      success: true,
    }));

    const response = await fastify.inject({
      method: "POST",
      url: "/session-replay/record/test-site",
      payload: { events: "x".repeat(11 * 1024 * 1024) },
    });

    expect(response.statusCode).toBe(200);
    expect(SESSION_REPLAY_BODY_LIMIT_BYTES).toBe(100 * 1024 * 1024);
    await fastify.close();
  });
});
