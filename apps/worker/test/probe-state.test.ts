import { describe, expect, it } from "vitest";
import { decideClaim, decideRetry, type ClaimRepository } from "../src/lib/probe-state.js";
import { permanentError, retryableError } from "../src/lib/media-errors.js";

function repoOf(status: string | null, updatedAt = new Date(), claimResult = true): ClaimRepository & { claimFrom: string[] | null } {
  const state = {
    claimFrom: null as string[] | null,
    async findStatus() {
      return status === null ? null : { status, updatedAt };
    },
    async claim(_id: string, from: string[]) {
      this.claimFrom = from;
      return claimResult;
    },
  };
  return state;
}

describe("decideClaim", () => {
  const staleMs = 10 * 60_000;

  it("claims UPLOADED media", async () => {
    const repo = repoOf("UPLOADED");
    const verdict = await decideClaim(repo, "m1", staleMs);
    expect(verdict).toEqual({ ok: true, reason: "claimed" });
    expect(repo.claimFrom).toEqual(["UPLOADED", "FAILED", "PROCESSING"]);
  });

  it("claims FAILED media (manual retry)", async () => {
    const verdict = await decideClaim(repoOf("FAILED"), "m1", staleMs);
    expect(verdict.ok).toBe(true);
  });

  it("takes over stale PROCESSING (crashed worker)", async () => {
    const old = new Date(Date.now() - staleMs - 1_000);
    const verdict = await decideClaim(repoOf("PROCESSING", old), "m1", staleMs);
    expect(verdict).toEqual({ ok: true, reason: "claimed" });
  });

  it("skips fresh PROCESSING (another live worker)", async () => {
    const verdict = await decideClaim(repoOf("PROCESSING"), "m1", staleMs);
    expect(verdict).toEqual({ ok: false, reason: "already-processing" });
  });

  it("skips READY / CANCELLED idempotently", async () => {
    expect(await decideClaim(repoOf("READY"), "m1", staleMs)).toEqual({ ok: false, reason: "already-terminal" });
    expect(await decideClaim(repoOf("CANCELLED"), "m1", staleMs)).toEqual({ ok: false, reason: "already-terminal" });
  });

  it("skips missing media", async () => {
    expect(await decideClaim(repoOf(null), "m1", staleMs)).toEqual({ ok: false, reason: "missing" });
  });

  it("skips non-processable states like PENDING_UPLOAD", async () => {
    expect(await decideClaim(repoOf("PENDING_UPLOAD"), "m1", staleMs)).toEqual({
      ok: false,
      reason: "not-processable",
    });
  });

  it("reports claim-lost on conditional-update race", async () => {
    const verdict = await decideClaim(repoOf("UPLOADED", new Date(), false), "m1", staleMs);
    expect(verdict).toEqual({ ok: false, reason: "claim-lost" });
  });
});

describe("decideRetry", () => {
  it("retries a retryable error while budget remains", () => {
    const decision = decideRetry(retryableError("PROBE_TIMEOUT", "probe"), 0, 4);
    expect(decision.retry).toBe(true);
    expect(decision.permanent).toBe(false);
  });

  it("stops retrying after the last attempt", () => {
    const decision = decideRetry(retryableError("PROBE_TIMEOUT", "probe"), 3, 4);
    expect(decision.retry).toBe(false);
    expect(decision.permanent).toBe(false);
    expect(decision.failure.spec.code).toBe("PROBE_TIMEOUT");
  });

  it("never retries permanent errors regardless of budget", () => {
    const decision = decideRetry(permanentError("NO_AUDIO_STREAM", "probe"), 0, 4);
    expect(decision.retry).toBe(false);
    expect(decision.permanent).toBe(true);
  });

  it("classifies a raw error as retryable by default", () => {
    const decision = decideRetry(new Error("boom"), 0, 4, "probe");
    expect(decision.retry).toBe(true);
    expect(decision.failure.retryable).toBe(true);
  });
});
