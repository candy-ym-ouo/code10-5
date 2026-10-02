import { describe, expect, it } from "vitest";
import { timeoutBudgetFor, totalBudgetMs } from "../src/lib/media.js";
import { MediaProcessingError, classifyMediaError, failureMessageFor } from "../src/lib/errors.js";

const MB = 1024 * 1024;

describe("timeoutBudgetFor", () => {
  it("gives small files the minimum budget", () => {
    const budget = timeoutBudgetFor(1 * MB);
    expect(budget.downloadMs).toBe(32_000);
    expect(budget.probeMs).toBe(15_500);
    expect(budget.peaksMs).toBe(63_000);
  });

  it("scales every stage with file size", () => {
    const small = timeoutBudgetFor(5 * MB);
    const large = timeoutBudgetFor(100 * MB);
    expect(large.downloadMs).toBeGreaterThan(small.downloadMs);
    expect(large.probeMs).toBeGreaterThan(small.probeMs);
    expect(large.peaksMs).toBeGreaterThan(small.peaksMs);
  });

  it("clamps each stage at its ceiling", () => {
    const budget = timeoutBudgetFor(10 * 1024 * MB);
    expect(budget.downloadMs).toBe(10 * 60_000);
    expect(budget.probeMs).toBe(120_000);
    expect(budget.peaksMs).toBe(15 * 60_000);
  });

  it("never goes below the floor for empty input", () => {
    const budget = timeoutBudgetFor(0n);
    expect(budget.downloadMs).toBe(30_000);
    expect(budget.probeMs).toBe(15_000);
    expect(budget.peaksMs).toBe(60_000);
    expect(totalBudgetMs(budget)).toBe(30_000 + 15_000 + 60_000);
  });
});

describe("classifyMediaError", () => {
  it("passes through already classified errors", () => {
    const original = new MediaProcessingError("NO_AUDIO_STREAM", "no audio", false, "probe");
    expect(classifyMediaError(original)).toBe(original);
  });

  it("treats missing S3 objects as permanent", () => {
    const error = Object.assign(new Error("The specified key does not exist."), { name: "NoSuchKey" });
    const classified = classifyMediaError(error);
    expect(classified.code).toBe("MEDIA_OBJECT_MISSING");
    expect(classified.retryable).toBe(false);
  });

  it("treats corrupt media output as permanent", () => {
    const classified = classifyMediaError(new Error("Invalid data found when processing input"));
    expect(classified.code).toBe("MEDIA_UNREADABLE");
    expect(classified.retryable).toBe(false);
  });

  it("treats moov atom failures as permanent", () => {
    const classified = classifyMediaError(new Error("moov atom not found"));
    expect(classified.code).toBe("MEDIA_UNREADABLE");
    expect(classified.retryable).toBe(false);
  });

  it("defaults unknown errors to retryable", () => {
    const classified = classifyMediaError(new Error("socket hang up"));
    expect(classified.code).toBe("MEDIA_PROBE_FAILED");
    expect(classified.retryable).toBe(true);
  });

  it("classifies non-error values without crashing", () => {
    const classified = classifyMediaError("weird failure");
    expect(classified.retryable).toBe(true);
  });
});

describe("MediaProcessingError timeout codes", () => {
  it("marks staged timeouts as retryable", () => {
    for (const code of ["DOWNLOAD_TIMEOUT", "PROBE_TIMEOUT", "PEAKS_TIMEOUT"] as const) {
      const error = new MediaProcessingError(code, "timeout", true, "probe");
      expect(error.retryable).toBe(true);
      expect(failureMessageFor(error.code)).toBeTruthy();
    }
  });
});

describe("failureMessageFor", () => {
  it("has a user-facing message for every code", () => {
    const codes = [
      "NO_AUDIO_STREAM",
      "INVALID_DURATION",
      "EMPTY_PCM",
      "MEDIA_UNREADABLE",
      "MEDIA_OBJECT_MISSING",
      "MEDIA_DOWNLOAD_FAILED",
      "DOWNLOAD_TIMEOUT",
      "PROBE_TIMEOUT",
      "PEAKS_TIMEOUT",
      "MEDIA_PROBE_FAILED",
    ] as const;
    for (const code of codes) {
      expect(failureMessageFor(code).length).toBeGreaterThan(0);
    }
  });
});
