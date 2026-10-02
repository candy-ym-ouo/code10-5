import { describe, expect, it } from "vitest";
import {
  classifyError,
  isMediaProcessError,
  mediaBackoffDelay,
  permanentError,
  retryableError,
} from "../src/lib/media-errors.js";

describe("classifyError", () => {
  it("keeps explicit permanent errors permanent", () => {
    const error = permanentError("NO_AUDIO_STREAM", "probe");
    const classified = classifyError(error, "probe");
    expect(classified.retryable).toBe(false);
    expect(classified.spec.code).toBe("NO_AUDIO_STREAM");
  });

  it("classifies explicit timeout codes by name (download stage)", () => {
    const error = new Error("download exceeded 60000ms");
    error.name = "DOWNLOAD_TIMEOUT";
    const classified = classifyError(error, "download");
    expect(classified.retryable).toBe(true);
    expect(classified.spec.code).toBe("DOWNLOAD_TIMEOUT");
  });

  it("classifies idle download as retryable DOWNLOAD_STALLED", () => {
    const error = new Error("download idle for 15000ms");
    error.name = "DOWNLOAD_STALLED";
    const classified = classifyError(error, "download");
    expect(classified.retryable).toBe(true);
    expect(classified.spec.code).toBe("DOWNLOAD_STALLED");
  });

  it("classifies missing S3 object as a permanent failure", () => {
    const error = new Error("The specified key does not exist.");
    error.name = "NoSuchKey";
    const classified = classifyError(error, "download");
    expect(classified.retryable).toBe(false);
    expect(classified.spec.code).toBe("OBJECT_NOT_FOUND");
  });

  it("treats generic storage errors as retryable", () => {
    const classified = classifyError(new Error("connect ECONNREFUSED"), "download");
    expect(classified.retryable).toBe(true);
    expect(classified.spec.code).toBe("STORAGE_UNAVAILABLE");
  });

  it("treats corrupt-container ffprobe stderr as permanent INVALID_MEDIA", () => {
    const error = new Error("Invalid data found when processing input");
    const classified = classifyError(error, "probe");
    expect(classified.retryable).toBe(false);
    expect(classified.spec.code).toBe("INVALID_MEDIA");
  });

  it("treats unknown probe errors as retryable", () => {
    const classified = classifyError(new Error("something unexpected"), "probe");
    expect(classified.retryable).toBe(true);
    expect(classified.spec.code).toBe("MEDIA_PROBE_FAILED");
  });

  it("guards with isMediaProcessError", () => {
    expect(isMediaProcessError(retryableError("PROBE_TIMEOUT", "probe"))).toBe(true);
    expect(isMediaProcessError(new Error("x"))).toBe(false);
  });
});

describe("mediaBackoffDelay", () => {
  it("uses per-stage base delay on the first retry", () => {
    expect(mediaBackoffDelay(1, retryableError("PROBE_TIMEOUT", "probe"))).toBe(5_000);
    expect(mediaBackoffDelay(1, retryableError("DOWNLOAD_TIMEOUT", "download"))).toBe(3_000);
    expect(mediaBackoffDelay(1, retryableError("PEAKS_TIMEOUT", "peaks"))).toBe(10_000);
  });

  it("grows exponentially with attempt count", () => {
    expect(mediaBackoffDelay(3, retryableError("PROBE_TIMEOUT", "probe"))).toBe(20_000);
  });

  it("caps delay at ten minutes", () => {
    expect(mediaBackoffDelay(20, retryableError("PEAKS_TIMEOUT", "peaks"))).toBe(10 * 60_000);
  });

  it("falls back to probe base for non-classified errors", () => {
    expect(mediaBackoffDelay(1, new Error("boom"))).toBe(5_000);
  });
});
