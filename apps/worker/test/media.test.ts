import { execFileSync, spawnSync } from "node:child_process";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { generatePeaks, probeAudio } from "../src/lib/media.js";
import { isMediaProcessError } from "../src/lib/media-errors.js";

const workRoot = await mkdtemp(path.join(tmpdir(), "worker-media-test-"));
let sampleWav = "";
let hasFfmpeg = false;

beforeAll(async () => {
  hasFfmpeg = spawnSync("ffmpeg", ["-version"]).status === 0;
  sampleWav = path.join(workRoot, "sample.wav");
  if (hasFfmpeg) {
    execFileSync(
      "ffmpeg",
      ["-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "sine=frequency=440:duration=1", "-y", sampleWav],
      { stdio: "ignore" },
    );
  }
});

afterAll(async () => {
  await rm(workRoot, { recursive: true, force: true });
});

describe.skipIf(!hasFfmpeg)("probeAudio with real ffprobe", () => {
  it("probes a valid wav", async () => {
    const result = await probeAudio(sampleWav);
    expect(Number(result.durationMs)).toBeGreaterThan(900);
    expect(result.channels).toBe(2);
  });

  it("classifies a non-audio file as a permanent media error", async () => {
    const text = path.join(workRoot, "not-audio.txt");
    await writeFile(text, "definitely not audio");
    await expect(probeAudio(text)).rejects.toSatisfy(
      (error: unknown) => isMediaProcessError(error) && error.retryable === false,
    );
  });
});

describe("probe timeout classification", () => {
  let fakeBin: string;
  let savedPath: string;

  beforeAll(async () => {
    fakeBin = path.join(workRoot, "bin");
    const { mkdir } = await import("node:fs/promises");
    await mkdir(fakeBin, { recursive: true });
    const probeScript = path.join(fakeBin, "ffprobe");
    // 忽略参数，睡眠超过超时，模拟挂死的探测进程
    await writeFile(probeScript, "#!/bin/sh\nsleep 30\n");
    await chmod(probeScript, 0o755);
    savedPath = process.env.PATH ?? "";
    process.env.PATH = `${fakeBin}${path.delimiter}${savedPath}`;
  });

  afterAll(() => {
    process.env.PATH = savedPath;
  });

  it("classifies a hung ffprobe as retryable PROBE_TIMEOUT", async () => {
    await expect(probeAudio(sampleWav || "whatever.wav", { probeTimeoutMs: 200, peaksTimeoutMs: 200 })).rejects.toSatisfy(
      (error: unknown) =>
        isMediaProcessError(error) && error.retryable === true && error.spec.code === "PROBE_TIMEOUT",
    );
  });
});

describe.skipIf(!hasFfmpeg)("generatePeaks", () => {
  it("produces normalized peak buckets", async () => {
    const peaks = await generatePeaks(sampleWav, 100);
    expect(peaks.length).toBe(100);
    for (const value of peaks) {
      expect(value).toBeGreaterThanOrEqual(0);
      expect(value).toBeLessThanOrEqual(1);
    }
  });
});
