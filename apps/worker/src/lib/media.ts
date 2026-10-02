import { spawn } from "node:child_process";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { MediaProcessingError, classifyMediaError } from "./errors.js";

const execFileAsync = promisify(execFile);

export interface ProbeResult {
  durationMs: bigint;
  codec: string;
  sampleRate: number | null;
  channels: number | null;
}

export interface TimeoutBudget {
  downloadMs: number;
  probeMs: number;
  peaksMs: number;
}

const MB = 1024 * 1024;

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, Math.round(value)));
}

/**
 * 按文件大小分级的超时预算：下载、格式探测、波形解码各自独立限时，
 * 小文件快速失败，大文件给足时间，任何单阶段挂起都不会拖死整个任务。
 */
export function timeoutBudgetFor(sizeBytes: bigint | number): TimeoutBudget {
  const sizeMb = Number(sizeBytes) / MB;
  return {
    downloadMs: clamp(30_000 + sizeMb * 2_000, 30_000, 10 * 60_000),
    probeMs: clamp(15_000 + sizeMb * 500, 15_000, 120_000),
    peaksMs: clamp(60_000 + sizeMb * 3_000, 60_000, 15 * 60_000),
  };
}

export function totalBudgetMs(budget: TimeoutBudget): number {
  return budget.downloadMs + budget.probeMs + budget.peaksMs;
}

interface FFProbeOutput {
  format?: { duration?: string };
  streams?: Array<{
    codec_type?: string;
    codec_name?: string;
    sample_rate?: string;
    channels?: number;
    duration?: string;
  }>;
}

export async function probeAudio(filePath: string, timeoutMs: number): Promise<ProbeResult> {
  let stdout: string;
  try {
    const result = await execFileAsync(
      "ffprobe",
      [
        "-v",
        "error",
        "-show_entries",
        "format=duration:stream=codec_type,codec_name,sample_rate,channels,duration",
        "-of",
        "json",
        filePath,
      ],
      { timeout: timeoutMs, killSignal: "SIGKILL", maxBuffer: 4 * 1024 * 1024 },
    );
    stdout = result.stdout;
  } catch (error) {
    const killed = typeof error === "object" && error !== null && (error as { killed?: boolean }).killed === true;
    if (killed) {
      throw new MediaProcessingError("PROBE_TIMEOUT", `ffprobe exceeded ${timeoutMs}ms`, true, "probe");
    }
    throw classifyMediaError(error, "probe");
  }

  let parsed: FFProbeOutput;
  try {
    parsed = JSON.parse(stdout) as FFProbeOutput;
  } catch {
    throw new MediaProcessingError("MEDIA_UNREADABLE", "ffprobe returned unparseable output", false, "probe");
  }
  const stream = parsed.streams?.find((item) => item.codec_type === "audio");
  if (!stream) throw new MediaProcessingError("NO_AUDIO_STREAM", "file contains no audio stream", false, "probe");
  const durationSeconds = Number(parsed.format?.duration ?? stream.duration ?? 0);
  if (!Number.isFinite(durationSeconds) || durationSeconds <= 0) {
    throw new MediaProcessingError("INVALID_DURATION", "audio duration is missing or invalid", false, "probe");
  }
  return {
    durationMs: BigInt(Math.round(durationSeconds * 1000)),
    codec: stream.codec_name ?? "unknown",
    sampleRate: stream.sample_rate ? Number(stream.sample_rate) : null,
    channels: stream.channels ?? null,
  };
}

export async function generatePeaks(filePath: string, timeoutMs: number, bucketCount = 1600): Promise<number[]> {
  const chunks: Buffer[] = [];
  await new Promise<void>((resolve, reject) => {
    const child = spawn(
      "ffmpeg",
      ["-hide_banner", "-loglevel", "error", "-i", filePath, "-vn", "-ac", "1", "-ar", "100", "-f", "s16le", "pipe:1"],
      { stdio: ["ignore", "pipe", "pipe"] },
    );
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new MediaProcessingError("PEAKS_TIMEOUT", `ffmpeg exceeded ${timeoutMs}ms`, true, "peaks"));
    }, timeoutMs);
    const settle = (error?: Error) => {
      clearTimeout(timer);
      if (error) reject(error);
      else resolve();
    };
    let stderr = "";
    child.stdout.on("data", (chunk: Buffer) => chunks.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString().slice(0, 2000);
    });
    child.on("error", (error) => settle(classifyMediaError(error, "peaks")));
    child.on("close", (code) => {
      if (code === 0) settle();
      else settle(classifyMediaError(new Error(stderr || `ffmpeg exited with ${code}`), "peaks"));
    });
  });

  const samples = Buffer.concat(chunks);
  const sampleCount = Math.floor(samples.length / 2);
  if (sampleCount === 0) {
    throw new MediaProcessingError("EMPTY_PCM", "ffmpeg produced no audio samples", false, "peaks");
  }
  const actualBuckets = Math.min(bucketCount, sampleCount);
  const bucketSize = Math.max(1, Math.ceil(sampleCount / actualBuckets));
  const result: number[] = [];
  for (let bucket = 0; bucket < actualBuckets; bucket += 1) {
    const start = bucket * bucketSize;
    const end = Math.min(sampleCount, start + bucketSize);
    let max = 0;
    for (let index = start; index < end; index += 1) {
      const value = Math.abs(samples.readInt16LE(index * 2));
      if (value > max) max = value;
    }
    result.push(Number((max / 32768).toFixed(4)));
  }
  return result;
}
