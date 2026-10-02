import { createWriteStream } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { UnrecoverableError, Worker } from "bullmq";
import type { Job } from "bullmq";
import { Redis } from "ioredis";
import { getConfig } from "./config/env.js";
import { prisma } from "./lib/prisma.js";
import { deleteObject, getObjectStream, putObject } from "./lib/s3.js";
import { generatePeaks, probeAudio, timeoutBudgetFor, totalBudgetMs } from "./lib/media.js";
import { MediaProcessingError, classifyMediaError, failureMessageFor } from "./lib/errors.js";
import { closeMediaQueue, enqueueProbe } from "./lib/queue.js";
import { buildUserExport } from "./lib/export.js";

const config = getConfig();
const redis = new Redis(config.REDIS_URL, { maxRetriesPerRequest: null });
const log = (level: "info" | "error" | "warn", data: Record<string, unknown>, message: string) => {
  const output = JSON.stringify({ timestamp: new Date().toISOString(), level, service: "worker", ...data, message });
  if (level === "error") console.error(output);
  else if (level === "warn") console.warn(output);
  else console.log(output);
};

/** 下载对象到本地，超时主动断开，避免挂起的连接拖死任务 */
async function downloadObject(objectKey: string, localPath: string, timeoutMs: number): Promise<void> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const stream = await getObjectStream(objectKey, controller.signal);
    await pipeline(stream, createWriteStream(localPath));
  } catch (error) {
    if (controller.signal.aborted) {
      throw new MediaProcessingError("DOWNLOAD_TIMEOUT", `download exceeded ${timeoutMs}ms`, true, "download");
    }
    const classified = classifyMediaError(error, "download");
    if (classified.code === "MEDIA_PROBE_FAILED") {
      throw new MediaProcessingError("MEDIA_DOWNLOAD_FAILED", classified.message, true, "download");
    }
    throw classified;
  } finally {
    clearTimeout(timer);
  }
}

async function markProbeFailed(mediaId: string, error: MediaProcessingError): Promise<void> {
  await prisma.mediaAsset.updateMany({
    where: { id: mediaId, status: "PROCESSING" },
    data: { status: "FAILED", failureCode: error.code, failureMessage: failureMessageFor(error.code), processedAt: new Date() },
  });
}

async function processMedia(mediaId: string, job: Job): Promise<void> {
  const media = await prisma.mediaAsset.findUnique({ where: { id: mediaId } });
  if (!media) return;
  // 幂等：已就绪的记录直接跳过，重复投递不产生副作用
  if (media.status === "READY") return;
  // 条件认领：只有仍处于待处理状态的记录才会进入 PROCESSING，
  // 并发删除、取消或用户重试导致的状态变化都不会被覆盖
  const claimed = await prisma.mediaAsset.updateMany({
    where: { id: mediaId, status: { in: ["UPLOADED", "PROCESSING"] } },
    data: { status: "PROCESSING", failureCode: null, failureMessage: null },
  });
  if (claimed.count === 0) {
    log("info", { mediaId, status: media.status }, "media no longer processable, skipping");
    return;
  }

  const budget = timeoutBudgetFor(media.sizeBytes);
  const workDir = await mkdtemp(path.join(tmpdir(), "practice-media-"));
  const extension = path.extname(media.originalName).slice(0, 12);
  const localPath = path.join(workDir, `audio${extension}`);
  try {
    await downloadObject(media.objectKey, localPath, budget.downloadMs);
    const probe = await probeAudio(localPath, budget.probeMs);
    const peaks = await generatePeaks(localPath, budget.peaksMs);
    await prisma.$transaction(async (tx) => {
      // 幂等写入：仅当记录仍处在 PROCESSING 时才落地结果，
      // 处理期间被删除或状态被外部改变时静默放弃
      const updated = await tx.mediaAsset.updateMany({
        where: { id: mediaId, status: "PROCESSING" },
        data: {
          status: "READY",
          durationMs: probe.durationMs,
          codec: probe.codec,
          sampleRate: probe.sampleRate,
          channels: probe.channels,
          peaks,
          processedAt: new Date(),
          expiresAt: null,
          failureCode: null,
          failureMessage: null,
        },
      });
      if (updated.count === 0) return;
      await tx.practiceSession.updateMany({
        where: { id: media.sessionId, userId: media.userId, status: "DRAFT" },
        data: { status: "IN_REVIEW", version: { increment: 1 } },
      });
    });
    log("info", { mediaId, durationMs: Number(probe.durationMs), attempt: job.attemptsMade + 1 }, "media probe completed");
  } catch (error) {
    const classified = classifyMediaError(error);
    const attempts = job.opts.attempts ?? 1;
    const finalAttempt = job.attemptsMade + 1 >= attempts;
    if (!classified.retryable) {
      // 永久错误：写入终态并抛出 UnrecoverableError，BullMQ 不再重试
      await markProbeFailed(mediaId, classified);
      log("error", { mediaId, code: classified.code, err: classified.message }, "media probe failed permanently");
      throw new UnrecoverableError(classified.message);
    }
    if (finalAttempt) {
      // 可重试错误但次数耗尽：写入终态，避免永远停在 PROCESSING
      await markProbeFailed(mediaId, classified);
      log("error", { mediaId, code: classified.code, attempts, err: classified.message }, "media probe retries exhausted");
    } else {
      log("warn", { mediaId, code: classified.code, attempt: job.attemptsMade + 1, attempts, err: classified.message }, "media probe failed, will retry");
    }
    throw classified;
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}

async function cleanupSession(sessionId: string) {
  const session = await prisma.practiceSession.findUnique({
    where: { id: sessionId },
    include: { mediaAssets: { select: { objectKey: true } } },
  });
  if (!session) return;
  try {
    const keys = new Set(session.mediaAssets.map((media) => media.objectKey));
    for (const objectKey of keys) {
      const references = await prisma.mediaAsset.count({ where: { objectKey, sessionId: { not: sessionId } } });
      if (references === 0) await deleteObject(objectKey);
    }
    await prisma.practiceSession.delete({ where: { id: sessionId } });
    log("info", { sessionId }, "session cleanup completed");
  } catch (error) {
    await prisma.practiceSession.updateMany({ where: { id: sessionId }, data: { status: "DELETE_FAILED" } });
    throw error;
  }
}

async function exportData(exportId: string) {
  const task = await prisma.dataExport.findUnique({ where: { id: exportId } });
  if (!task || !task.objectKey) return;
  await prisma.dataExport.update({ where: { id: exportId }, data: { status: "PROCESSING" } });
  try {
    const output = await buildUserExport(task.userId, task.format);
    await putObject(task.objectKey, output.body, output.contentType);
    await prisma.dataExport.update({ where: { id: exportId }, data: { status: "READY", failure: null } });
  } catch (error) {
    await prisma.dataExport.update({
      where: { id: exportId },
      data: { status: "FAILED", failure: error instanceof Error ? error.message.slice(0, 500) : "EXPORT_FAILED" },
    });
    throw error;
  }
}

async function scanOverdueGoals() {
  const startOfToday = new Date();
  startOfToday.setUTCHours(0, 0, 0, 0);
  const result = await prisma.goal.updateMany({
    where: {
      dueDate: { lt: startOfToday },
      status: { in: ["OPEN", "IN_PROGRESS"] },
    },
    data: { status: "MISSED" },
  });
  if (result.count > 0) log("info", { count: result.count }, "overdue goals marked missed");
}

const REAPER_INTERVAL_MS = 60_000;
const REAPER_BATCH_SIZE = 50;
/** UPLOADED 状态超过该时长仍未被处理，说明入队丢失或 worker 长期不可用 */
const STUCK_UPLOADED_MS = 10 * 60_000;
/** PROCESSING 卡死下限：即使很小的文件也至少等待该时长，避免与健康任务竞争 */
const STUCK_PROCESSING_FLOOR_MS = 5 * 60_000;
const STUCK_PROCESSING_MARGIN_MS = 60_000;

/**
 * 兜底恢复：Worker 崩溃、Redis 任务丢失或 stalled 次数耗尽时，
 * 数据库中的记录不能永远停在 PROCESSING/UPLOADED。
 * 借助 @updatedAt 的条件更新做原子认领，多个 worker 实例并发扫描只会补投一次。
 */
async function recoverStuckMedia() {
  const now = Date.now();
  const candidates = await prisma.mediaAsset.findMany({
    where: {
      status: { in: ["UPLOADED", "PROCESSING"] },
      updatedAt: { lt: new Date(now - STUCK_PROCESSING_FLOOR_MS) },
    },
    select: { id: true, status: true, sizeBytes: true, updatedAt: true },
    orderBy: { updatedAt: "asc" },
    take: REAPER_BATCH_SIZE,
  });
  for (const candidate of candidates) {
    const stuckMs =
      candidate.status === "UPLOADED"
        ? STUCK_UPLOADED_MS
        : Math.max(STUCK_PROCESSING_FLOOR_MS, totalBudgetMs(timeoutBudgetFor(candidate.sizeBytes)) + STUCK_PROCESSING_MARGIN_MS);
    const stuckCutoff = new Date(now - stuckMs);
    if (candidate.updatedAt >= stuckCutoff) continue;
    // 原子认领：更新会刷新 @updatedAt，并发 reaper 中只有一个能匹配到旧的 updatedAt
    const claimed = await prisma.mediaAsset.updateMany({
      where: { id: candidate.id, status: candidate.status, updatedAt: { lt: stuckCutoff } },
      data: { failureMessage: null },
    });
    if (claimed.count === 0) continue;
    try {
      await enqueueProbe(redis, candidate.id);
      log("warn", { mediaId: candidate.id, status: candidate.status }, "re-enqueued stuck media");
    } catch (error) {
      log("error", { mediaId: candidate.id, err: error instanceof Error ? error.message : String(error) }, "failed to re-enqueue stuck media");
    }
  }
}

const worker = new Worker(
  "media-processing",
  async (job) => {
    if (job.name === "probe-media") return processMedia(String(job.data.mediaId), job);
    if (job.name === "cleanup-session") return cleanupSession(String(job.data.sessionId));
    if (job.name === "export-data") return exportData(String(job.data.exportId));
    throw new Error(`Unknown job: ${job.name}`);
  },
  { connection: redis, concurrency: config.WORKER_CONCURRENCY, maxStalledCount: 2 },
);

worker.on("failed", (job, error) => log("error", { jobId: job?.id, jobName: job?.name, err: error.message }, "job failed"));
worker.on("error", (error) => log("error", { err: error.message }, "worker error"));

const heartbeat = setInterval(async () => {
  await redis.set("worker:heartbeat", new Date().toISOString(), "EX", 30);
}, 10_000);
await redis.set("worker:heartbeat", new Date().toISOString(), "EX", 30);
await scanOverdueGoals().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "overdue scan failed"));
const overdueInterval = setInterval(() => {
  void scanOverdueGoals().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "overdue scan failed"));
}, 24 * 60 * 60_000);

await recoverStuckMedia().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "stuck media recovery failed"));
const reaperInterval = setInterval(() => {
  void recoverStuckMedia().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "stuck media recovery failed"));
}, REAPER_INTERVAL_MS);

async function shutdown(signal: string) {
  log("info", { signal }, "shutting down worker");
  clearInterval(heartbeat);
  clearInterval(overdueInterval);
  clearInterval(reaperInterval);
  await worker.close();
  await closeMediaQueue();
  await redis.quit();
  await prisma.$disconnect();
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
log("info", {}, "worker started");
