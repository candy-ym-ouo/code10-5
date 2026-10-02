import { createWriteStream } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { Queue, UnrecoverableError, Worker } from "bullmq";
import { Redis } from "ioredis";
import { getConfig } from "./config/env.js";
import { prisma } from "./lib/prisma.js";
import { deleteObject, downloadObject, putObject } from "./lib/s3.js";
import { generatePeaks, probeAudio, type MediaTimeouts } from "./lib/media.js";
import { classifyError, isMediaProcessError, mediaBackoffDelay, type MediaProcessError } from "./lib/media-errors.js";
import { decideClaim, decideRetry, type ClaimRepository, type ProbeOutput } from "./lib/probe-state.js";
import { buildUserExport } from "./lib/export.js";

const config = getConfig();
const redis = new Redis(config.REDIS_URL, { maxRetriesPerRequest: null });
const recoveryQueue = new Queue("media-processing", { connection: redis.duplicate() });
const DOWNLOAD_TIMEOUTS = {
  totalTimeoutMs: config.DOWNLOAD_TOTAL_TIMEOUT_MS,
  idleTimeoutMs: config.DOWNLOAD_IDLE_TIMEOUT_MS,
};
const PROCESS_TIMEOUTS: MediaTimeouts = {
  probeTimeoutMs: config.PROBE_TIMEOUT_MS,
  peaksTimeoutMs: config.PEAKS_TIMEOUT_MS,
};
// 定时扫描 PROCESSING 卡死记录的间隔
const STALE_SWEEP_INTERVAL_MS = 60_000;
const STALE_SWEEP_BATCH = 100;

const log = (level: "info" | "error" | "warn", data: Record<string, unknown>, message: string) => {
  const output = JSON.stringify({ timestamp: new Date().toISOString(), level, service: "worker", ...data, message });
  if (level === "error") console.error(output);
  else if (level === "warn") console.warn(output);
  else console.log(output);
};

const claimRepository: ClaimRepository = {
  async findStatus(mediaId) {
    return prisma.mediaAsset.findUnique({
      where: { id: mediaId },
      select: { id: true, status: true, updatedAt: true },
    });
  },
  async claim(mediaId, from) {
    const result = await prisma.mediaAsset.updateMany({
      where: { id: mediaId, status: { in: from } },
      data: { status: "PROCESSING", failureCode: null, failureMessage: null },
    });
    return result.count === 1;
  },
};

/**
 * 认领一条待处理的媒体：
 * - UPLOADED / FAILED：正常任务（FAILED 仅来自用户手动重试的重新入队）
 * - PROCESSING：只允许接管崩溃遗留的卡死记录（updatedAt 超过阈值），
 *   存活中的记录直接跳过，避免重复执行
 * - READY / CANCELLED：幂等跳过
 */
async function claimMedia(mediaId: string): Promise<boolean> {
  const verdict = await decideClaim(claimRepository, mediaId, config.STALE_PROCESSING_MS);
  if (!verdict.ok && verdict.reason !== "already-terminal") {
    log("warn", { mediaId, reason: verdict.reason }, "probe job skipped");
  }
  return verdict.ok;
}

/** 幂等成功写入：仅当记录仍处于本次任务的 PROCESSING 时才落结果 */
async function writeProbeResult(
  mediaId: string,
  sessionId: string,
  userId: string,
  probe: ProbeOutput,
): Promise<boolean> {
  return prisma.$transaction(async (tx) => {
    const result = await tx.mediaAsset.updateMany({
      where: { id: mediaId, status: "PROCESSING" },
      data: {
        status: "READY",
        durationMs: probe.durationMs,
        codec: probe.codec,
        sampleRate: probe.sampleRate,
        channels: probe.channels,
        peaks: probe.peaks,
        processedAt: new Date(),
        expiresAt: null,
        failureCode: null,
        failureMessage: null,
      },
    });
    if (result.count === 0) {
      // 已被取消、删除或被另一个任务终结：不再推进练习状态
      log("warn", { mediaId }, "skip probe result write: media left PROCESSING state");
      return false;
    }
    await tx.practiceSession.updateMany({
      where: { id: sessionId, userId, status: "DRAFT" },
      data: { status: "IN_REVIEW", version: { increment: 1 } },
    });
    return true;
  });
}

/** 幂等失败终态写入：永久错误或重试耗尽时调用 */
async function writeProbeFailure(mediaId: string, classified: MediaProcessError): Promise<void> {
  const result = await prisma.mediaAsset.updateMany({
    where: { id: mediaId, status: "PROCESSING" },
    data: {
      status: "FAILED",
      failureCode: classified.spec.code,
      failureMessage: classified.spec.message.slice(0, 500),
      processedAt: new Date(),
    },
  });
  if (result.count === 0) log("warn", { mediaId }, "skip probe failure write: media left PROCESSING state");
}

async function processMedia(mediaId: string, attemptsMade: number, maxAttempts: number) {
  if (!(await claimMedia(mediaId))) return;
  const media = await prisma.mediaAsset.findUnique({ where: { id: mediaId } });
  if (!media) return;

  const workDir = await mkdtemp(path.join(tmpdir(), "practice-media-"));
  const extension = path.extname(media.originalName).slice(0, 12);
  const localPath = path.join(workDir, `audio${extension}`);
  try {
    try {
      await downloadObject(media.objectKey, createWriteStream(localPath), DOWNLOAD_TIMEOUTS);
    } catch (error) {
      throw classifyError(error, "download");
    }

    let probe: Awaited<ReturnType<typeof probeAudio>>;
    let peaks: number[];
    try {
      [probe, peaks] = await Promise.all([
        probeAudio(localPath, PROCESS_TIMEOUTS),
        generatePeaks(localPath, 1600, PROCESS_TIMEOUTS),
      ]);
    } catch (error) {
      throw isMediaProcessError(error) ? error : classifyError(error, "probe");
    }

    try {
      const written = await writeProbeResult(mediaId, media.sessionId, media.userId, { ...probe, peaks });
      if (written) log("info", { mediaId, durationMs: Number(probe.durationMs) }, "media probe completed");
    } catch (error) {
      throw classifyError(error, "result");
    }
  } catch (error) {
    const { retry, permanent, failure } = decideRetry(error, attemptsMade, maxAttempts);

    if (retry) {
      // 保持 PROCESSING，仅记录最近一次瞬时失败，供排障与卡死恢复判断
      const marked = await prisma.mediaAsset.updateMany({
        where: { id: mediaId, status: "PROCESSING" },
        data: { failureCode: failure.spec.code, failureMessage: failure.spec.message.slice(0, 500) },
      });
      if (marked.count === 0) {
        log("warn", { mediaId }, "skip probe retry: media left PROCESSING state");
        return;
      }
      log(
        "warn",
        { mediaId, code: failure.spec.code, attempt: attemptsMade + 1, maxAttempts },
        "media probe transient failure, scheduled for retry",
      );
      throw failure;
    }

    await writeProbeFailure(mediaId, failure);
    log(
      "error",
      { mediaId, code: failure.spec.code, retryable: failure.retryable, attempt: attemptsMade + 1 },
      permanent ? "media probe permanent failure" : "media probe retries exhausted",
    );
    if (permanent) {
      // 永久错误立即终止 BullMQ 重试，用户替换文件后可手动重试
      throw new UnrecoverableError(`${failure.spec.code}: ${failure.spec.message}`);
    }
    // 可重试错误但次数耗尽：DB 已是 FAILED 终态，任务正常结束，等待用户手动重试
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}

/**
 * 启动/定时恢复：崩溃的 worker 会把 media_assets 留在 PROCESSING。
 * 超过阈值仍未更新的记录重新投递探测任务（稳定 jobId 天然去重）。
 */
async function recoverStaleProcessing(): Promise<void> {
  try {
    const cutoff = new Date(Date.now() - config.STALE_PROCESSING_MS);
    const stale = await prisma.mediaAsset.findMany({
      where: { status: "PROCESSING", updatedAt: { lt: cutoff } },
      select: { id: true },
      take: STALE_SWEEP_BATCH,
    });
    for (const item of stale) {
      const jobId = `probe:${item.id}`;
      // 清理已终结的旧任务；仍在排队/执行中的任务保持不动
      const existing = await recoveryQueue.getJob(jobId);
      if (existing) {
        const state = await existing.getState();
        if (state === "completed" || state === "failed") await existing.remove();
      }
      await recoveryQueue.add(
        "probe-media",
        { mediaId: item.id },
        {
          jobId,
          attempts: config.PROBE_MAX_ATTEMPTS,
          backoff: { type: "media-tiered" },
          removeOnComplete: 100,
          removeOnFail: 500,
        },
      );
      log("warn", { mediaId: item.id }, "re-enqueued stale PROCESSING media");
    }
  } catch (error) {
    log("error", { err: error instanceof Error ? error.message : String(error) }, "stale processing sweep failed");
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

const worker = new Worker(
  "media-processing",
  async (job) => {
    if (job.name === "probe-media") {
      const maxAttempts = typeof job.opts.attempts === "number" && job.opts.attempts > 0 ? job.opts.attempts : 5;
      return processMedia(String((job.data as { mediaId?: unknown }).mediaId), job.attemptsMade, maxAttempts);
    }
    if (job.name === "cleanup-session") return cleanupSession(String((job.data as { sessionId?: unknown }).sessionId));
    if (job.name === "export-data") return exportData(String((job.data as { exportId?: unknown }).exportId));
    throw new Error(`Unknown job: ${job.name}`);
  },
  {
    connection: redis,
    concurrency: config.WORKER_CONCURRENCY,
    // 进程僵死时 BullMQ 兜底重投，DB 侧卡死恢复由 recoverStaleProcessing 负责
    stalledInterval: config.STALLED_INTERVAL_MS,
    maxStalledCount: config.MAX_STALLED_COUNT,
    settings: {
      backoffStrategy: (attemptsMade, type, error) => {
        if (type !== "media-tiered") return Math.min(3_000 * 2 ** Math.max(0, attemptsMade - 1), 10 * 60_000);
        return mediaBackoffDelay(attemptsMade, error);
      },
    },
  },
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

// 启动即恢复一次，之后每分钟扫描，保证重启后不会永久卡在 PROCESSING
await recoverStaleProcessing();
const staleSweepInterval = setInterval(() => {
  void recoverStaleProcessing();
}, STALE_SWEEP_INTERVAL_MS);

async function shutdown(signal: string) {
  log("info", { signal }, "shutting down worker");
  clearInterval(heartbeat);
  clearInterval(overdueInterval);
  clearInterval(staleSweepInterval);
  await worker.close();
  await recoveryQueue.close();
  await redis.quit();
  await prisma.$disconnect();
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
log("info", {}, "worker started");
