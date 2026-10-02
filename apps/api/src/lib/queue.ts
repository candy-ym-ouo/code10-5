import { Queue } from "bullmq";
import { getConfig } from "../config/env.js";
import { getRedis } from "./redis.js";

let queue: Queue | undefined;

// 可重试错误的自动重试次数与分级退避；永久错误由 Worker 抛 UnrecoverableError 立即终止
const PROBE_ATTEMPTS = Number(process.env.PROBE_MAX_ATTEMPTS ?? 4);
const PROBE_JOB_PREFIX = "probe";

export function getMediaQueue(): Queue {
  if (!queue) {
    queue = new Queue("media-processing", { connection: getRedis().duplicate() });
  }
  return queue;
}

export function probeJobId(mediaId: string): string {
  return `${PROBE_JOB_PREFIX}:${mediaId}`;
}

/**
 * 稳定 jobId 会让 BullMQ 对重复 add 静默 no-op（包括已终结任务）。
 * 因此入队前删除已 completed/failed 的旧任务，保证手动重试与卡死恢复能真正投递；
 * 仍在 waiting/delayed/active 的任务保留，天然对重复确认去重。
 */
async function pruneFinishedJob(target: Queue, jobId: string): Promise<void> {
  const existing = await target.getJob(jobId);
  if (!existing) return;
  const state = await existing.getState();
  if (state === "completed" || state === "failed") await existing.remove();
}

export async function enqueueProbe(mediaId: string): Promise<void> {
  const target = getMediaQueue();
  const jobId = probeJobId(mediaId);
  await pruneFinishedJob(target, jobId);
  await target.add(
    "probe-media",
    { mediaId },
    {
      jobId,
      attempts: PROBE_ATTEMPTS,
      // 由 Worker 的 backoffStrategy 按错误阶段（下载/探测/波形/写库）计算延迟
      backoff: { type: "media-tiered" },
      removeOnComplete: 100,
      removeOnFail: 500,
    },
  );
}

export async function enqueueCleanup(sessionId: string): Promise<void> {
  await getMediaQueue().add(
    "cleanup-session",
    { sessionId },
    {
      jobId: `cleanup:${sessionId}:${Date.now()}`,
      attempts: 5,
      backoff: { type: "exponential", delay: 5000 },
      removeOnComplete: 100,
      removeOnFail: 500,
    },
  );
}

export async function enqueueExport(exportId: string): Promise<void> {
  await getMediaQueue().add(
    "export-data",
    { exportId },
    {
      jobId: `export:${exportId}`,
      attempts: 3,
      backoff: { type: "exponential", delay: 3000 },
      removeOnComplete: 100,
      removeOnFail: 500,
    },
  );
}

export async function closeQueue(): Promise<void> {
  if (queue) {
    await queue.close();
    queue = undefined;
  }
}
