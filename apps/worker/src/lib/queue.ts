import { Queue } from "bullmq";
import type { Redis } from "ioredis";

export const MEDIA_QUEUE_NAME = "media-processing";

/** 探测任务统一的重试策略：API 首次投递与 Worker 恢复投递保持一致 */
export const PROBE_JOB_OPTIONS = {
  attempts: 4,
  backoff: { type: "exponential" as const, delay: 5000 },
  removeOnComplete: 100,
  removeOnFail: 500,
};

let queue: Queue | undefined;

export function getMediaQueue(redis: Redis): Queue {
  if (!queue) {
    queue = new Queue(MEDIA_QUEUE_NAME, { connection: redis.duplicate() });
  }
  return queue;
}

export async function enqueueProbe(redis: Redis, mediaId: string): Promise<void> {
  await getMediaQueue(redis).add("probe-media", { mediaId }, { ...PROBE_JOB_OPTIONS, jobId: `probe:${mediaId}:${Date.now()}` });
}

export async function closeMediaQueue(): Promise<void> {
  if (queue) {
    await queue.close();
    queue = undefined;
  }
}
