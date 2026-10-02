import { classifyError, isMediaProcessError, type FailureStage, type MediaProcessError } from "./media-errors.js";

/**
 * 探测任务状态机（不连接任何基础设施，便于单测）：
 *
 * - decideClaim：决定当前任务是否允许接管一条媒体记录
 * - decideRetry：区分可重试错误与永久错误以及是否还有重试预算
 *
 * 所有 DB 访问由 Worker 用 Prisma 实现，状态写入都带 PROCESSING 守卫因而幂等。
 */

export type ProcessableStatus = "UPLOADED" | "FAILED" | "PROCESSING";

export interface MediaSnapshot {
  status: string;
  updatedAt: Date;
}

export interface ProbeOutput {
  durationMs: bigint;
  codec: string;
  sampleRate: number | null;
  channels: number | null;
  peaks: number[];
}

export type ClaimVerdict =
  | { ok: true; reason: "claimed" }
  | {
      ok: false;
      reason: "missing" | "already-processing" | "already-terminal" | "not-processable" | "claim-lost";
    };

export interface ClaimRepository {
  findStatus(mediaId: string): Promise<MediaSnapshot | null>;
  /** 条件认领为 PROCESSING，返回是否命中一行 */
  claim(mediaId: string, from: ProcessableStatus[]): Promise<boolean>;
}

export async function decideClaim(
  repo: ClaimRepository,
  mediaId: string,
  staleProcessingMs: number,
): Promise<ClaimVerdict> {
  const media = await repo.findStatus(mediaId);
  if (!media) return { ok: false, reason: "missing" };
  if (media.status === "READY" || media.status === "CANCELLED") {
    return { ok: false, reason: "already-terminal" };
  }
  if (media.status === "PROCESSING" && Date.now() - media.updatedAt.getTime() < staleProcessingMs) {
    return { ok: false, reason: "already-processing" };
  }
  if (!["UPLOADED", "FAILED", "PROCESSING"].includes(media.status)) {
    return { ok: false, reason: "not-processable" };
  }
  const claimed = await repo.claim(mediaId, ["UPLOADED", "FAILED", "PROCESSING"]);
  return claimed ? { ok: true, reason: "claimed" } : { ok: false, reason: "claim-lost" };
}

export interface RetryDecision {
  /** 是否应把错误抛回 BullMQ 触发自动重试 */
  retry: boolean;
  /** 是否为永久错误（永久错误立即终止，不消耗重试预算） */
  permanent: boolean;
  failure: MediaProcessError;
}

/**
 * attemptsMade 为 BullMQ 当前已完成的尝试次数（从 0 起），
 * 当前是第 attemptsMade + 1 次执行。
 */
export function decideRetry(
  error: unknown,
  attemptsMade: number,
  maxAttempts: number,
  stage: FailureStage = "probe",
): RetryDecision {
  const failure = isMediaProcessError(error) ? error : classifyError(error, stage);
  const hasBudget = attemptsMade + 1 < maxAttempts;
  return {
    retry: failure.retryable && hasBudget,
    permanent: !failure.retryable,
    failure,
  };
}
