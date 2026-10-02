/**
 * 音频探测错误分级
 *
 * - 永久错误（用户文件问题）：直接标记 FAILED，不消耗 BullMQ 重试，
 *   用户替换文件或手动调用 retry-probe 后才能再次处理。
 * - 可重试错误（超时 / 基础设施抖动）：抛回 BullMQ 按退避策略自动重试，
 *   达到最大次数后才落为 FAILED。
 */

export type FailureStage = "download" | "probe" | "peaks" | "result";

export interface FailureSpec {
  code: string;
  /** 面向用户的中文提示 */
  message: string;
  retryable: boolean;
  /** 自定义退避档位，决定下一次重试的基础延迟 */
  backoffTier: "download" | "probe" | "peaks" | "result";
}

/** 永久错误码（写入 media_assets.failure_code） */
export const PERMANENT_FAILURES: Record<string, FailureSpec> = {
  OBJECT_NOT_FOUND: {
    code: "OBJECT_NOT_FOUND",
    message: "音频对象已丢失，请重新上传文件",
    retryable: false,
    backoffTier: "download",
  },
  NO_AUDIO_STREAM: {
    code: "NO_AUDIO_STREAM",
    message: "文件中没有可用的音轨",
    retryable: false,
    backoffTier: "probe",
  },
  INVALID_DURATION: {
    code: "INVALID_DURATION",
    message: "无法读取有效的音频时长，文件可能已损坏",
    retryable: false,
    backoffTier: "probe",
  },
  UNSUPPORTED_CODEC: {
    code: "UNSUPPORTED_CODEC",
    message: "音频编码暂不支持，请转码后重新上传",
    retryable: false,
    backoffTier: "probe",
  },
  INVALID_MEDIA: {
    code: "INVALID_MEDIA",
    message: "音频文件已损坏或格式无法识别，请替换文件后重试",
    retryable: false,
    backoffTier: "probe",
  },
  EMPTY_PCM: {
    code: "EMPTY_PCM",
    message: "音频解码结果为空，请替换文件后重试",
    retryable: false,
    backoffTier: "peaks",
  },
};

/** 可重试错误码 */
export const RETRYABLE_FAILURES: Record<string, FailureSpec> = {
  DOWNLOAD_TIMEOUT: {
    code: "DOWNLOAD_TIMEOUT",
    message: "音频下载超时，将自动重试",
    retryable: true,
    backoffTier: "download",
  },
  DOWNLOAD_STALLED: {
    code: "DOWNLOAD_STALLED",
    message: "音频下载长时间无数据，将自动重试",
    retryable: true,
    backoffTier: "download",
  },
  STORAGE_UNAVAILABLE: {
    code: "STORAGE_UNAVAILABLE",
    message: "对象存储暂不可用，将自动重试",
    retryable: true,
    backoffTier: "download",
  },
  PROBE_TIMEOUT: {
    code: "PROBE_TIMEOUT",
    message: "音频解析超时，将自动重试",
    retryable: true,
    backoffTier: "probe",
  },
  PROBE_FAILED: {
    code: "PROBE_FAILED",
    message: "音频解析失败，将自动重试",
    retryable: true,
    backoffTier: "probe",
  },
  PEAKS_TIMEOUT: {
    code: "PEAKS_TIMEOUT",
    message: "波形生成超时，将自动重试",
    retryable: true,
    backoffTier: "peaks",
  },
  PEAKS_FAILED: {
    code: "PEAKS_FAILED",
    message: "波形生成失败，将自动重试",
    retryable: true,
    backoffTier: "peaks",
  },
  RESULT_WRITE_FAILED: {
    code: "RESULT_WRITE_FAILED",
    message: "结果写入失败，将自动重试",
    retryable: true,
    backoffTier: "result",
  },
  MEDIA_PROBE_FAILED: {
    code: "MEDIA_PROBE_FAILED",
    message: "音频解析服务暂时不可用，将自动重试",
    retryable: true,
    backoffTier: "probe",
  },
};

const PERMANENT_FFPROBE_PATTERNS = [
  "Invalid data found when processing input",
  "Invalid argument",
  "could not find codec parameters",
  "could not find codec tag",
  "missing audio stream",
  "Operation not permitted", // 容器/编码不被支持时常见
];

/** 任务处理器抛出的分级错误。retryable=true 时由 BullMQ 自动重试。 */
export class MediaProcessError extends Error {
  readonly spec: FailureSpec;
  readonly stage: FailureStage;
  readonly cause?: unknown;

  constructor(spec: FailureSpec, stage: FailureStage, cause?: unknown) {
    super(spec.message);
    this.name = "MediaProcessError";
    this.spec = spec;
    this.stage = stage;
    this.cause = cause;
  }

  get retryable(): boolean {
    return this.spec.retryable;
  }
}

export function isMediaProcessError(error: unknown): error is MediaProcessError {
  return error instanceof MediaProcessError;
}

export function permanentError(code: keyof typeof PERMANENT_FAILURES, stage: FailureStage, cause?: unknown): MediaProcessError {
  return new MediaProcessError(PERMANENT_FAILURES[code]!, stage, cause);
}

export function retryableError(code: keyof typeof RETRYABLE_FAILURES, stage: FailureStage, cause?: unknown): MediaProcessError {
  return new MediaProcessError(RETRYABLE_FAILURES[code]!, stage, cause);
}

/**
 * 把底层错误归一化成 MediaProcessError。
 * 已经是 MediaProcessError 或带 code 的探测错误直接采用其分级。
 */
export function classifyError(error: unknown, stage: FailureStage): MediaProcessError {
  if (error instanceof MediaProcessError) return error;

  const rawMessage = error instanceof Error ? error.message : String(error ?? "UNKNOWN_MEDIA_ERROR");
  const errorName = error instanceof Error ? error.name : "";

  const byCode = PERMANENT_FAILURES[rawMessage] ?? RETRYABLE_FAILURES[rawMessage] ??
    (errorName ? PERMANENT_FAILURES[errorName] ?? RETRYABLE_FAILURES[errorName] : undefined);
  if (byCode) return new MediaProcessError(byCode, stage, error);

  const normalized = rawMessage.toLowerCase();
  if (stage === "download") {
    if (
      normalized.includes("nosuchkey") ||
      normalized.includes("not found") ||
      normalized.includes("no such key") ||
      errorName === "NoSuchKey"
    ) {
      return permanentError("OBJECT_NOT_FOUND", stage, error);
    }
    return retryableError("STORAGE_UNAVAILABLE", stage, error);
  }

  if (PERMANENT_FFPROBE_PATTERNS.some((pattern) => rawMessage.includes(pattern))) {
    return permanentError("INVALID_MEDIA", stage, error);
  }

  // 未知错误按可重试处理，避免一次偶发问题永久判死用户文件
  return retryableError("MEDIA_PROBE_FAILED", stage, error);
}

/**
 * 自定义退避：各阶段独立基础延迟 + 指数退避（上限 10 分钟）。
 * 必须始终返回数字：BullMQ 对 undefined 会按 0ms 立即重试。
 */
export const BACKOFF_BASE_DELAY_MS: Record<FailureSpec["backoffTier"], number> = {
  download: 3_000,
  probe: 5_000,
  peaks: 10_000,
  result: 5_000,
};

export function mediaBackoffDelay(attemptsMade: number, error: unknown): number {
  const spec = (error instanceof MediaProcessError ? error.spec : RETRYABLE_FAILURES.MEDIA_PROBE_FAILED)!;
  const base = BACKOFF_BASE_DELAY_MS[spec.backoffTier]!;
  return Math.min(base * 2 ** Math.max(0, attemptsMade - 1), 10 * 60_000);
}
