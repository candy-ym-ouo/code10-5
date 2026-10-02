export type MediaFailureCode =
  | "NO_AUDIO_STREAM"
  | "INVALID_DURATION"
  | "EMPTY_PCM"
  | "MEDIA_UNREADABLE"
  | "MEDIA_OBJECT_MISSING"
  | "MEDIA_DOWNLOAD_FAILED"
  | "DOWNLOAD_TIMEOUT"
  | "PROBE_TIMEOUT"
  | "PEAKS_TIMEOUT"
  | "MEDIA_PROBE_FAILED";

export type MediaStage = "download" | "probe" | "peaks";

/**
 * 音频处理错误。retryable 为 true 时交给 BullMQ 按退避重试，
 * 否则视为永久错误，直接写入终态 FAILED，不再浪费重试。
 */
export class MediaProcessingError extends Error {
  readonly code: MediaFailureCode;
  readonly retryable: boolean;
  readonly stage: MediaStage;

  constructor(code: MediaFailureCode, message: string, retryable: boolean, stage: MediaStage) {
    super(message);
    this.name = "MediaProcessingError";
    this.code = code;
    this.retryable = retryable;
    this.stage = stage;
  }
}

/** ffprobe/ffmpeg 输出中可判定为文件本身损坏或不受支持的特征，重试无意义 */
const PERMANENT_OUTPUT_PATTERNS = [
  /invalid data found/i,
  /moov atom not found/i,
  /could not find codec parameters/i,
  /unknown decoder/i,
  /unsupported codec/i,
  /does not contain any stream/i,
  /end of file/i,
  /invalid argument/i,
  /operation not permitted/i,
];

interface S3LikeError {
  name?: string;
  Code?: string;
  $metadata?: { httpStatusCode?: number };
}

function isS3MissingObject(error: S3LikeError): boolean {
  const code = error.name ?? error.Code ?? "";
  return code === "NoSuchKey" || code === "NotFound" || error.$metadata?.httpStatusCode === 404;
}

/**
 * 把任意异常归类为可重试或永久的 MediaProcessingError。
 * 未知错误默认按可重试处理：瞬态基础设施故障占多数，
 * 确定性错误会在重试耗尽后落入终态 FAILED。
 */
export function classifyMediaError(error: unknown, stage: MediaStage = "probe"): MediaProcessingError {
  if (error instanceof MediaProcessingError) return error;
  const like = error as S3LikeError & { code?: string };
  const message = error instanceof Error ? error.message : String(error);

  if (isS3MissingObject(like)) {
    return new MediaProcessingError("MEDIA_OBJECT_MISSING", message, false, "download");
  }
  if (PERMANENT_OUTPUT_PATTERNS.some((pattern) => pattern.test(message))) {
    return new MediaProcessingError("MEDIA_UNREADABLE", message.slice(0, 400), false, stage);
  }
  return new MediaProcessingError("MEDIA_PROBE_FAILED", message.slice(0, 400), true, stage);
}

const FAILURE_MESSAGES: Record<MediaFailureCode, string> = {
  NO_AUDIO_STREAM: "文件中没有可用的音轨",
  INVALID_DURATION: "音频时长无效，请替换文件后重试",
  EMPTY_PCM: "文件中没有可解码的音频数据",
  MEDIA_UNREADABLE: "音频无法解析，请替换文件后重试",
  MEDIA_OBJECT_MISSING: "音频对象已丢失，请重新上传",
  MEDIA_DOWNLOAD_FAILED: "音频下载多次失败，请稍后重试",
  DOWNLOAD_TIMEOUT: "音频下载多次超时，请稍后重试",
  PROBE_TIMEOUT: "音频探测多次超时，请稍后重试",
  PEAKS_TIMEOUT: "波形生成多次超时，请稍后重试",
  MEDIA_PROBE_FAILED: "音频处理失败，请稍后重试",
};

export function failureMessageFor(code: MediaFailureCode): string {
  return FAILURE_MESSAGES[code];
}
