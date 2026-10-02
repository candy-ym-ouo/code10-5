import { z } from "zod";

const envSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  DATABASE_URL: z.string().min(1),
  REDIS_URL: z.string().url(),
  S3_ENDPOINT: z.string().url(),
  S3_REGION: z.string().min(1).default("us-east-1"),
  S3_BUCKET: z.string().min(1).default("practice-audio"),
  S3_ACCESS_KEY: z.string().min(1),
  S3_SECRET_KEY: z.string().min(1),
  S3_FORCE_PATH_STYLE: z.string().optional().transform((value) => value == null || value.toLowerCase() === "true"),
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"]).default("info"),
  WORKER_CONCURRENCY: z.coerce.number().int().min(1).max(32).default(3),
  // 音频下载：整体超时与空闲超时（毫秒）
  DOWNLOAD_TOTAL_TIMEOUT_MS: z.coerce.number().int().min(5_000).max(600_000).default(60_000),
  DOWNLOAD_IDLE_TIMEOUT_MS: z.coerce.number().int().min(2_000).max(120_000).default(15_000),
  // ffprobe / ffmpeg 分级超时（毫秒）
  PROBE_TIMEOUT_MS: z.coerce.number().int().min(2_000).max(300_000).default(30_000),
  PEAKS_TIMEOUT_MS: z.coerce.number().int().min(5_000).max(600_000).default(90_000),
  // 自动重试
  PROBE_MAX_ATTEMPTS: z.coerce.number().int().min(1).max(10).default(4),
  // PROCESSING 卡死恢复：超过该时长未更新即视为崩溃遗留
  STALE_PROCESSING_MS: z.coerce.number().int().min(30_000).max(6 * 60 * 60_000).default(10 * 60_000),
  // BullMQ stalled 检测间隔与最大 stalled 次数（兜底，主恢复靠 stale sweep）
  STALLED_INTERVAL_MS: z.coerce.number().int().min(10_000).max(600_000).default(30_000),
  MAX_STALLED_COUNT: z.coerce.number().int().min(1).max(10).default(3),
});

export type WorkerConfig = z.infer<typeof envSchema>;

let cached: WorkerConfig | undefined;

export function getConfig(): WorkerConfig {
  if (cached) return cached;
  const result = envSchema.safeParse(process.env);
  if (!result.success) {
    throw new Error(result.error.issues.map((issue) => `${issue.path.join(".")}: ${issue.message}`).join("; "));
  }
  cached = result.data;
  return result.data;
}
