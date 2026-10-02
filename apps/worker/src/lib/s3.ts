import { GetObjectCommand, PutObjectCommand, S3Client, DeleteObjectCommand } from "@aws-sdk/client-s3";
import type { Readable } from "node:stream";
import { getConfig } from "../config/env.js";

let client: S3Client | undefined;

export function getS3(): S3Client {
  if (!client) {
    const config = getConfig();
    client = new S3Client({
      endpoint: config.S3_ENDPOINT,
      region: config.S3_REGION,
      forcePathStyle: config.S3_FORCE_PATH_STYLE,
      credentials: { accessKeyId: config.S3_ACCESS_KEY, secretAccessKey: config.S3_SECRET_KEY },
    });
  }
  return client;
}

export interface DownloadTimeouts {
  /** 整个下载允许的最长时间 */
  totalTimeoutMs: number;
  /** 两次数据块之间允许的最大间隔，防止挂死的连接无限等待 */
  idleTimeoutMs: number;
}

export async function getObjectStream(objectKey: string): Promise<Readable> {
  const object = await getS3().send(new GetObjectCommand({ Bucket: getConfig().S3_BUCKET, Key: objectKey }));
  if (!object.Body) throw new Error("object body is empty");
  return object.Body as Readable;
}

/**
 * 下载对象并写入 destination，带整体超时与空闲超时。
 * 超时会销毁底层流并抛出 DOWNLOAD_TIMEOUT / DOWNLOAD_STALLED，
 * 由调用方按可重试错误分级；其他 S3 错误（如 NoSuchKey）原样抛出。
 */
export async function downloadObject(
  objectKey: string,
  destination: NodeJS.WritableStream,
  timeouts: DownloadTimeouts,
): Promise<void> {
  const controller = new AbortController();
  let idleTimer: NodeJS.Timeout | undefined;
  const fail = (code: string, message: string) => {
    const error = new Error(message);
    error.name = code;
    controller.abort(error);
    return error;
  };
  const totalTimer = setTimeout(
    () => controller.abort(fail("DOWNLOAD_TIMEOUT", `download exceeded ${timeouts.totalTimeoutMs}ms`)),
    timeouts.totalTimeoutMs,
  );
  const clearIdle = () => {
    if (idleTimer) clearTimeout(idleTimer);
  };
  const resetIdle = () => {
    clearIdle();
    idleTimer = setTimeout(
      () => controller.abort(fail("DOWNLOAD_STALLED", `download idle for ${timeouts.idleTimeoutMs}ms`)),
      timeouts.idleTimeoutMs,
    );
  };

  try {
    const object = await getS3().send(
      new GetObjectCommand({ Bucket: getConfig().S3_BUCKET, Key: objectKey }),
      { abortSignal: controller.signal },
    );
    if (!object.Body) throw new Error("object body is empty");
    const body = object.Body as Readable;
    resetIdle();
    body.on("data", resetIdle);
    // 用超时错误销毁流，使 await 得到可分级的错误码而非笼统的 AbortError
    controller.signal.addEventListener("abort", () => {
      const reason = (controller.signal as { reason?: unknown }).reason;
      body.destroy(reason instanceof Error ? reason : new Error("download aborted"));
    });

    await new Promise<void>((resolve, reject) => {
      destination.on("finish", resolve);
      destination.on("error", reject);
      body.on("error", reject);
      body.pipe(destination);
    });
  } finally {
    clearTimeout(totalTimer);
    clearIdle();
  }
}

export async function putObject(objectKey: string, body: string, contentType: string): Promise<void> {
  await getS3().send(
    new PutObjectCommand({
      Bucket: getConfig().S3_BUCKET,
      Key: objectKey,
      Body: body,
      ContentType: contentType,
    }),
  );
}

export async function deleteObject(objectKey: string): Promise<void> {
  await getS3().send(new DeleteObjectCommand({ Bucket: getConfig().S3_BUCKET, Key: objectKey }));
}
