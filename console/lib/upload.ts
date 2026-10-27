// Browser uploads through the admin API: one request for small files, a
// multipart upload (several parts in flight) for large ones. XMLHttpRequest,
// because fetch cannot report upload progress.

import { api, ApiError, CONSOLE_HEADER, paths, toSignIn } from "./api";

const SINGLE_LIMIT = 32 * 1024 * 1024;
const MIN_PART = 16 * 1024 * 1024;
const MAX_PARTS = 10_000;
const PARALLEL = 3;

export type Progress = { sent: number; total: number; part?: { done: number; count: number } };

export type UploadHandle = { done: Promise<void>; cancel: () => void };

export function partSize(size: number): number {
  const mib = 1024 * 1024;
  return Math.max(MIN_PART, Math.ceil(size / MAX_PARTS / mib) * mib);
}

function put(url: string, body: Blob, contentType: string, onProgress: (loaded: number) => void, xhrs: Set<XMLHttpRequest>) {
  return new Promise<unknown>((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhrs.add(xhr);
    xhr.open("PUT", url);
    xhr.setRequestHeader("x-aether-console", CONSOLE_HEADER["x-aether-console"]);
    xhr.setRequestHeader("content-type", contentType);
    xhr.upload.onprogress = (e) => onProgress(e.loaded);
    xhr.onload = () => {
      xhrs.delete(xhr);
      let body: { error?: { code?: string; message?: string } } | null = null;
      try {
        body = xhr.responseText ? JSON.parse(xhr.responseText) : null;
      } catch {
        body = null;
      }
      if (xhr.status >= 200 && xhr.status < 300) {
        onProgress(0);
        resolve(body);
      } else {
        if (xhr.status === 401) toSignIn();
        reject(new ApiError(xhr.status, body?.error?.code ?? "Error", body?.error?.message ?? `HTTP ${xhr.status}`));
      }
    };
    xhr.onerror = () => {
      xhrs.delete(xhr);
      reject(new ApiError(0, "NetworkError", "The upload was interrupted."));
    };
    xhr.onabort = () => {
      xhrs.delete(xhr);
      reject(new ApiError(0, "Cancelled", "Cancelled"));
    };
    xhr.send(body);
  });
}

export function upload(bucket: string, key: string, file: File, onProgress: (p: Progress) => void): UploadHandle {
  const xhrs = new Set<XMLHttpRequest>();
  let cancelled = false;
  let uploadId: string | null = null;
  const contentType = file.type || "application/octet-stream";

  let lastReport = 0;
  const reportProgress = (p: Progress) => {
    const now = Date.now();
    if (p.sent === p.total || now - lastReport > 100) {
      lastReport = now;
      onProgress(p);
    }
  };

  const done = (async () => {
    if (file.size <= SINGLE_LIMIT) {
      await put(`/api/v1${paths.object(bucket, key)}`, file, contentType, (loaded) => reportProgress({ sent: loaded || file.size, total: file.size }), xhrs);
      reportProgress({ sent: file.size, total: file.size });
      return;
    }

    const size = partSize(file.size);
    const count = Math.ceil(file.size / size);
    const created = await api<{ upload_id: string }>(paths.uploads(bucket), {
      method: "POST",
      // The size lets the server refuse a file that breaks the quota before any part is sent.
      json: { key, content_type: contentType, size: file.size },
    });
    uploadId = created.upload_id;
    if (cancelled) throw new ApiError(0, "Cancelled", "Cancelled");

    const etags: string[] = new Array(count);
    const inFlight = new Map<number, number>();
    let finished = 0;
    let finishedBytes = 0;
    const report = () => {
      let sent = finishedBytes;
      for (const loaded of inFlight.values()) sent += loaded;
      reportProgress({ sent, total: file.size, part: { done: finished, count } });
    };

    let next = 0;
    const worker = async () => {
      while (next < count && !cancelled) {
        const index = next++;
        const start = index * size;
        const blob = file.slice(start, Math.min(file.size, start + size));
        inFlight.set(index, 0);
        const part = (await put(
          paths.part(bucket, uploadId!, index + 1, key),
          blob,
          "application/octet-stream",
          (loaded) => {
            inFlight.set(index, loaded);
            report();
          },
          xhrs,
        )) as { etag: string };
        inFlight.delete(index);
        etags[index] = part.etag;
        finished += 1;
        finishedBytes += blob.size;
        report();
      }
    };
    await Promise.all(Array.from({ length: Math.min(PARALLEL, count) }, worker));
    if (cancelled) throw new ApiError(0, "Cancelled", "Cancelled");
    await api(paths.complete(bucket, uploadId), {
      method: "POST",
      json: { key, parts: etags.map((etag, i) => ({ part_number: i + 1, etag })) },
    });
  })();

  // Give back the space of a failed or cancelled multipart upload.
  done.catch(() => {
    if (uploadId) api(paths.upload(bucket, uploadId, key), { method: "DELETE" }).catch(() => {});
  });

  return {
    done,
    cancel: () => {
      cancelled = true;
      for (const xhr of xhrs) xhr.abort();
    },
  };
}
