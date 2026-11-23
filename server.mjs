import { createServer } from "node:http";
import { createWriteStream } from "node:fs";
import { mkdir, readFile, rm, stat } from "node:fs/promises";import { extname, join, normalize, resolve } from "node:path";
import { randomUUID } from "node:crypto";
import { pipeline as streamPipeline } from "node:stream/promises";

import { Store } from "./lib/store.mjs";
import { providerList } from "./lib/rank.mjs";
import { queuePipeline, isRunning } from "./lib/pipeline.mjs";
import { probe, renderClip, renderThumbnail, extractCover, extractAudio } from "./lib/render.mjs";
import { downloadAudio, downloadSegment, fetchMetadata, parseYouTubeUrl, ytDlpAvailable } from "./lib/youtube.mjs";
import { PROJECTS_DIR } from "./lib/store.mjs";

const PORT = Number(process.env.PORT ?? 4180);
const HOST = process.env.HOST ?? "127.0.0.1";
const PUBLIC = join(import.meta.dirname, "public");
const MAX_UPLOAD = 2 * 1024 * 1024 * 1024;
/** Longest YouTube import we will handle. Audio-only, so length is disk-cheap. */
const MAX_IMPORT_SECONDS = 6 * 60 * 60;
/** Measured on a laptop CPU: roughly 0.7 s of Whisper per second of audio. */
const estimateTranscriptionMs = (seconds) => Math.round((Number(seconds) || 0) * 700);

const CONTENT_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".mp4": "video/mp4",
  ".webm": "video/webm",
  ".mov": "video/quicktime",
};

const store = await Store.open();
const ytDlpVersion = await ytDlpAvailable();
const clients = new Set();

// A crash or restart leaves jobs mid-flight; surface them instead of showing a
// frozen progress bar. Retry reuses the transcript when one already exists.
for (const project of [...store.projects]) {
  if (["queued", "downloading", "transcribing", "ranking", "rendering"].includes(project.status)) {
    await store.update(project.id, {
      status: "error",
      stage: "Interrupted",
      error: "The app restarted while this job was running. Press Try again to resume.",
    });
  }
}

function broadcast(payload) {
  const frame = `data: ${JSON.stringify(payload)}\n\n`;
  for (const client of clients) client.write(frame);
}

const json = (response, status, value) => {
  const body = JSON.stringify(value);
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "content-length": Buffer.byteLength(body),
  });
  response.end(body);
};

const publicProject = (project) => ({
  ...project,
  transcript: project.transcript
    ? { segments: project.transcript.segments?.slice(0, 400) ?? [] }
    : undefined,
});

const publicSettings = () => ({  provider: store.settings.provider,
  model: store.settings.model,
  clipCount: store.settings.clipCount,
  accent: store.settings.accent,
  style: store.settings.style,
  platform: store.settings.platform,
  keySet: Boolean(store.settings.key),
});

async function readJson(request) {
  let body = "";
  for await (const chunk of request) body += chunk;
  if (!body.trim()) return {};
  return JSON.parse(body);
}

function safeJoin(base, relative) {
  const target = resolve(base, `.${normalize(`/${relative}`)}`);
  return target.startsWith(resolve(base)) ? target : null;
}

/** A frame past the intro, so cards do not show a black title card. */
const coverTime = (duration) => Math.min(Math.max(1, (Number(duration) || 0) * 0.12), 10);

const clockLabel = (seconds) => {
  const total = Math.max(0, Math.round(Number(seconds) || 0));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = String(total % 60).padStart(2, "0");
  return hours ? `${hours}:${String(minutes).padStart(2, "0")}:${secs}` : `${minutes}:${secs}`;
};

function serveFile(response, file) {
  return stat(file).then(async (info) => {
    const type = CONTENT_TYPES[extname(file).toLowerCase()] ?? "application/octet-stream";
    const headers = { "content-type": type, "content-length": info.size, "cache-control": "no-cache" };
    response.writeHead(200, headers);
    response.end(await readFile(file));
  });
}

async function handleUpload(request, response) {
  const name = decodeURIComponent(request.headers["x-filename"] ?? "video.mp4");
  const size = Number(request.headers["content-length"] ?? 0);
  if (size > MAX_UPLOAD) return json(response, 413, { error: "That file is larger than 2 GB." });
  if (!size) return json(response, 400, { error: "Empty upload." });

  const id = randomUUID().slice(0, 8);
  const dir = join(PROJECTS_DIR, id);
  await mkdir(dir, { recursive: true });
  const extension = (extname(name) || ".mp4").toLowerCase();
  const sourceFile = `source${extension}`;

  await streamPipeline(request, createWriteStream(join(dir, sourceFile)));

  const media = await probe(join(dir, sourceFile)).catch(() => ({ duration: 0, width: 0, height: 0, hasAudio: false }));
  await extractCover({
    input: join(dir, sourceFile),
    output: join(dir, "cover.jpg"),
    at: coverTime(media.duration),
  }).catch(() => {});
  const project = {
    id,
    name: name.replace(/\.[^.]+$/, "").slice(0, 120) || "Untitled video",
    sourceFile,
    sizeBytes: size,
    duration: media.duration,
    width: media.width,
    height: media.height,
    hasAudio: media.hasAudio,
    status: "queued",
    progress: 0.01,
    stage: "Queued",
    clips: [],
    engine: null,
    error: null,
    createdAt: Date.now(),
    updatedAt: Date.now(),
  };
  await store.add(project);
  broadcast({ type: "project", project: publicProject(project) });
  json(response, 201, { project: publicProject(project) });

  setTimeout(() => {
    void queuePipeline({ project: store.find(id), store, settings: store.settings, broadcast });
  }, 80);
}

/**
 * Paste a YouTube link: yt-dlp fetches the video into the project folder, then
 * the usual transcribe → rank → render pipeline takes over.
 */
async function handleImport(request, response) {
  if (!ytDlpVersion) {
    return json(response, 503, { error: "YouTube import needs yt-dlp. Install it with: pip install yt-dlp" });
  }
  const body = await readJson(request);
  const parsed = parseYouTubeUrl(body.url);
  if (!parsed) return json(response, 400, { error: "That does not look like a YouTube link." });

  const id = randomUUID().slice(0, 8);
  const dir = join(PROJECTS_DIR, id);
  await mkdir(dir, { recursive: true });
  const project = {
    id,
    name: "YouTube import",
    sourceFile: null,
    importUrl: parsed.url,
    sizeBytes: 0,
    duration: 0,
    width: 0,
    height: 0,
    hasAudio: true,
    status: "downloading",
    progress: 0.01,
    stage: "Fetching title",
    clips: [],
    engine: null,
    error: null,
    createdAt: Date.now(),
    updatedAt: Date.now(),
  };
  await store.add(project);
  broadcast({ type: "project", project: publicProject(project) });
  json(response, 201, { project: publicProject(project) });

  void runImportJob({
    id,
    url: parsed.url,
    dir,
    start: Number(body.start) || 0,
    end: Number(body.end) || null,
  });
}

/** Download → probe → cover → hand off to the clip pipeline. */
async function runImportJob({ id, url, dir, start = 0, end = null }) {
  let lastWrite = 0;
  const progress = async (patch, force = false) => {
    const now = Date.now();
    if (!force && now - lastWrite < 700) return;
    lastWrite = now;
    const next = await store.update(id, patch);
    broadcast({ type: "project", project: publicProject(next) });
  };
  try {
    const meta = await fetchMetadata(url);
    if (meta.isLive) {
      throw new Error("That link is a live stream. Cutline needs a finished video with a fixed length.");
    }
    const from = Math.max(0, Number(start) || 0);
    const to = Number(end) > from ? Math.min(Number(end), meta.duration || Number(end)) : null;
    if (!to && meta.duration && meta.duration > MAX_IMPORT_SECONDS) {
      throw new Error(
        `That video is ${(meta.duration / 3600).toFixed(1)} hours long. Analyse a window instead — set “from” and “to” in the link box (for example 0:20 and 0:50).`,
      );
    }
    const windowSeconds = to ? to - from : meta.duration ?? 0;
    await progress(
      {
        name: meta.title,
        duration: meta.duration ?? 0,
        audioOffset: from,
        analysisSeconds: windowSeconds,
        stage: to ? `Downloading audio ${clockLabel(from)}–${clockLabel(to)}` : "Downloading audio",
        progress: 0.03,
      },
      true,
    );

    // Audio only: a six hour stream is ~150 MB of audio, not a multi-GB video.
    const { file: audioFile } = await downloadAudio({
      url,
      dir,
      start: to ? from : undefined,
      end: to ?? undefined,
      onProgress: (fraction) =>
        void progress({ progress: 0.03 + fraction * 0.1, stage: `Downloading audio ${Math.round(fraction * 100)}%` }),
    });
    await progress({ stage: "Preparing audio", progress: 0.13 }, true);
    const wav = join(dir, "audio.wav");
    await extractAudio({ input: join(dir, audioFile), output: wav });
    await rm(join(dir, audioFile), { force: true }).catch(() => {});

    const estimated = estimateTranscriptionMs(windowSeconds);
    const updated = await store.update(id, {
      lazyVideo: true,
      audioFile: "audio.wav",
      audioOffset: from,
      sizeBytes: (await stat(wav).catch(() => ({ size: 0 }))).size,
      duration: meta.duration ?? 0,
      hasAudio: true,
      status: "queued",
      progress: 0.15,
      stage: estimated > 60_000 ? `Audio ready · transcription will take ~${Math.round(estimated / 60_000)} min` : "Audio ready",
    });
    broadcast({ type: "project", project: publicProject(updated) });
    void queuePipeline({ project: store.find(id), store, settings: store.settings, broadcast });
  } catch (cause) {
    console.error("import failed:", cause);
    const failed = await store.update(id, {
      status: "error",
      stage: "Import failed",
      error: cause.message ?? String(cause),
    });
    broadcast({ type: "project", project: publicProject(failed) });
  }
}

async function handleCut(request, response, project) {
  const body = await readJson(request);
  const start = Math.max(0, Number(body.start));
  const end = Math.min(project.duration || Number(body.end), Number(body.end));
  if (!(end - start >= 3)) return json(response, 400, { error: "A clip needs to be at least 3 seconds." });

  const transcript = JSON.parse(
    await readFile(join(PROJECTS_DIR, project.id, "transcript.json"), "utf8").catch(() => '{"words":[]}'),
  );
  const clipId = randomUUID().slice(0, 8);
  const dir = join(PROJECTS_DIR, project.id);

  let input = join(dir, project.sourceFile ?? "");
  let timeOffset = 0;
  if (project.lazyVideo && project.importUrl) {
    const segmentsDir = join(dir, "segments");
    await mkdir(segmentsDir, { recursive: true });
    const from = Math.max(0, start - 2);
    const to = Math.min(project.duration || end + 2, end + 2);
    const { file } = await downloadSegment({
      url: project.importUrl,
      dir: segmentsDir,
      start: from,
      end: to,
      name: clipId,
    });
    input = join(segmentsDir, file);
    timeOffset = from;
  }

  const renderStart = start - timeOffset;
  const renderEnd = end - timeOffset;

  await renderClip({
    input,
    output: join(dir, "clips", `${clipId}.mp4`),
    workDir: join(dir, "work", clipId),
    start: renderStart,
    end: renderEnd,
    title: String(body.title ?? "Custom clip").slice(0, 80),
    words: transcript.words ?? [],
    accent: store.settings.accent,
    timeOffset,
    style: store.settings.style ?? "classic",
  });
  await renderThumbnail({
    clipPath: join(dir, "clips", `${clipId}.mp4`),
    output: join(dir, "thumbs", `${clipId}.jpg`),
  });
  const clip = {
    id: clipId,
    title: String(body.title ?? "Custom clip").slice(0, 80),
    hook: "",
    reason: "Cut by hand from the timeline.",
    score: 0,
    start,
    end,
    duration: Number((end - start).toFixed(2)),
    file: `clips/${clipId}.mp4`,
    thumb: `thumbs/${clipId}.jpg`,
    createdAt: Date.now(),
  };
  project.clips = [...(project.clips ?? []), clip];
  await store.saveProjects();
  broadcast({ type: "project", project: publicProject(project) });
  json(response, 200, { clip });
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
  const path = url.pathname;

  try {
    if (path === "/api/state") {
      return json(response, 200, {
        projects: store.projects.map(publicProject),
        settings: publicSettings(),
        providers: providerList(),
        youtube: { available: Boolean(ytDlpVersion), version: ytDlpVersion },
      });
    }

    if (path === "/api/events") {
      response.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
      });
      response.write(`data: ${JSON.stringify({ type: "hello" })}\n\n`);
      clients.add(response);
      const beat = setInterval(() => response.write(": ping\n\n"), 20_000);
      request.on("close", () => {
        clearInterval(beat);
        clients.delete(response);
      });
      return;
    }

    if (path === "/api/upload" && request.method === "POST") return handleUpload(request, response);

    if (path === "/api/import" && request.method === "POST") return await handleImport(request, response);

    if (path === "/api/settings" && request.method === "POST") {
      const body = await readJson(request);
      if (typeof body.provider === "string") store.settings.provider = body.provider;
      if (typeof body.model === "string") store.settings.model = body.model;
      if (typeof body.accent === "string") store.settings.accent = body.accent;
      if (body.clipCount !== undefined) store.settings.clipCount = Math.max(1, Math.min(Number(body.clipCount) || 3, 6));
      if (typeof body.key === "string") store.settings.key = body.key.trim();
      if (typeof body.style === "string") store.settings.style = body.style;
      if (typeof body.platform === "string") store.settings.platform = body.platform;
      await store.saveSettings();
      return json(response, 200, publicSettings());
    }

    const projectMatch = /^\/api\/projects\/([a-f0-9]{8})(?:\/(retry|cut))?$/.exec(path);
    if (projectMatch) {
      const project = store.find(projectMatch[1]);
      if (!project) return json(response, 404, { error: "No such project." });
      const action = projectMatch[2];
      if (!action && request.method === "GET") return json(response, 200, { project: publicProject(project) });
      if (action === "cut" && request.method === "POST") return await handleCut(request, response, project);
      if (action === "retry" && request.method === "POST") {
        if (isRunning(project.id)) return json(response, 409, { error: "That project is already running." });
        // An import that never finished downloading has no audio yet: re-fetch it.
        if (!project.sourceFile && !project.audioFile && project.importUrl) {
          if (!ytDlpVersion) {
            return json(response, 503, { error: "YouTube import needs yt-dlp. Install it with: pip install yt-dlp" });
          }
          await store.update(project.id, {
            status: "downloading",
            stage: "Retrying download",
            error: null,
            progress: 0.01,
            clips: [],
          });
          json(response, 202, { project: publicProject(store.find(project.id)) });
          void runImportJob({ id: project.id, url: project.importUrl, dir: join(PROJECTS_DIR, project.id) });
          return;
        }
        await store.update(project.id, { status: "queued", error: null, progress: 0.01, stage: "Queued", clips: [] });
        json(response, 202, { project: publicProject(store.find(project.id)) });
        setTimeout(() => {
          void queuePipeline({ project: store.find(project.id), store, settings: store.settings, broadcast });
        }, 80);
        return;
      }
      if (request.method === "DELETE") {
        await store.remove(project.id);
        broadcast({ type: "delete", id: project.id });
        return json(response, 200, { ok: true });
      }
      return json(response, 405, { error: "Method not allowed." });
    }

    const mediaMatch = /^\/media\/([a-f0-9]{8})\/(.+)$/.exec(path);
    if (mediaMatch) {
      const project = store.find(mediaMatch[1]);
      if (!project) return response.writeHead(404).end("Not found");
      const file = safeJoin(join(PROJECTS_DIR, project.id), mediaMatch[2]);
      if (!file) return response.writeHead(403).end("Forbidden");
      try {
        return await serveFile(response, file);
      } catch {
        return response.writeHead(404).end("Not found");
      }
    }

    if (path.startsWith("/api/")) return json(response, 404, { error: "Unknown endpoint." });

    const route = path === "/" ? "/landing.html" : path === "/app" ? "/app.html" : path;
    const file = safeJoin(PUBLIC, route);
    if (!file) return response.writeHead(403).end("Forbidden");
    try {
      return await serveFile(response, file);
    } catch {
      return response.writeHead(404, { "content-type": "text/plain" }).end("Not found");
    }
  } catch (cause) {
    console.error(cause);
    if (!response.headersSent) json(response, 500, { error: cause.message ?? "Server error" });
    else response.end();
  }
});

server.listen(PORT, HOST, () => {
  console.log(`Cutline at http://${HOST}:${PORT}`);
  console.log(store.settings.key ? "Model ranking enabled." : "No API key: clips will be picked by local scoring.");
});
