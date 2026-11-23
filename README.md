# Cutline — long videos in, shorts out

A local clipping studio: drop in a long video, and it transcribes the audio, picks the moments
worth posting, and renders vertical 9:16 clips with burned-in captions. Runs on your machine —
no upload to a service, no account, no queue.

```
npm install        # once: transformers.js + onnxruntime
npm start          # http://127.0.0.1:4180
```

Requires Node 22+, `ffmpeg` and `ffprobe` on your PATH. The first transcription downloads a
~150 MB Whisper model into your Hugging Face cache; after that it works offline.

YouTube import needs `yt-dlp`: `pip install yt-dlp`. The app detects it at startup and disables the
link field when it is missing. Private, age-restricted or region-locked videos are refused by
YouTube itself — the error is surfaced verbatim.

## Screens

| | |
| --- | --- |
| ![Landing](screenshots/01-landing.png) | ![Projects](screenshots/02-projects.png) |
| ![YouTube import](screenshots/05-import.png) | ![YouTube clips](screenshots/06-youtube-clips.png) |
| ![Clips](screenshots/03-clips.png) | ![Clip detail](screenshots/04-clip.png) |

## What it does

1. **Import** — upload an MP4 / MOV / WebM (up to 2 GB), or paste a YouTube link. For links only the
   **audio** is downloaded (a six hour stream is ~150 MB), and each chosen clip is fetched
   individually with `--download-sections`, so the full video never lands on disk.
   Long streams can be limited to a window (`from` / `to` in the link box) so only that audio is
   transcribed.
2. **Transcribe** — audio is extracted to 16 kHz mono and passed through Whisper
   (`Xenova/whisper-base.en` via transformers.js) with word-level timestamps.
3. **Rank** — the transcript is split into sentences, then either a model (OpenAI, Anthropic,
   Gemini) picks the best self-contained passages, or the built-in scorer does it locally.
4. **Render** — each moment becomes a 1080×1920 MP4: blurred fill backdrop, the source framed in
   the middle, a two-line hook title at the top and three-word captions burned in.
5. **Review** — clips appear in the project with thumbnails, title, score and reason. Play,
   download, copy the caption, or cut your own range from the timeline.

## Ranking engines

| Engine | Needs | Behaviour |
| --- | --- | --- |
| Local scoring | nothing | Ranks 18–55 s sentence windows by hook words, questions, numbers, contrasts, speech density and completeness. Free, offline, occasionally blunt. |
| OpenAI / Anthropic / Gemini | an API key in **Settings** | One call per video; the model returns start/end, title, hook, score and reason. Falls back to local scoring if the call fails. |

The key is written to `data/settings.json` on this machine and only ever sent to the provider you
pick. With no key, nothing leaves the machine except nothing at all.

## Files

```
cutline/
  server.mjs              HTTP server, upload, SSE, media
  lib/pipeline.mjs        probe → audio → transcript → rank → render
  lib/asr.mjs             Whisper via transformers.js
  lib/rank.mjs            model ranking + local scorer
  lib/render.mjs          ffmpeg: vertical layout, drawtext title and captions
  lib/store.mjs           projects and settings on disk
  public/landing.html     marketing page
  public/app.html         dashboard
  data/projects/<id>/     source, transcript.json, clips/, thumbs/
```

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/state` | projects, settings, providers |
| `POST` | `/api/upload` | raw body + `x-filename` header |
| `POST` | `/api/import` | `{ url }` — YouTube link, downloaded with yt-dlp |
| `GET` | `/api/projects/:id` | one project with transcript and clips |
| `POST` | `/api/projects/:id/retry` | re-run ranking and rendering (reuses the transcript) |
| `POST` | `/api/projects/:id/cut` | `{ start, end, title }` → one hand-cut clip |
| `DELETE` | `/api/projects/:id` | remove the project and its files |
| `POST` | `/api/settings` | provider, model, key, clip count, accent colour |
| `GET` | `/api/events` | SSE: `project`, `delete` |
| `GET` | `/media/:projectId/*` | source video, clips, thumbnails |

## Notes and limits

- One job at a time. Whisper on CPU runs around 2–3× faster than real time on a laptop, so a
  10 minute video takes roughly 3–5 minutes to transcribe, then ~40 s per clip to render.
- YouTube imports are capped at six hours of *source*. What actually costs time is transcription
  (~0.7 s of CPU per second of audio), so for a multi-hour stream use the `from`/`to` window —
  only that slice is downloaded and transcribed. Live streams and members-only/age-gated videos
  are refused before anything is downloaded.
- Clips are always 1080×1920. There is no face tracking: the frame is centred and the backdrop is
  a blurred fill, which keeps landscape footage usable without cropping people in half.
- English transcription (`base.en`). Override with `CUTLINE_ASR_MODEL=Xenova/whisper-small` for
  better accuracy at ~3× the cost in disk and time.
- Titles and captions come from the transcript; expect the usual speech-to-text wobble on names.
- Cutline is an independent project. It is not affiliated with, endorsed by, or a copy of OpusClip;
  it implements the same broad idea (long video → vertical clips) with its own code and design.
