# Twitter Bot Scheduler (Cloudflare Durable Objects)

>
> This is personal software that may not be supported for your use case. The scheduling system is very diverse and can be adapted to most scheduling needs beyond X bots.
>

Durable Object–based scheduling and execution engine for Twitter/X bots. This Worker hosts a per-bot scheduler that:

- Only runs exactly when tasks are scheduled to run, zero cost for idle or waiting tasks.
- Maintains isolated, stateful schedulers per bot (one Durable Object instance per `botId`)
- Schedules recurring tasks with priorities, dependencies, retries, and locks
- Integrates with D1 for persistence (tasks, executions, logs, rate limits)
- Streams live status via WebSockets to the UI
- Coordinates with the shared library for API access, repositories, and services

See the shared library [companion repo](https://github.com/darwinmiller/x-bot-lib) for the Twitter client, repositories, services, and types used here.

---

## Why Durable Objects

- Strong single-threaded consistency: each bot’s scheduler has its own DO instance, eliminating cross-bot contention and race conditions.
- Built-in state and alarms: stable scheduling timers without external cron infra.
- Horizontal scalability: DO routing guarantees all actions for a given `botId` land on the same instance.
- Simpler locking: per-instance processing means fewer global locks. Where needed, we use DB-backed locks in the shared lib.
- Low-latency push: live status/log events via DO WebSockets to the UI.

---

## Architecture

- Entry Worker (`src/index.ts`) routes `fetch` requests to a `BotScheduler` instance by `botId`.
- `BotScheduler` (Durable Object) keeps in-memory state (`isRunning`, `currentAlarmTime`, `botId`) and persists a copy via DO storage.
- The DO orchestrates tasks using `TaskService` (from the shared lib), which writes to D1.
- Task execution delegates to handlers in `src/tasks/*` which use services from `x-bot-lib`:
  - Pull mentions -> persist posts/media/users (`PostPullService`)
  - Generate replies with LLMs (`LLMService` → OpenAI/Vertex)
  - Send replies and Discord notifications (`PostReplyService` + `DiscordService`)
  - Refresh user data (`TwitterUserService`)
- Rate limiting is enforced via `RateLimitService` + `RateLimitTrackingRepository` in the shared lib.

---

## HTTP API

All endpoints are namespaced by bot ID.

- `POST /bots/:botId/start` – Start the scheduler for this bot. Schedules next alarm if tasks exist.
- `POST /bots/:botId/stop` – Stop and clear the alarm.
- `GET  /bots/:botId/status` – Returns `{ isRunning, nextAlarmTime }` and self-heals if needed.

Responses are JSON. Non-2xx include a minimal `{ error }` message.

---

## WebSockets

Upgrade to WebSocket on the same DO to receive live events:

- `connection_ack` – Initial handshake with current scheduler status
- `scheduler_status_update` – Running flag and next alarm timestamp updates
- `entity_update` – CRUD-style events for `task_execution` and `task_definition`
- `log` – Streaming logs with `level`, `content`, and `botId`

These events back the UI dashboard and can also be consumed by other monitoring tools.

---

## Tasks and Handlers

Handlers live in `src/tasks/*` and are registered via `initializeTaskHandlers`:

- `PullMentionsTaskHandler` (`pull_mentions`)
- `ReplyGenerationTaskHandler` (`generate_replys`)
- `ReplyTaskHandler` (`reply_to_mentions`)
- `UserFetchTaskHandler` (`user_fetch`)

Each handler uses shared-lib services and repositories to do real work. The DO coordinates:

- Dependencies: ensure prerequisite tasks have run recently
- Rate limits: check available headroom before executing
- Executions: create/update `bot_task_executions` with duration, attempts, errors
- Retries: backoff and reschedule according to task config

Add a new task type:

1) Create a handler in `src/tasks/YourTask.ts` implementing `TaskHandler`.
2) Register it in `src/tasks/taskHandlers.ts`.
3) Define a `bot_task_definitions` row (via UI or SQL) with `task_type`, interval, priority, etc.

---

## Rate Limiting

This worker defers rate limiting logic to the shared lib:

- `RateLimiter` (static plan limits per endpoint)
- `RateLimitService` (persistence + enforcement per bot and endpoint)
- `RateLimitTrackingRepository` (D1 table `rate_limit_tracking`)

Handlers should set `rate_limit_endpoint` on task definitions and optionally `min_rate_limit_remaining`. The scheduler checks usage before execution and records usage after successful API calls.

---

## Local Development

Prerequisites:

- Node 18+ or Bun
- Cloudflare Wrangler (`npm i -g wrangler`) and a D1 database

Install and run the monorepo:

```bash
# In shared lib
cd x-bot-lib
bun install && bun run build

# In scheduler
cd ../twitter-bot
bun install
bun run dev
```

Environment/bindings:

- `DB` – D1 binding (configured in `wrangler.jsonc`)
- `BOT_SCHEDULER` – Durable Object binding
- `TWITTER_CLIENT_ID`, `TWITTER_CLIENT_SECRET` – used by the shared lib to refresh OAuth tokens

Schema:

- Run the SQL migrations in `twitter-bot-ui/migrations/` against your D1 instance. The shared lib repositories expect those tables.

---

## Deployment

```bash
bun run build   # optional: typegen/build steps
bun run deploy  # wrangler deploy
```

Wrangler configuration lives in `wrangler.jsonc` (compatibility date/flags, DO class, D1 binding).

---

## Observability

- WebSocket stream for UI live updates
- Structured logs persisted via `LoggerService` → `logs` table
- Task stats: query `bot_task_executions` for durations, error rates, attempts

Consider forwarding logs to external sinks or adding alerts on error thresholds.

---

## Integration with the Shared Library

This worker relies heavily on `x-bot-lib` for:

- Twitter API client with auth/refresh and rate limiting
- D1 repositories for all data models
- High-level services (post pulls, replies, LLM generation, users, media, tasks, Discord)

See `../x-bot-lib/README.md` for usage examples, provider setup (OpenAI/Vertex), and repository details.

---

## API Quick Test

```bash
# Start scheduler for a bot
curl -X POST http://127.0.0.1:8787/bots/<BOT_ID>/start

# Check status
curl http://127.0.0.1:8787/bots/<BOT_ID>/status

# Stop
curl -X POST http://127.0.0.1:8787/bots/<BOT_ID>/stop
```

---

## FAQ

- Why DOs over cron? DOs give per-bot single-threaded state, built-in alarms, and easy fanout across many bots without shared locks.
- Can I add custom tasks? Yes—add a handler, register it, and create a task definition row.
- Does this post to Twitter? Yes—handlers call into the shared lib’s `TwitterClient` and services.
- Is OAuth handled here? Refresh logic lives in the shared lib; this worker supplies `TWITTER_CLIENT_ID/SECRET` and bot tokens in D1.

