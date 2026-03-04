# YepAnywhere Server-Side Session Management Specification

> **Purpose:** Enable someone to replicate YA's server-side session management algorithm in their own environment — discovering agent sessions from multiple providers on disk, managing their lifecycle as supervised processes, streaming conversations to WebSocket clients at high fidelity, and calculating history from heterogeneous file formats. This is the server-side counterpart to the [Session Client Integration Specification](./session-client-integration-spec.md).

> **Key insight:** YA monitors ALL agent sessions on the system regardless of how they were started (VS Code, terminal, another supervisor) AND creates/manages its own. External sessions get read-only streaming via file watching; YA-started sessions get full interactive control.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Session Discovery: Scanning the Filesystem](#2-session-discovery)
3. [Provider Abstraction: Claude, Codex, Gemini](#3-provider-abstraction)
4. [Process Lifecycle: The Supervisor State Machine](#4-process-lifecycle)
5. [Concurrency & Idle Preemption: The Worker Queue](#5-concurrency)
6. [Message Flow: SDK to WebSocket Client](#6-message-flow)
7. [Stream Augmentation: Server-Side Rendering](#7-stream-augmentation)
8. [Late-Join Replay: The Dual-Bucket Buffer](#8-late-join-replay)
9. [History Loading & DAG Resolution](#9-history-loading)
10. [Pagination & Compaction Boundaries](#10-pagination)
11. [External Session Monitoring](#11-external-session-monitoring)
12. [Event Bus & File Watcher Infrastructure](#12-event-bus)
13. [API & WebSocket Protocol Reference](#13-api-reference)

---

## 1. Architecture Overview

The server is a layered pipeline that transforms filesystem events and SDK messages into real-time WebSocket streams:

```
┌───────────────────────────────────────────────────────────────────────┐
│  Filesystem                                                           │
│  ~/.claude/projects/  ~/.codex/sessions/  ~/.gemini/tmp/              │
├───────────────────────────────────────────────────────────────────────┤
│  FileWatcher (recursive fs.watch, 200ms debounce per file)            │
│  → emits FileChangeEvent with fileType, provider, changeType          │
├───────────────────────────────────────────────────────────────────────┤
│  EventBus (in-memory pub/sub, 17 event types)                         │
│  → broadcasts to all subscribers synchronously                        │
├───────────────────────────────────────────────────────────────────────┤
│  Session Discovery                        External Session Tracker    │
│  ProjectScanner (5s TTL cache)            (30s decay, batch parsing)  │
│  ├── Claude: JSONL in projects/           Detects non-owned sessions  │
│  ├── Codex: JSONL by date hierarchy       via file change events      │
│  └── Gemini: JSON by project hash                                     │
├───────────────────────────────────────────────────────────────────────┤
│  Supervisor                                                           │
│  Maps processId→Process and sessionId→processId                       │
│  WorkerQueue (FIFO, idle preemption)                                  │
│  Session ID resolution (temp→real)                                    │
├───────────────────────────────────────────────────────────────────────┤
│  Process (per-session wrapper)                                        │
│  State machine: in-turn → idle → waiting-input → hold → terminated    │
│  Dual-bucket message buffer (15s swap, ~30s replay)                   │
│  Tool approval flow (InputRequest → client response)                  │
├───────────────────────────────────────────────────────────────────────┤
│  Provider Abstraction                                                 │
│  AgentProvider interface → Claude SDK | Codex subprocess | Gemini CLI │
│  MessageQueue (AsyncGenerator bridge)                                 │
├───────────────────────────────────────────────────────────────────────┤
│  Streaming Pipeline                                                   │
│  Subscription handler → StreamAugmenter → emit() → WebSocket/Relay   │
│  Markdown rendering, diff computation, syntax highlighting            │
│  Three channels: session | activity | session-watch                   │
└───────────────────────────────────────────────────────────────────────┘
```

**Key design decisions:**

- **Provider-agnostic supervisor** — The same `Process` wrapper handles Claude SDK, Codex subprocess, and Gemini CLI. Adding a new provider means implementing one interface.
- **Filesystem-first discovery** — Sessions exist as files before YA knows about them. No central registry. Scan the filesystem, build the index.
- **Ownership duality** — Every session is either `self` (YA started it), `external` (another program is writing to it), or `none` (idle file on disk). All three coexist.
- **Memory-bounded replay** — Two 15-second rotating buffers (not unbounded). A long-running agent can produce thousands of messages; unbounded buffers would OOM.
- **Server-rendered augments** — Diffs, syntax highlighting, and markdown are computed server-side. Clients receive pre-rendered HTML. This keeps mobile clients thin and ensures consistency.

**Key source files:**
- `packages/server/src/supervisor/Supervisor.ts` — Process manager (~800 lines)
- `packages/server/src/supervisor/Process.ts` — Core process wrapper (~1600 lines)
- `packages/server/src/supervisor/ExternalSessionTracker.ts` — External session detection
- `packages/server/src/projects/scanner.ts` — Filesystem scanner
- `packages/server/src/watcher/EventBus.ts` — Event backbone

---

## 2. Session Discovery: Scanning the Filesystem {#2-session-discovery}

YA discovers sessions from all three providers without any central registry. Each provider stores sessions in a different location with a different format.

### 2.1 On-Disk Locations

| Provider | Base Directory | File Pattern | Format |
|----------|---------------|-------------|--------|
| Claude | `~/.claude/projects/{dirProjectId}/` | `{sessionId}.jsonl` | DAG-structured JSONL (parentUuid chains) |
| Codex | `~/.codex/sessions/{YYYY}/{MM}/{DD}/` | `rollout-{model}-{uuid}.jsonl` | Linear JSONL (session_meta + response_item + event_msg) |
| Gemini | `~/.gemini/tmp/{projectHash}/chats/` | `session-{id}.json` | Single JSON file with messages array |

**Claude directory variants:**
```
~/.claude/projects/
  ├── -home-user-myproject/              # Direct: encoded path (slash→hyphen)
  │   ├── abc123.jsonl                   # Main session
  │   └── agent-def456.jsonl             # Subagent session
  └── myhostname/                        # Hostname prefix (cross-machine)
      └── -home-user-myproject/
          └── ghi789.jsonl
```

Agent session files (prefixed `agent-`) are subagent conversations spawned by the Task tool. They live in the same directory as their parent session.

### 2.2 Project ID Encoding

Project IDs use two encoding schemes:

**URL-safe IDs** (base64url, reversible — used in APIs and events):
```typescript
encodeProjectId(path: string): UrlProjectId
  // "/home/user/project" → "L2hvbWUvdXNlci9wcm9qZWN0"
  return Buffer.from(path).toString("base64url");

decodeProjectId(id: UrlProjectId): string
  return Buffer.from(id, "base64url").toString("utf-8");
```

**Directory names** (slash-to-hyphen, lossy — used on disk by the Claude CLI):
```
"/home/user/project" → "-home-user-project"
```
This encoding is lossy because hyphens in the original path become ambiguous. When you need the real path, read the `cwd` field from the first few lines of the session file.

### 2.3 ProjectScanner: The Composite Scanner

`ProjectScanner` is the central entry point. It delegates to provider-specific scanners and merges results:

```typescript
class ProjectScanner {
  // Composite: scans Claude sessions directly, delegates Codex/Gemini
  private codexScanner: CodexSessionScanner;
  private geminiScanner: GeminiSessionScanner;

  // 5-second TTL snapshot cache with in-flight deduplication
  private cachedSnapshot: ProjectSnapshot | null;
  private cacheTimestamp: number;
  private inFlightScan: Promise<ProjectSnapshot> | null;
  private cacheTtlMs = 5000;

  async listProjects(): Promise<Project[]> {
    const snapshot = await this.getSnapshot();
    return snapshot.projects;
  }
}
```

**Snapshot structure:**
```typescript
interface ProjectSnapshot {
  projects: Project[];
  byId: Map<string, Project>;               // O(1) lookup by encoded ID
  bySessionDirSuffix: Map<string, Project>;  // O(1) cross-machine merge
  timestamp: number;
}
```

**Cache invalidation:** The 5-second TTL is deliberately short. File change events trigger client refreshes which call `getProjects()`, and caching prevents hammering the filesystem during bursts of file events.

### 2.4 Cross-Machine Session Merging

When Claude Code runs on multiple machines against the same project (e.g., local + SSH remote), sessions end up under different hostname directories. YA merges them into a single project:

```typescript
normalizeProjectPathForDedup(path: string): string
  // "/Users/kyle/dotfiles" → "kyle/dotfiles"
  // "/home/kyle/dotfiles"  → "kyle/dotfiles"  (matches!)
  // "/root/dotfiles"       → "root/dotfiles"
  // "/opt/shared/project"  → "/opt/shared/project" (unchanged)
```

**Merge algorithm:**
1. For each Claude project directory, extract the encoded project path
2. Normalize via `normalizeProjectPathForDedup()` — strips `/Users/` and `/home/` prefixes
3. If two directories normalize to the same key, merge them:
   - Aggregate `sessionCount` from both
   - Build `mergedSessionDirs` array for session loading
   - Prefer the local machine's path for new session creation
   - Use the latest `lastActivity` timestamp

### 2.5 Codex Session Scanning

Codex stores sessions in a date hierarchy. The scanner reads the first line of each `.jsonl` file to extract `session_meta`:

```typescript
// First line format:
{ "type": "session_meta", "payload": { "cwd": "/home/user/project", "timestamp": "...", "model": "..." } }
```

**Algorithm:**
1. Recursively find all `.jsonl` files under `~/.codex/sessions/`
2. Batch-read first lines (50 files per batch) to extract `cwd`
3. Group sessions by `cwd` → each unique `cwd` becomes a project
4. Session ID extracted from filename: UUID portion of `rollout-{model}-{uuid}.jsonl`

### 2.6 Gemini Session Scanning

Gemini uses a SHA-256 hash of the working directory as the project folder name:

```
~/.gemini/tmp/
  └── a1b2c3d4.../          # SHA-256(cwd)
      └── chats/
          └── session-*.json
```

**Hash-to-path resolution:** YA maintains a `GeminiProjectMap` that maps hashes to real paths. It's seeded from known Claude and Codex project paths. If a hash can't be resolved, the project is shown as `gemini:{hash_prefix}`.

**Scanning:** Batch-reads 20 project hash directories in parallel, finds all `session-*.json` files, extracts metadata from JSON content.

### 2.7 The Project Type

```typescript
interface Project {
  id: UrlProjectId;              // base64url of absolute path
  path: string;                  // absolute path (e.g., "/home/user/project")
  name: string;                  // directory name (e.g., "project")
  sessionCount: number;          // non-agent sessions only
  sessionDir: string;            // primary session directory on disk
  mergedSessionDirs?: string[];  // additional dirs from cross-machine duplicates
  activeOwnedCount: number;      // sessions owned by this server
  activeExternalCount: number;   // sessions controlled by external processes
  lastActivity: string | null;   // ISO timestamp
  provider: ProviderName;        // "claude" | "codex" | "gemini" | etc.
}
```

**Key source files:**
- `packages/server/src/projects/scanner.ts`
- `packages/server/src/projects/codex-scanner.ts`
- `packages/server/src/projects/gemini-scanner.ts`
- `packages/server/src/projects/paths.ts`

---

## 3. Provider Abstraction: Claude, Codex, Gemini {#3-provider-abstraction}

Every provider implements a single interface. The current `ProviderName` type includes `"claude" | "codex" | "codex-oss" | "gemini" | "gemini-acp" | "opencode"`. This spec covers the three primary providers; adding a new one means implementing this contract:

### 3.1 The AgentProvider Interface

```typescript
interface AgentProvider {
  readonly name: ProviderName;
  readonly displayName: string;
  readonly supportsPermissionMode: boolean;   // Can restrict tool usage
  readonly supportsThinkingToggle: boolean;   // Extended thinking support
  readonly supportsSlashCommands: boolean;    // CLI slash commands

  isInstalled(): Promise<boolean>;
  isAuthenticated(): Promise<boolean>;
  getAuthStatus(): Promise<AuthStatus>;
  startSession(options: StartSessionOptions): Promise<AgentSession>;
  getAvailableModels(): Promise<ModelInfo[]>;
}

interface StartSessionOptions {
  cwd: string;
  initialMessage?: UserMessage;
  resumeSessionId?: string;
  permissionMode?: PermissionMode;
  model?: string;
  thinking?: ThinkingConfig;
  effort?: EffortLevel;
  onToolApproval?: CanUseTool;
  executor?: string;              // SSH host for remote execution
  globalInstructions?: string;    // Appended to system prompt
}

interface AgentSession {
  iterator: AsyncIterableIterator<SDKMessage>;  // The message stream
  queue: MessageQueue;                           // For sending user messages
  abort: () => void;
  sessionId?: string;                            // May arrive later via messages
  steer?: (message: UserMessage) => Promise<boolean>;
  setMaxThinkingTokens?: (tokens: number | null) => Promise<void>;
  interrupt?: () => Promise<void>;
  supportedModels?: () => Promise<ModelInfo[]>;
  supportedCommands?: () => Promise<SlashCommand[]>;
  setModel?: (model?: string) => Promise<void>;
}
```

### 3.2 Claude Provider

**Execution model:** Single long-lived SDK `query()` call that returns an async iterator.

```typescript
// SDK call
const sdkQuery = query({
  prompt: queue.generator(),    // AsyncGenerator<SDKUserMessage>
  options: {
    cwd: effectiveCwd,
    resume: options.resumeSessionId,
    abortController,
    permissionMode,  // "default" or "plan"; "bypassPermissions" mapped to "default" (callback stays active)
    canUseTool: async (toolName, input, opts) => { ... },
    systemPrompt: { type: "preset", preset: "claude_code", append?: globalInstructions },
    includePartialMessages: true,
    model, thinking, effort,
    spawnClaudeCodeProcess?: remoteSpawnFn,  // For SSH remote execution
  }
});
```

**Message flow:**
1. `MessageQueue.push(userMessage)` → resolves waiting generator
2. `queue.generator()` yields `SDKUserMessage` to the SDK
3. SDK processes, calls tools, yields `SDKMessage` objects via async iterator
4. Messages are wrapped/normalized by `wrapIterator()` before reaching Process

**Tool approval:** The SDK calls `canUseTool(toolName, input, opts)` asynchronously during execution. The provider always passes this callback when `onToolApproval` is provided (which is always). YA's callback creates a `PendingToolApproval`, transitions Process to `waiting-input`, and waits for the client to respond.

**Permission mode bypass:** When `permissionMode === "bypassPermissions"`, the provider passes `"default"` to the SDK but **keeps the `canUseTool` callback active**. The bypass logic lives in `Process.handleToolApproval`, which auto-approves all tools EXCEPT `AskUserQuestion` and `ExitPlanMode` — these still prompt the user. This preserves the interactive escape hatch that would be lost if the callback were omitted.

**Session ID:** Extracted from the SDK's `init` message (`session_id` field). Not available at `startSession()` return time — arrives asynchronously.

**Remote execution:** If `executor` is set (SSH host), the provider validates SSH connectivity, discovers the remote Claude CLI path, translates home directory paths, and uses `spawnClaudeCodeProcess` to spawn tools remotely. Session files are synced back after each turn.

### 3.3 Codex Provider

**Execution model:** Long-lived `codex app-server` subprocess communicating via JSON-RPC over stdin/stdout.

```bash
codex app-server --listen stdio://
```

**Turn lifecycle:**
```
1. thread/start or thread/resume  →  Returns { thread: { id } }
2. turn/start { input, effort }   →  Returns { turn: { id } }
3. Listen for notifications:
   - item/started, item/completed  →  Tool use, text output
   - turn/completed                →  Turn done
   - thread/tokenUsage/updated     →  Token usage stats
4. Repeat from step 2 for next user message
```

**Tool approval:** Codex uses JSON-RPC server requests (bidirectional), not callbacks:
- `item/commandExecution/requestApproval` → Bash tool approval
- `item/fileChange/requestApproval` → Edit/Write tool approval
- `item/tool/requestUserInput` → User input request (returns empty answers MVP)

**Permission mode mapping:**
| YA Mode | Codex approvalPolicy | Codex sandbox |
|---------|---------------------|---------------|
| `default` | `on-request` | `workspace-write` |
| `plan` | `on-request` | `read-only` |
| `bypassPermissions` | `never` | `danger-full-access` |

**Session ID:** Returned immediately from `thread/start` response as `thread.id`.

### 3.4 Gemini Provider

**Execution model:** A new subprocess is spawned for EACH turn. No persistent server.

```bash
gemini -o stream-json [--resume sessionId] [-m model] "user prompt"
```

**Output parsing:** Line-delimited JSON events on stdout:
- `init` → Session ID, model info
- `message` (role=user/assistant) → Content fragments
- `tool_use` → Tool invocation
- `tool_result` → Tool output
- `result` → Exit code, token usage

**Assistant message buffering:** Gemini streams assistant content in fragments. The provider buffers fragments and emits a single `assistant` message with a stable UUID when the next non-assistant event arrives or the stream ends.

**Session ID:** Synthetic format `gemini-{timestamp}-{random}` if new, or from `init` event if resuming. Updated when the CLI returns the real ID.

**Limitations:**
- No tool approval support (`supportsPermissionMode: false`)
- No thinking toggle support
- Static model list (no dynamic query)
- Per-turn process spawn means higher latency between turns

### 3.5 Cross-Provider Comparison

| Aspect | Claude | Codex | Gemini |
|--------|--------|-------|--------|
| Execution model | Single long-lived SDK query | Long-lived app-server subprocess | Per-turn subprocess |
| Session persistence | SDK manages (sessionId resume) | App-server manages (threadId) | CLI manages (--resume flag) |
| Tool approval | `canUseTool` callback | JSON-RPC server requests | Not supported |
| Permission modes | 4 (default/acceptEdits/plan/bypass) | 4 (default/acceptEdits/plan/bypass) | None |
| Thinking/effort | `setMaxThinkingTokens()` + effort | Effort level in turn/start | Not supported |
| Session ID timing | Async (from init message) | Immediate (from thread/start) | Async (from init event) |
| Remote execution | SSH via spawnClaudeCodeProcess | Not supported | Not supported |

**Key source files:**
- `packages/server/src/sdk/providers/types.ts`
- `packages/server/src/sdk/providers/claude.ts`
- `packages/server/src/sdk/providers/codex.ts`
- `packages/server/src/sdk/providers/gemini.ts`

---

## 4. Process Lifecycle: The Supervisor State Machine {#4-process-lifecycle}

### 4.1 The State Machine

Every managed session is wrapped in a `Process` that tracks its execution state:

```
                      ┌──────────┐
           ┌────────→ │ in-turn  │ ←──── user message from queue
           │          └────┬─────┘
           │               │ SDK yields final result for turn
           │          ┌────▼─────┐
           │          │   idle   │ ←──── idle timer starts (5 min default)
           │          └────┬─────┘
           │               │ SDK calls canUseTool / requestUserInput
           │          ┌────▼──────────┐
           │          │ waiting-input │ ←──── InputRequest with tool info
           │          └────┬──────────┘
           │               │ client responds (allow/deny)
           │               └──────────→ back to in-turn
           │
      hold ◄─── preempted by WorkerQueue (idle > 10s while queue has waiters)
           │
  terminated ◄── abort / error / idle timeout (5 min) / stale (5 min no SDK msgs)
```

```typescript
type ProcessState =
  | { type: "in-turn" }
  | { type: "idle"; since: Date }
  | { type: "waiting-input"; request: InputRequest }
  | { type: "hold"; since: Date }
  | { type: "terminated"; reason: string; error?: Error };
```

### 4.2 Session Creation Flow

```
Client: POST /api/projects/:projectId/sessions { message, mode, model, ... }
  │
  ▼
Supervisor.createSession(projectPath, message, mode, settings)
  │
  ├── Check max workers limit
  │   ├── Under limit → proceed
  │   └── At limit → WorkerQueue.enqueue() → return QueuedResponse (202)
  │
  ├── Get provider: getProvider(settings.providerName ?? "claude")
  │
  ├── Create temp session ID: "temp-{uuid}"
  │
  ├── Call provider.startSession({
  │     cwd: projectPath,
  │     initialMessage: message,
  │     permissionMode: mode,
  │     model, thinking, effort,
  │     onToolApproval: (toolName, input, opts) => { ... }
  │   })
  │
  ├── Create Process(id, tempSessionId, { iterator, queue, abort, ... })
  │
  ├── Register: processes.set(processId, process)
  │             sessionToProcess.set(tempSessionId, processId)
  │
  ├── Start processMessages() loop (async, runs in background)
  │
  └── Emit "session-created" event on EventBus
```

### 4.3 Session ID Resolution

Claude and Gemini don't return the real session ID at creation time. It arrives in the first SDK message:

```
1. Supervisor creates with tempId = "temp-{uuid}"
2. Process.processMessages() starts consuming iterator
3. First system/init message from SDK has session_id field
4. Process updates internal _sessionId and emits:
   { type: "session-id-changed", oldSessionId: tempId, newSessionId: realId }
5. Supervisor's event listener (set up during registerProcess()) handles the mapping:
   - sessionToProcess.set(realId, processId)
   - everOwnedSessions.add(realId)
   - NOTE: temp ID mapping is RETAINED (not deleted) so clients still using
     the temp ID can look up the process during the transition window
6. Connected clients receive the event and update their session reference
```

### 4.4 The processMessages() Loop

This is the heart of the server — the event loop that consumes SDK output and distributes it:

```typescript
async processMessages(iterator: AsyncIterableIterator<SDKMessage>): Promise<void> {
  try {
    while (!this.iteratorDone) {
      const result = await this.sdkIterator.next();
      if (result.done) { this.iteratorDone = true; this.transitionToIdle(); break; }
      const message = result.value;
      // 1. Store in dual-bucket buffer (skip transient stream_events)
      //    Dedup: only user messages with uuid are checked for duplicates
      if (message.type !== "stream_event") {
        const isDuplicate = message.type === "user" && message.uuid
          && (currentBucket.some(m => m.uuid === message.uuid)
           || previousBucket.some(m => m.uuid === message.uuid));
        if (!isDuplicate) currentBucket.push(message);
      }

      // 2. Extract session ID from system init message only
      if (message.type === "system" && message.subtype === "init" && message.session_id) {
        this.resolveSessionId(message.session_id);
      }

      // 3. Emit to all listeners (subscription handlers)
      this.emit({ type: "message", message });

      // 4. Detect state transitions: result message → idle
      if (message.type === "result") {
        this.transitionToIdle();
      }

      // 5. Update last SDK message timestamp (for stale detection)
      this.lastSdkMessageAt = Date.now();
    }
  } catch (error) {
    this.terminate("error", error);
  }
}
```

### 4.5 Tool Approval Flow

When an SDK needs permission to use a tool:

```
1. SDK calls canUseTool(toolName, input, opts)
   │
2. Process creates PendingToolApproval:
   │  { request: InputRequest, resolve: (result) => void }
   │  Stored in pendingToolApprovals Map (supports concurrent approvals)
   │
3. Process transitions to "waiting-input" state
   │  Emits: { type: "state-change", state: { type: "waiting-input", request } }
   │
4. Subscription forwards to WebSocket client:
   │  { type: "status", data: { state: "waiting-input", request: { toolName, input, ... } } }
   │
5. Client displays approval UI, user responds
   │
6. Client sends: POST /sessions/:id/input { requestId, response: "approve"|"deny" }
   │
7. Process.respondToInput(requestId, response):
   │  pendingToolApprovals.get(requestId).resolve(result)
   │
8. SDK canUseTool callback resolves → SDK continues execution
   │  Process transitions back to "in-turn"
```

The `InputRequest` type:
```typescript
interface InputRequest {
  id: string;           // Unique request ID
  type: "tool_approval" | "user_question";
  toolName?: string;    // e.g., "Bash", "Edit", "Write"
  input?: unknown;      // Tool input (command, file path, etc.)
  message?: string;     // Human-readable prompt
}
```

### 4.6 Process Termination

Processes terminate for several reasons:

| Trigger | Constant | Description |
|---------|----------|-------------|
| Idle timeout | `DEFAULT_IDLE_TIMEOUT_MS` = 5 min | Process idle with no user messages |
| Stale detection | `STALE_IN_TURN_THRESHOLD_MS` = 5 min | In-turn but no SDK messages (stuck) |
| User abort | — | Client calls DELETE on session |
| SDK error | — | Iterator throws an error |
| Iterator done | — | SDK gracefully completes |

**Terminated process retention:** The Supervisor keeps info about the last 50 terminated processes for 10 minutes (`MAX_TERMINATED_PROCESSES` = 50, `TERMINATED_RETENTION_MS` = 10 min). This lets clients show "session ended" status even after the Process object is gone.

### 4.7 Constants

```typescript
const DEFAULT_IDLE_TIMEOUT_MS = 5 * 60 * 1000;           // 5 minutes
const DEFAULT_IDLE_PREEMPT_THRESHOLD_MS = 10 * 1000;      // 10 seconds
const MAX_TERMINATED_PROCESSES = 50;
const TERMINATED_RETENTION_MS = 10 * 60 * 1000;           // 10 minutes
const STALE_CHECK_INTERVAL_MS = 60 * 1000;                // 60 seconds
const STALE_IN_TURN_THRESHOLD_MS = 5 * 60 * 1000;         // 5 minutes
```

**Key source files:**
- `packages/server/src/supervisor/Supervisor.ts`
- `packages/server/src/supervisor/Process.ts`
- `packages/server/src/supervisor/types.ts`

---

## 5. Concurrency & Idle Preemption: The Worker Queue {#5-concurrency}

### 5.1 The Problem

Agent processes are resource-intensive. Running unlimited concurrent sessions would exhaust memory and API rate limits. The `WorkerQueue` manages this:

### 5.2 Queue Mechanics

```typescript
class WorkerQueue {
  private queue: QueuedRequest[] = [];  // FIFO
  private maxQueueSize: number;         // 0 = unlimited

  enqueue(params): EnqueueResult {
    // Returns { queueId, position, promise } or QueueFullError
    // promise resolves when started or cancelled
  }

  dequeue(): QueuedRequest | undefined {
    // FIFO: removes and returns first item
    // Emits position updates for remaining items
  }

  cancel(queueId): boolean {
    // Resolves promise with { status: "cancelled" }
  }
}
```

### 5.3 Idle Preemption

When the queue has waiters and an active process is idle, the Supervisor preempts:

```
1. Supervisor.onWorkerIdle(process):
   │
2. Check: WorkerQueue.isEmpty?
   │  ├── Yes → do nothing, process stays idle
   │  └── No → check idle duration
   │
3. Is process idle > DEFAULT_IDLE_PREEMPT_THRESHOLD_MS (10s)?
   │  ├── No → wait, check again later
   │  └── Yes → preempt:
   │      a. process.hold()  →  state = { type: "hold", since: now }
   │      b. Worker slot freed
   │      c. WorkerQueue.dequeue() → start queued session
```

A held process can be resumed later if a worker slot opens up.

### 5.4 Queue Events

The queue emits events through the EventBus so clients can show queue status:

```typescript
// When request is added to queue
{ type: "queue-request-added", queueId, sessionId, projectId, position }

// When position changes (item ahead was started/cancelled)
{ type: "queue-position-changed", queueId, sessionId, position }

// When request is started or cancelled
{ type: "queue-request-removed", queueId, sessionId, reason: "started" | "cancelled" }

// Global worker pool status
{ type: "worker-activity-changed", activeWorkers, queueLength, hasActiveWork }
```

**Key source file:** `packages/server/src/supervisor/WorkerQueue.ts`

---

## 6. Message Flow: SDK to WebSocket Client {#6-message-flow}

### 6.1 End-to-End Pipeline

```
Provider (Claude SDK / Codex subprocess / Gemini CLI)
    │ yields SDKMessage via async iterator
    ▼
Process.processMessages() loop
    │ 1. Resolve session ID (if init message)
    │ 2. Detect state transitions
    │ 3. Store in dual-bucket buffer (skip stream_events)
    │ 4. Accumulate streaming text
    │ 5. Emit to process.listeners
    ▼
Subscription handler (createSessionSubscription)
    │ 1. Pass message to StreamAugmenter.processMessage()
    │    → Computes diffs, syntax highlighting, markdown HTML
    │    → Mutates message in-place (adds _structuredPatch, _diffHtml, etc.)
    │ 2. Extract text deltas for streaming markdown
    │    → StreamCoordinator emits markdown-augment and pending events
    │ 3. Emit augmented message via transport
    ▼
Transport-specific emit function
    │ Direct WebSocket: ws.send(JSON.stringify(event))
    │ Relay: encrypt with NaCl → binary envelope → ws.send()
    ▼
Client receives: { type: "message", data: { ...augmentedSDKMessage } }
```

### 6.2 The MessageQueue Bridge

The MessageQueue bridges HTTP/WebSocket message receives to the SDK's AsyncGenerator interface:

```typescript
class MessageQueue {
  private queue: UserMessage[] = [];           // Stores app-level messages
  private waiting: ((msg: UserMessage) => void) | null = null;

  push(message: UserMessage): number {
    if (this.waiting) {
      // Generator is waiting — resolve immediately
      this.waiting(message);
      this.waiting = null;
      return 0;
    }
    this.queue.push(message);
    return this.queue.length;
  }

  async *generator(): AsyncGenerator<SDKUserMessage> {
    while (true) {
      const message = await this.next();       // Blocks until push()
      yield this.toSDKMessage(message);        // Convert at yield time, not push time
    }
  }

  private next(): Promise<UserMessage> {
    const queued = this.queue.shift();
    if (queued) return Promise.resolve(queued);
    return new Promise(resolve => { this.waiting = resolve; });
  }

  private toSDKMessage(msg: UserMessage): SDKUserMessage {
    // Append uploaded file paths, convert images to base64 content blocks,
    // preserve client-provided uuid for deduplication
  }
}
```

**Message formatting (`toSDKMessage`):**
- Appends uploaded file paths to text (for the Read tool)
- Converts images to base64 content blocks with media type detection
- Preserves client-provided `uuid` for deduplication

### 6.3 Message Types Emitted to Clients

| Event Type | Data | When |
|-----------|------|------|
| `connected` | `{ processId, sessionId, state, permissionMode, model }` | On subscription start |
| `message` | SDKMessage (augmented) | Each message from SDK |
| `status` | `{ state: AgentActivity, request?: InputRequest }` | State transitions |
| `mode-change` | `{ mode: PermissionMode, version: number }` | Permission mode changed |
| `session-id-changed` | `{ oldSessionId, newSessionId }` | Temp ID resolved |
| `markdown-augment` | `{ messageId, blockIndex, html, type }` | Completed markdown block |
| `pending` | `{ messageId, html }` | Partial markdown (streaming) |
| `complete` | `{}` | Turn finished |
| `deferred-queue` | `{ messages: [...] }` | Messages queued while busy |
| `heartbeat` | `{ timestamp }` | Every 30 seconds |
| `error` | `{ message }` | Process error |

### 6.4 Subscription Lifecycle

```typescript
function createSessionSubscription(process, emit, options?): { cleanup } {
  // 1. Subscribe to process events FIRST (before state capture)
  //    This prevents race: state changes during replay are caught
  const unsubscribe = process.subscribe(handleEvent);

  // 2. Capture current state snapshot
  const state = process.getState();
  emit("connected", { processId, sessionId, state, ... });

  // 3. Replay message history (dual-bucket contents)
  for (const msg of process.getMessageHistory()) {
    await augmenter.processMessage(msg);
    emit("message", msg);
  }

  // 4. Catch up streaming text (if mid-stream)
  const streaming = process.getStreamingContent();
  if (streaming) {
    await augmenter.processCatchUp(streaming.text, streaming.messageId);
  }

  // 5. Forward live events going forward
  function handleEvent(event: ProcessEvent) { ... }

  return { cleanup: () => { unsubscribe(); clearInterval(heartbeat); } };
}
```

**Key source files:**
- `packages/server/src/subscriptions.ts`
- `packages/server/src/sdk/messageQueue.ts`

---

## 7. Stream Augmentation: Server-Side Rendering {#7-stream-augmentation}

### 7.1 Why Server-Side

Mobile clients can't afford to run syntax highlighters, diff algorithms, and markdown parsers. The server computes all rich content and sends pre-rendered HTML. This also ensures visual consistency across clients.

### 7.2 Augmentation Types

| Tool | Augmented Fields | Description |
|------|-----------------|-------------|
| Edit | `_structuredPatch`, `_diffHtml` | Unified diff with syntax-highlighted additions/deletions |
| Write | `_highlightedContentHtml`, `_highlightedLanguage` | Syntax-highlighted file content |
| Read | `_highlightedContentHtml` | Syntax-highlighted file content from tool result |
| ExitPlanMode | `_renderedHtml` | Markdown plan rendered to HTML |

**Convention:** Augmented fields use the `_` prefix to signal "computed/augmented, not from the SDK."

### 7.3 Streaming Markdown Pipeline

For assistant text responses, markdown is rendered incrementally as text streams in:

```
Text delta arrives (e.g., "Here's the ")
    ▼
StreamCoordinator.onChunk("Here's the ")
    ▼
BlockDetector identifies block boundaries
    │ (code fences, list markers, headers, etc.)
    ▼
Completed blocks → Full render → emit markdown-augment
    │ { messageId, blockIndex, html, type: "code"|"list"|"text" }
    │
Incomplete block → Optimistic render → emit pending
    │ { messageId, html }  (re-rendered on each chunk)
    │
On stream completion:
    │ flush() → render final blocks → emit remaining augments
    │ renderFinalMarkdown() → full HTML for the complete message
```

**Key insight:** Each markdown block gets its own `blockIndex`. The client maps these to DOM elements for incremental updates. Completed blocks are never re-rendered; only the pending (last) block updates on each chunk.

### 7.4 StreamAugmenter Lifecycle

```typescript
const augmenter = await createStreamAugmenter({
  onMarkdownAugment: (data) => emit("markdown-augment", data),
  onPending: (data) => emit("pending", data),
  onError: (err, context) => log.warn(context, err),
});

// For each message:
await augmenter.processMessage(message);  // Augments tool content, feeds text to coordinator

// On stream completion:
await augmenter.flush();  // Finalize last blocks
```

**Lazy initialization:** The augmenter is created lazily on first message event. If a client connects to an idle process, no augmenter overhead is incurred.

**Key source files:**
- `packages/server/src/augments/stream-augmenter.ts`
- `packages/server/src/augments/stream-coordinator.ts`

---

## 8. Late-Join Replay: The Dual-Bucket Buffer {#8-late-join-replay}

### 8.1 The Problem

A client that connects mid-conversation needs to catch up on recent messages. But the JSONL file on disk may lag behind the live stream (writes are buffered). We need an in-memory buffer, but it can't be unbounded — a long agent session produces thousands of messages.

### 8.2 Dual-Bucket Algorithm

Two arrays swap every 15 seconds, providing a 15–30 second replay window:

```
Time:  0s ─────────── 15s ─────────── 30s ─────────── 45s
       │  bucket A     │  bucket B     │  bucket A     │
       │  (current)    │  (current)    │  (current)    │
       │               │  A→previous   │  B→previous   │
       │               │  B→current    │  A→current    │
```

```typescript
class Process {
  private currentBucket: SDKMessage[] = [];
  private previousBucket: SDKMessage[] = [];
  private static readonly BUCKET_SWAP_INTERVAL_MS = 15_000;

  constructor() {
    this.bucketSwapTimer = setInterval(() => {
      this.previousBucket = this.currentBucket;
      this.currentBucket = [];
    }, Process.BUCKET_SWAP_INTERVAL_MS);
  }

  // In processMessages loop (not a separate method):
  // Skip transient stream_events (deltas that would cause flickering on replay)
  if (message.type !== "stream_event") {
    // Dedup only user messages with uuid (SDK echoes back user messages we already stored)
    const isDuplicate = message.type === "user" && message.uuid
      && (this.currentBucket.some(m => m.uuid === message.uuid)
       || this.previousBucket.some(m => m.uuid === message.uuid));
    if (!isDuplicate) {
      this.currentBucket.push(message);
    }
  }

  getMessageHistory(): SDKMessage[] {
    return [...this.previousBucket, ...this.currentBucket];
  }
}
```

### 8.3 Streaming Text Catch-Up

For clients joining while the assistant is mid-response, the accumulated text is tracked separately:

```typescript
private _streamingText = "";
private _streamingMessageId: string | null = null;

accumulateStreamingText(messageId: string, text: string): void {
  if (this._streamingMessageId !== messageId) {
    this._streamingText = "";
    this._streamingMessageId = messageId;
  }
  this._streamingText += text;
}

getStreamingContent(): { messageId: string; text: string } | null {
  if (!this._streamingMessageId || !this._streamingText) return null;
  return { messageId: this._streamingMessageId, text: this._streamingText };
}

clearStreamingText(): void {
  this._streamingText = "";
  this._streamingMessageId = null;
}
```

The subscription handler uses this for catch-up:
```typescript
// In createSessionSubscription, after replaying message history:
const streaming = process.getStreamingContent();
if (streaming) {
  // Render accumulated text as pending HTML
  await augmenter.processCatchUp(streaming.text, streaming.messageId);
  // Client receives a single "pending" event with the rendered HTML
}
```

### 8.4 Why Not Unbounded Buffers

A coding agent session can run for hours producing thousands of tool calls and messages. Storing all of them in memory would grow without bound. The 30-second window is a deliberate trade-off:

- **Within the window:** Late-joining clients get instant catch-up from memory
- **Beyond the window:** Clients load from the JSONL file on disk (the history loading path in Section 9)
- **Memory cost:** Bounded at ~30 seconds of messages per active process

---

## 9. History Loading & DAG Resolution {#9-history-loading}

When a client loads a session (not live-streaming), messages come from the persisted file on disk. Each provider has a different format and algorithm.

### 9.1 Claude Sessions: DAG-Based JSONL

Claude Code's JSONL files are not linear logs — they form a directed acyclic graph. Each message has a `uuid` and `parentUuid`. This enables conversation branching (forking from any point) and clean recovery (resumption picks any node as a continuation point).

**Step 1: Parse JSONL into entries**
```
Line 0: { "type": "user",      "uuid": "aaa", "parentUuid": null, ... }
Line 1: { "type": "assistant",  "uuid": "bbb", "parentUuid": "aaa", ... }
Line 2: { "type": "user",      "uuid": "ccc", "parentUuid": "bbb", ... }
Line 3: { "type": "assistant",  "uuid": "ddd", "parentUuid": "ccc", ... }
Line 4: { "type": "user",      "uuid": "eee", "parentUuid": "bbb", ... }  ← FORK!
Line 5: { "type": "assistant",  "uuid": "fff", "parentUuid": "eee", ... }
```

**Step 2: Build DAG**
```typescript
function buildDag(entries: ClaudeSessionEntry[]): DagResult {
  const nodeMap = new Map<string, DagNode>();   // uuid → node
  const childrenMap = new Map<string, string[]>(); // parentUuid → [childUuids]

  // Build maps in single pass
  for (const [lineIndex, entry] of entries.entries()) {
    const uuid = entry.uuid;
    if (!uuid) continue;  // Skip entries without UUID (rare)
    nodeMap.set(uuid, { uuid, parentUuid: entry.parentUuid ?? null, lineIndex, raw: entry });
    if (entry.parentUuid) {
      const children = childrenMap.get(entry.parentUuid) ?? [];
      children.push(uuid);
      childrenMap.set(entry.parentUuid, children);
    }
  }
  // ...
}
```

**Step 3: Find tips** (nodes with no children)
```typescript
const tipsWithLength: Array<{ uuid: string; length: number; timestamp: string }> = [];

for (const [uuid, node] of nodeMap) {
  if (!childrenMap.has(uuid) || childrenMap.get(uuid)!.length === 0) {
    const length = walkBranchLength(uuid, nodeMap);  // Count user/assistant only
    tipsWithLength.push({ uuid, length, timestamp: node.raw.timestamp ?? "" });
  }
}
```

**Step 4: Select active tip** (priority order)
1. Most recent timestamp (ISO string comparison)
2. Tiebreaker: longest conversation branch (user/assistant count only — not system/progress)
3. Tiebreaker: latest line index in file

**Step 5: Walk tip to root**
```typescript
const activeBranch: DagNode[] = [];
let current: string | null = selectedTip.uuid;
const visited = new Set<string>();

while (current && !visited.has(current)) {
  visited.add(current);
  const node = nodeMap.get(current);
  if (!node) break;
  activeBranch.unshift(node);  // Prepend for root→tip order

  // Follow parentUuid, or logicalParentUuid for compact_boundary nodes
  let next = node.parentUuid;
  if (!next) {
    const logicalParent = getLogicalParentUuid(node.raw);
    if (logicalParent && nodeMap.has(logicalParent)) {
      next = logicalParent;
    } else {
      // Fallback: find nearest preceding node by line index.
      // This bridges compact boundaries where the logicalParentUuid
      // target has been compacted away (common in long sessions).
      const fallback = findFallbackParentByLineIndex(node.lineIndex, nodeMap, visited);
      next = fallback?.uuid ?? null;
    }
  }
  current = next;
}
```

**Step 6: Return result**
```typescript
return {
  activeBranch,                              // Root→tip ordered messages
  activeBranchUuids: new Set(activeBranch.map(n => n.uuid)),
  tip: activeBranch[activeBranch.length - 1] ?? null,
  hasBranches: tipsWithLength.length > 1,
  alternateBranches: otherTips.map(t => ({ tipUuid: t.uuid, length: t.length, tipType })),
};
```

**Orphaned tool use detection:** After DAG resolution, scan for `tool_use` blocks on the active branch that have no matching `tool_result` anywhere in the entire session (across all branches). These are marked with `orphanedToolUseIds` so clients can show "pending" state for tool calls that were interrupted.

**Sibling tool branches:** When the agent runs parallel tools (e.g., three Read calls simultaneously), results may appear on different branches. `findSiblingToolBranches()` recovers complete tool result chains from dead branches so the client can display all results.

### 9.2 Codex Sessions: Linear JSONL

Codex sessions are straightforward linear logs. Each line is one of four entry types:

```typescript
// Line 0: Session initialization
{ "type": "session_meta", "payload": { "cwd": "/path", "timestamp": "...", "model": "gpt-4o" } }

// Lines 1+: Turn context (per-turn config)
{ "type": "turn_context", "payload": { "approval_policy": "on-request", "sandbox_policy": {...} } }

// Lines 2+: Aggregated messages and tool calls
{ "type": "response_item", "payload": { "type": "message", "role": "user"|"assistant", "content": [...] } }
{ "type": "response_item", "payload": { "type": "function_call", "name": "...", "arguments": "..." } }
{ "type": "response_item", "payload": { "type": "function_call_output", "output": "..." } }

// Lines N: Streaming events and token counts
{ "type": "event_msg", "payload": { "type": "user_message"|"agent_message"|"token_count", ... } }
```

**Deduplication logic:** If `response_item` entries contain user messages, skip `event_msg.user_message` (which would duplicate them). Prefer `response_item` because it has the complete text.

**Function call pairing:** Track `pendingCalls` map to pair `function_call` with its corresponding `function_call_output` by call ID.

### 9.3 Gemini Sessions: Single JSON File

```typescript
interface GeminiSessionFile {
  sessionId: string;
  projectHash: string;
  startTime: string;
  lastUpdated?: string;
  messages: Array<{
    type: "user" | "gemini";
    id: string;
    content: string;
    model?: string;                          // Only on "gemini" messages
    tokens?: { input: number; cached?: number; output?: number };
    timestamp?: string;
  }>;
}
```

Linear array. Tool use is embedded in message content as `functionCall`/`functionResponse` objects. No branching, no compaction.

### 9.4 Context Usage Extraction

All three providers extract context window usage differently:

| Provider | Source | Formula |
|----------|--------|---------|
| Claude | Last assistant message `usage` field | `input_tokens + cache_read + cache_creation + compactionOverhead` |
| Codex | Last `token_count` event | `info.last_token_usage.input_tokens` (latest turn only) |
| Gemini | Last gemini message `tokens` field | `tokens.input + (tokens.cached ?? 0)` |

**Claude compaction overhead:** After context compaction, the SDK only reports tokens actually sent (summary + new messages). The "hidden overhead" (system prompt, tools, pre-compaction context) must be recovered:
```
overhead = compact_boundary.compactMetadata.preTokens - lastPreCompactionTokens
```
This is added to `inputTokens` for an accurate context fill percentage.

**Key source files:**
- `packages/server/src/sessions/dag.ts`
- `packages/server/src/sessions/reader.ts`
- `packages/server/src/sessions/codex-reader.ts`
- `packages/server/src/sessions/gemini-reader.ts`

---

## 10. Pagination & Compaction Boundaries {#10-pagination}

### 10.1 The Problem

A long Claude session can have thousands of messages across multiple context compactions. Loading all of them on initial page load would be slow and wasteful. YA paginates by compaction boundaries.

### 10.2 Claude's Compact Boundaries

When Claude Code's context window fills up, the SDK compacts: it summarizes older messages and writes a `compact_boundary` entry to the JSONL file. Each boundary marks a "chapter break" in the conversation.

### 10.3 The Pagination Algorithm

```typescript
function sliceAtCompactBoundaries(
  messages: Message[],       // Active branch, root→tip order (from DAG resolution)
  tailCompactions: number,   // How many recent compaction segments to return (default: 1)
  beforeMessageId?: string,  // Cursor: load messages before this ID
): SliceResult {

  // 1. Apply cursor (for "load older" requests)
  let workingMessages = messages;
  if (beforeMessageId) {
    const idx = messages.findIndex(m => (m.uuid ?? m.id) === beforeMessageId);
    if (idx > 0) workingMessages = messages.slice(0, idx);
  }

  // 2. Find all compact_boundary indices
  const compactIndices = workingMessages
    .map((m, i) => m.type === "system" && m.subtype === "compact_boundary" ? i : -1)
    .filter(i => i >= 0);

  // 3. If fewer boundaries than requested, return everything
  if (compactIndices.length <= tailCompactions) {
    return { messages: workingMessages, pagination: { hasOlderMessages: false, ... } };
  }

  // 4. Slice from Nth-from-last boundary (includes the boundary itself)
  const sliceFromIdx = compactIndices[compactIndices.length - tailCompactions];
  const sliced = workingMessages.slice(sliceFromIdx);

  return {
    messages: sliced,
    pagination: {
      hasOlderMessages: true,
      totalMessageCount: messages.length,
      returnedMessageCount: sliced.length,
      truncatedBeforeMessageId: sliced[0]?.uuid ?? sliced[0]?.id,
      totalCompactions: compactIndices.length,
    }
  };
}
```

**Example flow:**
```
Session with 10 compactions, tailCompactions=1 (default):
  → Returns messages from compaction 10 onwards
  → pagination.hasOlderMessages = true
  → pagination.truncatedBeforeMessageId = first message UUID

Client requests "load older" with truncatedBeforeMessageId:
  → Returns messages from compaction 9 to just before the cursor
  → Repeat until hasOlderMessages = false
```

### 10.4 Codex and Gemini

Neither Codex nor Gemini have compaction. Full sessions are returned on load. The `tailCompactions` parameter still works — it just finds zero boundaries and returns everything. These sessions are typically shorter (Codex sessions are turn-based, Gemini sessions are per-turn process spawns), so pagination is rarely needed.

**Key source file:** `packages/server/src/sessions/pagination.ts`

---

## 11. External Session Monitoring {#11-external-session-monitoring}

### 11.1 Why This Matters

A user might have Claude Code running in VS Code, Codex in a terminal, and YA supervising. YA needs to show ALL sessions — not just ones it started. This is the "monitor the whole system" capability.

### 11.2 Session Ownership Model

```typescript
type SessionOwnership =
  | { owner: "none" }                                        // File on disk, no active process
  | { owner: "self"; processId: string; permissionMode?: PermissionMode }  // YA controls it
  | { owner: "external" };                                   // Another program is writing to it
```

Every session has one of these three states at any time.

### 11.3 External Session Detection Algorithm

```
FileWatcher detects session file change
    │ (fs.watch event, 200ms debounced)
    ▼
EventBus emits FileChangeEvent
    │ { type: "file-change", path, relativePath, provider, fileType, changeType }
    ▼
ExternalSessionTracker.handleFileChange(event)
    │
    ├── Filter: only "session" or "agent-session" fileTypes
    │
    ├── Parse sessionId and projectId from file path
    │   Claude: projects/{projectId}/{sessionId}.jsonl
    │   Codex: extract UUID from rollout-{model}-{uuid}.jsonl
    │   ⚠️ Gemini: NOT currently handled (ExternalSessionTracker only
    │      processes .jsonl files; Gemini .json files are watched but
    │      not parsed for external session detection)
    │
    ├── Check: Does Supervisor own this session?
    │   │
    │   ├── Yes (self-owned):
    │   │   Remove from external tracking if present
    │   │   Queue session parsing for title/messageCount updates
    │   │   Return
    │   │
    │   └── No (not owned):
    │       │
    │       ├── Check abort grace period (30s after YA aborted this session)
    │       │   If in grace period → ignore (cleanup writes, not external activity)
    │       │
    │       └── Mark as external:
    │           ├── New external session:
    │           │   Create ExternalSessionInfo with 30s decay timeout
    │           │   Queue batch session parsing
    │           │   Emit "session-status-changed" { ownership: "external" }
    │           │   Parse session → emit "session-created" (first time only)
    │           │
    │           └── Known external session:
    │               Reset 30s decay timer
    │               Queue re-parse for title/messageCount updates
```

### 11.4 Decay Timeout

External status isn't permanent. If no file changes occur for 30 seconds, the session reverts to `{ owner: "none" }`:

```typescript
private createDecayTimeout(sessionId: string): NodeJS.Timeout {
  return setTimeout(() => {
    this.externalSessions.delete(sessionId);
    this.emitOwnershipChange(sessionId, { owner: "none" });
  }, this.decayMs);  // Default: 30000ms
}
```

Each file change resets this timer. As long as the external agent keeps writing, the session stays "external." When it stops, the status decays after 30 seconds.

### 11.5 Abort Grace Period

When YA aborts a session it owns, the SDK/CLI writes cleanup data to the session file. Without a grace period, these writes would trigger external detection (false positive). The 30-second grace period prevents this:

```typescript
markAborted(sessionId: string): void {
  this.recentlyAborted.set(sessionId, Date.now());
  this.removeExternal(sessionId);  // Clean up if somehow external
}

isInAbortGracePeriod(sessionId: string): boolean {
  const abortedAt = this.recentlyAborted.get(sessionId);
  if (!abortedAt) return false;
  return (Date.now() - abortedAt) < this.abortGraceMs;  // Default: 30000ms
}
```

### 11.6 Batched Session Parsing

When many file changes arrive at once (e.g., user opens VS Code and multiple sessions start), parsing all session files concurrently could exhaust memory. The `BatchProcessor` limits concurrency:

```typescript
this.sessionParser = new BatchProcessor<SessionSummary | null>({
  concurrency: 5,     // Max 5 concurrent JSONL parses
  batchMs: 300,       // 300ms batching window (dedup rapid changes to same session)
  onResult: (sessionId, summary) => {
    // Emit session-created (first time) or session-updated (changes detected)
  },
  onError: (sessionId, error) => {
    // Log but don't fail — session may not be readable yet
  },
});
```

**Deduplication:** If the same session file changes 5 times in 300ms, only the last change triggers a parse. The `BatchProcessor` deduplicates by session ID.

### 11.7 Change Detection (session-updated Events)

The tracker caches session state and only emits `session-updated` when something actually changed:

```typescript
private sessionStateCache: Map<string, {
  title: string | null;
  messageCount: number;
  projectId: UrlProjectId;
  contextUsage?: ContextUsage;
  model?: string;
}>;

// On parse result:
const cached = this.sessionStateCache.get(sessionId);
const titleChanged = cached?.title !== summary.title;
const messageCountChanged = cached?.messageCount !== summary.messageCount;
const contextUsageChanged = cached?.contextUsage?.inputTokens !== summary.contextUsage?.inputTokens;
const modelChanged = cached?.model !== summary.model;

if (titleChanged || messageCountChanged || contextUsageChanged || modelChanged) {
  this.eventBus.emit({ type: "session-updated", sessionId, ... });
  this.sessionStateCache.set(sessionId, { ... });
}
```

**Key source file:** `packages/server/src/supervisor/ExternalSessionTracker.ts`

---

## 12. Event Bus & File Watcher Infrastructure {#12-event-bus}

### 12.1 EventBus

Simple in-memory pub/sub. No persistence, no guaranteed delivery. All emissions are synchronous.

```typescript
class EventBus {
  private subscribers = new Set<(event: BusEvent) => void>();

  subscribe(handler: (event: BusEvent) => void): () => void {
    this.subscribers.add(handler);
    return () => this.subscribers.delete(handler);
  }

  emit(event: BusEvent): void {
    for (const handler of this.subscribers) {
      handler(event);
    }
  }
}
```

### 12.2 BusEvent Types (Complete List)

```typescript
type BusEvent =
  // File system
  | FileChangeEvent            // File created/modified/deleted

  // Session lifecycle
  | SessionCreatedEvent        // New session discovered or created
  | SessionUpdatedEvent        // Title, messageCount, contextUsage, model changed
  | SessionStatusEvent         // Ownership changed (self/external/none)
  | SessionAbortedEvent        // Session terminated by server
  | SessionSeenEvent           // Session marked as read (cross-tab sync)
  | SessionMetadataChangedEvent // User-set metadata (customTitle, archived, starred)

  // Process state
  | ProcessStateEvent          // Agent activity (running, waiting-input, idle)

  // Worker queue
  | QueueRequestAddedEvent     // Request added to queue
  | QueuePositionChangedEvent  // Position changed
  | QueueRequestRemovedEvent   // Request started or cancelled
  | WorkerActivityEvent        // Worker pool status (activeWorkers, queueLength)

  // Infrastructure
  | SourceChangeEvent          // Source code changed (dev mode reload)
  | BackendReloadedEvent       // Server restarted
  | NetworkBindingChangedEvent // Network config changed
  | BrowserTabConnectedEvent   // Client connected to activity stream
  | BrowserTabDisconnectedEvent; // Client disconnected
```

### 12.3 FileWatcher

One watcher per provider directory. Uses `fs.watch({ recursive: true })` for real-time events.

```typescript
interface FileWatcherOptions {
  watchDir: string;
  provider: WatchProvider;     // "claude" | "gemini" | "codex" (subset of ProviderName)
  eventBus: EventBus;
  debounceMs?: number;         // Default: 200ms
  periodicRescanMs?: number;   // Optional fallback rescan interval
}

class FileWatcher {
  private knownFileMtimes = new Map<string, number>();  // path → mtime
  private debounceTimers = new Map<string, NodeJS.Timeout>();
  private debounceMs: number;  // Configurable per-instance (default 200)

  constructor(options: FileWatcherOptions) {
    // Stores options only — does NOT start watching
    this.debounceMs = options.debounceMs ?? 200;
  }

  start(): void {
    // Two-phase init: must call start() explicitly
    // 1. Initial scan: build mtime index
    this.scanExistingFiles(this.watchDir);
    // 2. Start watching
    fs.watch(this.watchDir, { recursive: true }, (eventType, filename) => {
      if (!filename) {
        this.scheduleRescan();  // macOS fallback: null filename
        return;
      }
      this.debounceChange(path.join(this.watchDir, filename));
    });
  }
}
```

**Change type detection** (synchronous, inline in `emitEvent()`):
```typescript
private emitEvent(fullPath: string): void {
  if (!fs.existsSync(fullPath)) {
    if (this.knownFiles.has(fullPath)) { /* delete */ }
    else return;  // Never existed from our POV
  } else {
    const mtimeMs = fs.statSync(fullPath).mtimeMs;
    if (this.knownFiles.has(fullPath)) {
      if (this.knownFileMtimes.get(fullPath) === mtimeMs) return;  // Same mtime → SKIP
      changeType = "modify";
    } else {
      changeType = "create";
    }
    this.knownFileMtimes.set(fullPath, mtimeMs);
  }
  // Emit FileChangeEvent...
}
```

**File type classification:**
- `session` — Main session files (e.g., `abc123.jsonl`)
- `agent-session` — Subagent files (e.g., `agent-def456.jsonl`)
- `other` — Anything else

Note: The type system also reserves `settings`, `credentials`, and `telemetry` values for future use, but currently only `session`, `agent-session`, and `other` are emitted.

**Fallback rescan:** On macOS, `fs.watch` sometimes provides `null` as the filename. When this happens, a full directory rescan is scheduled (400ms delay to batch). The rescan compares current mtimes against `knownFileMtimes` to detect creates, modifies, and deletes.

### 12.4 Activity Subscription

The activity channel forwards all EventBus events to WebSocket clients:

```typescript
function createActivitySubscription(eventBus, emit, options?): { cleanup } {
  emit("connected", { timestamp: new Date().toISOString() });

  const unsubscribe = eventBus.subscribe((event: BusEvent) => {
    emit(event.type, event);  // Forward all events as-is
  });

  const heartbeat = setInterval(() => {
    emit("heartbeat", { timestamp: new Date().toISOString() });
  }, 30_000);

  return { cleanup: () => { unsubscribe(); clearInterval(heartbeat); } };
}
```

This powers the dashboard's real-time session list — new sessions appearing, status changes, message count updates, all without polling.

**Key source files:**
- `packages/server/src/watcher/EventBus.ts`
- `packages/server/src/watcher/FileWatcher.ts`
- `packages/server/src/watcher/BatchProcessor.ts`

---

## 13. API & WebSocket Protocol Reference {#13-api-reference}

### 13.1 REST Endpoints: Sessions

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/projects/:projectId/sessions` | Start new session (returns 200 with process or 202 with queue position) |
| POST | `/api/projects/:projectId/sessions/create` | Create empty session (two-phase: create → upload files → message) |
| POST | `/api/projects/:projectId/sessions/:sessionId/resume` | Resume existing session |
| GET | `/api/projects/:projectId/sessions/:sessionId` | Get session detail with messages (reads from disk, augments with live state) |
| POST | `/api/sessions/:sessionId/messages` | Send message to session (queue if busy) |
| POST | `/api/sessions/:sessionId/input` | Respond to tool approval (approve/deny) |
| PUT | `/api/sessions/:sessionId/mode` | Change permission mode |
| PUT | `/api/sessions/:sessionId/hold` | Pause/resume session |
| GET | `/api/sessions/:sessionId/pending-input` | Get current tool approval request |
| GET | `/api/sessions/:sessionId/process` | Get process info (state, model, etc.) |
| POST | `/api/sessions/:sessionId/mark-seen` | Mark session as read |
| DELETE | `/api/sessions/:sessionId` | Abort session |
| DELETE | `/api/sessions/:sessionId/deferred/:tempId` | Cancel deferred message |

### 13.2 REST Endpoints: Projects & Providers

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/projects` | List all projects across providers |
| GET | `/api/sessions` | Global session list with filters & pagination |
| GET | `/api/sessions/stats` | Session counts by status/provider |
| GET | `/api/providers` | List available providers with auth status |
| GET | `/api/providers/:name/models` | Available models for provider |

### 13.3 Session Start Request/Response

**Request:** `POST /api/projects/:projectId/sessions`
```typescript
{
  message: string;
  permissionMode?: "default" | "acceptEdits" | "plan" | "bypassPermissions";
  model?: string;
  thinking?: ThinkingConfig;
  effort?: EffortLevel;
  providerName?: ProviderName;
  executor?: string;        // SSH host for remote execution
  globalInstructions?: string;
}
```

**Response (started):** 200
```typescript
{
  processId: string;
  sessionId: string;        // Temp ID initially, resolves later
  state: AgentActivity;
  queued: false;
}
```

**Response (queued):** 202
```typescript
{
  queued: true;
  queueId: string;
  position: number;         // 1-based position in queue
}
```

### 13.4 Session Detail Response

**Request:** `GET /api/projects/:projectId/sessions/:sessionId?tailCompactions=1`

**Response:**
```typescript
{
  id: string;
  projectId: UrlProjectId;
  title: string | null;
  fullTitle: string | null;
  createdAt: string;
  updatedAt: string;
  messageCount: number;
  ownership: SessionOwnership;
  provider: ProviderName;
  model?: string;
  contextUsage?: ContextUsage;
  messages: Message[];
  pagination: {
    hasOlderMessages: boolean;
    totalMessageCount: number;
    returnedMessageCount: number;
    truncatedBeforeMessageId?: string;
    totalCompactions: number;
  };
  pendingInputRequest?: InputRequest;  // If waiting for tool approval
}
```

### 13.5 Tool Approval Request/Response

**Request:** `POST /api/sessions/:sessionId/input`
```typescript
{
  requestId: string;
  response: "approve" | "deny" | "approve_accept_edits";
  answers?: Record<string, string>;  // For user question responses
  feedback?: string;                 // Denial reason
}
```

### 13.6 WebSocket Channels

Three WebSocket subscription channels, all multiplexed over the same connection:

| Channel | Subscribe Message | Purpose |
|---------|------------------|---------|
| `session` | `{ type: "subscribe", channel: "session", sessionId }` | Live session stream (messages, status, augments) |
| `activity` | `{ type: "subscribe", channel: "activity" }` | Global event bus (session-created, session-updated, etc.) |
| `session-watch` | `{ type: "subscribe", channel: "session-watch", sessionId, projectId }` | External session file changes (for read-only viewing) |

### 13.7 WebSocket Message Format

All WebSocket messages are JSON objects with a `type` field:

**Server → Client:**
```typescript
// Session channel
{ type: "connected", data: { processId, sessionId, state, permissionMode, model } }
{ type: "message", data: SDKMessage }
{ type: "status", data: { state: AgentActivity, request?: InputRequest } }
{ type: "mode-change", data: { mode: PermissionMode, version: number } }
{ type: "session-id-changed", data: { oldSessionId, newSessionId } }
{ type: "markdown-augment", data: { messageId, blockIndex, html, type } }
{ type: "pending", data: { messageId, html } }
{ type: "complete", data: {} }
{ type: "deferred-queue", data: { messages: Array<{ tempId?, content, timestamp }> } }
{ type: "heartbeat", data: { timestamp } }
{ type: "error", data: { message } }

// Activity channel
{ type: "connected", data: { timestamp } }
{ type: "session-created", data: SessionSummary }
{ type: "session-updated", data: { sessionId, title, messageCount, ... } }
{ type: "session-status-changed", data: { sessionId, ownership } }
{ type: "process-state-changed", data: { sessionId, state } }
{ type: "queue-request-added", data: { queueId, position } }
{ type: "worker-activity-changed", data: { activeWorkers, queueLength, hasActiveWork } }
// ... all BusEvent types

// Session-watch channel
{ type: "session-watch-change", data: { sessionId, changeType } }
```

**Client → Server:**
```typescript
{ type: "subscribe", subscriptionId, channel: "session", sessionId }
{ type: "subscribe", subscriptionId, channel: "activity" }
{ type: "subscribe", subscriptionId, channel: "session-watch", sessionId, projectId, providerHint? }
{ type: "unsubscribe", subscriptionId }
```

### 13.8 Relay Connection (SRP + Encryption)

When connecting through a relay server, messages are encrypted:

1. **SRP handshake:** Client proves password knowledge without revealing it to the relay
2. **Session key derivation:** 32-byte key from SRP exchange
3. **Message encryption:** NaCl XSalsa20-Poly1305 (authenticated encryption)
4. **Binary envelopes:** Compressed, encrypted, framed binary protocol
5. **Sequence numbers:** Each encrypted message includes a monotonic sequence number for replay prevention

The relay server sees only opaque ciphertext. All application messages (subscribe, message, status, etc.) are wrapped in encrypted envelopes.

**Key source files:**
- `packages/server/src/routes/sessions.ts`
- `packages/server/src/routes/ws-relay-handlers.ts`
- `packages/server/src/subscriptions.ts`

---

## Summary: The Complete Picture

To replicate YA's session management:

1. **Scan** the filesystem for agent session files (Section 2) — scan periodically or on file events
2. **Watch** for file changes (Section 12) — detect new and modified sessions in real-time
3. **Track ownership** (Section 11) — distinguish sessions you started vs external ones
4. **Create sessions** via provider abstraction (Section 3) — implement the `AgentProvider` interface for each CLI/SDK
5. **Manage lifecycle** with a state machine (Section 4) — track in-turn/idle/waiting-input/hold/terminated
6. **Limit concurrency** with a worker queue (Section 5) — preempt idle sessions when queue has waiters
7. **Stream messages** through the pipeline (Section 6) — SDK iterator → buffer → augment → WebSocket
8. **Augment** messages server-side (Section 7) — diffs, syntax highlighting, streaming markdown
9. **Buffer** for late-joining clients (Section 8) — dual-bucket with 30-second window
10. **Load history** from heterogeneous file formats (Section 9) — DAG resolution for Claude, linear for Codex/Gemini
11. **Paginate** long sessions (Section 10) — slice at compaction boundaries
12. **Serve** via REST + WebSocket (Section 13) — three subscription channels for different client needs
