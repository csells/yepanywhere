# YepAnywhere Session Client Integration Specification

> **Purpose:** Enable embedding a fully interactive YepAnywhere agent session viewer into a third-party React application. This document details every component, hook, utility, type, and CSS file required, how they connect, and how to initialize them against the YA API.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Session List: Fetching & Displaying Available Sessions](#2-session-list)
3. [Session History: Loading Past Messages](#3-session-history)
4. [Real-Time Streaming: WebSocket Protocol](#4-real-time-streaming)
5. [Session Page Layout: Header, Content, Footer](#5-session-page-layout)
6. [Message Preprocessing & Render Pipeline](#6-message-preprocessing)
7. [Content Rendering: Text, Tools, Thinking](#7-content-rendering)
8. [Streaming Markdown: Server-Rendered HTML](#8-streaming-markdown)
9. [Input Area: Prompts, Approvals, Questions](#9-input-area)
10. [Connection Infrastructure](#10-connection-infrastructure)
11. [Files to Copy](#11-files-to-copy)
12. [Integration Guide](#12-integration-guide)
13. [API Reference](#13-api-reference)

---

## 1. Architecture Overview

The YA client follows a layered architecture:

```
┌──────────────────────────────────────────────────────────────────┐
│  Pages                                                           │
│  SessionPage.tsx ─ top-level orchestrator (header/content/input) │
├──────────────────────────────────────────────────────────────────┤
│  Hooks (state orchestration)                                     │
│  useSession ← useSessionMessages + useSessionStream              │
│              + useStreamingContent + useStreamingMarkdown         │
├──────────────────────────────────────────────────────────────────┤
│  Components                                                      │
│  MessageList → RenderItemComponent → TextBlock / ToolCallRow     │
│  MessageInput, ToolApprovalPanel, QuestionAnswerPanel             │
├──────────────────────────────────────────────────────────────────┤
│  Renderers                                                       │
│  RendererRegistry (block types) + ToolRendererRegistry (17 tools)│
├──────────────────────────────────────────────────────────────────┤
│  Preprocessing                                                   │
│  preprocessMessages() → RenderItem[] → groupItemsIntoTurns()     │
├──────────────────────────────────────────────────────────────────┤
│  Connection Layer                                                │
│  WebSocketConnection / SecureConnection → RelayProtocol          │
│  ConnectionManager (reconnection, keepalive, stale detection)    │
│  ActivityBus (SSE-style event hub)                               │
├──────────────────────────────────────────────────────────────────┤
│  API Client                                                      │
│  api.* methods wrapping fetch / WebSocket relay protocol         │
├──────────────────────────────────────────────────────────────────┤
│  Shared Types (packages/shared/)                                 │
│  Zod schemas, message types, relay protocol, binary framing      │
└──────────────────────────────────────────────────────────────────┘
```

**Key design decisions:**
- **Plain CSS** with CSS custom properties for theming (no Tailwind/CSS-in-JS)
- **No virtual scrolling** — pagination + browser DOM rendering handles long sessions
- **Server-rendered markdown** streamed to client via WebSocket (avoids client-side parsing)
- **Two registry pattern** — one for content block types, one for tool-specific renderers
- **Ref-based streaming** — streaming content updates bypass React state for performance

---

## 2. Session List: Fetching & Displaying Available Sessions {#2-session-list}

### 2.1 API Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/sessions` | Global session list with filters & pagination |
| GET | `/api/sessions/stats` | Counts by status, provider, executor |
| GET | `/api/inbox` | Priority-tiered inbox (needs-attention, active, recent, unread) |
| GET | `/api/recents` | Recently visited sessions |

#### GET `/api/sessions`

**Query parameters:**
- `project` — Filter by project ID
- `q` — Full-text search query (server-side)
- `after` — Pagination cursor (updatedAt timestamp of last item)
- `limit` — Results per page (default: 50)
- `includeArchived` — Include archived sessions
- `starred` — Only starred sessions
- `includeStats` — Include global stats in response

**Response shape:**
```typescript
interface GlobalSessionsResponse {
  sessions: GlobalSessionItem[];
  hasMore: boolean;
  stats: GlobalSessionStats;
  projects: ProjectOption[];
}

interface GlobalSessionItem {
  id: string;
  title: string | null;
  createdAt: string;       // ISO 8601
  updatedAt: string;
  messageCount: number;
  provider: ProviderName;  // "claude" | "codex" | "gemini" | ...
  projectId: string;
  projectName: string;
  ownership: SessionStatus;  // { owner: "self" | "external" | "none"; processId?: string }
  pendingInputType?: PendingInputType;  // "tool-approval" | "user-question"
  activity?: AgentActivity;             // "idle" | "in-turn" | "waiting-input"
  hasUnread?: boolean;
  customTitle?: string;
  isArchived?: boolean;
  isStarred?: boolean;
  executor?: string;
}
// Note: contextUsage is NOT available on GlobalSessionItem — only on full Session objects

```

### 2.2 Primary Hook: `useGlobalSessions`

**File:** `packages/client/src/hooks/useGlobalSessions.ts`

```typescript
function useGlobalSessions(options?: {
  projectId?: string | null;
  searchQuery?: string;
  limit?: number;
  includeArchived?: boolean;
  starred?: boolean;
  includeStats?: boolean;
}): {
  sessions: GlobalSessionItem[];
  stats: GlobalSessionStats;
  projects: ProjectOption[];
  loading: boolean;
  error: Error | null;
  hasMore: boolean;
  loadMore: () => Promise<void>;
  refetch: () => Promise<void>;
}
```

**Real-time updates:** Subscribes to `useFileActivity` for SSE events:
- `session-created` — Adds new session to top of list
- `session-status-changed` — Updates ownership field
- `process-state-changed` — Updates activity indicator
- `session-metadata-changed` — Updates title/star/archive
- `session-updated` — Updates timestamp, message count, context usage
- `reconnect` — Full refetch after SSE reconnection

All updates are debounced (500ms) and applied in-place to preserve list order.

### 2.3 Session List Component: `SessionListItem`

**File:** `packages/client/src/components/SessionListItem.tsx`

Reusable list item with two render modes:

| Mode | Usage | Layout |
|------|-------|--------|
| `card` | GlobalSessionsPage | Full metadata, timestamps, context usage bar |
| `compact` | Sidebar | Single line with abbreviated status badges |

**Key props:**
```typescript
interface SessionListItemProps {
  sessionId: string;
  projectId: string;
  title: string | null;
  mode: "card" | "compact";
  activity?: AgentActivity;
  pendingInputType?: PendingInputType;
  hasUnread?: boolean;
  isStarred?: boolean;
  isArchived?: boolean;
  provider?: ProviderName;
  contextUsage?: ContextUsage;
  onToggleStar?: () => void;
  onToggleArchive?: () => void;
  onNavigate?: () => void;
  basePath?: string;  // For custom routing prefix
}
```

**Navigation:** Renders a React Router `<Link>` to:
```
{basePath}/projects/{projectId}/sessions/{sessionId}
```

**Optimistic updates:** Star/archive toggles update UI immediately, then fire API call. Reverts on failure.

---

## 3. Session History: Loading Past Messages {#3-session-history}

### 3.1 API Endpoint

**GET** `/api/projects/{projectId}/sessions/{sessionId}`

**Query parameters:**
- `afterMessageId` — Load messages after this ID (incremental fetch)
- `tailCompactions` — Number of compaction sections from the end (default: 2)
- `beforeMessageId` — Load messages before this ID (older history)

**Response:**
```typescript
{
  session: Session;
  messages: Message[];
  ownership: SessionStatus;  // { owner: "self" | "external" | "none" }
  pendingInputRequest?: InputRequest | null;
  pagination?: PaginationInfo;
}

interface PaginationInfo {
  hasOlderMessages: boolean;
  totalMessageCount: number;
  returnedMessageCount: number;
  truncatedBeforeMessageId?: string;
  totalCompactions: number;
}
```

Messages are loaded from JSONL files on disk. The `tailCompactions` parameter controls how far back to load — each "compaction" is a context-compression boundary where the conversation was summarized. Loading `tailCompactions: 2` gives approximately the last two "pages" of conversation.

### 3.2 Hook: `useSessionMessages`

**File:** `packages/client/src/hooks/useSessionMessages.ts`

```typescript
interface SessionLoadResult {
  session: Session;
  status: SessionStatus;
  pendingInputRequest?: unknown;
}

function useSessionMessages(options: {
  projectId: string;
  sessionId: string;
  onLoadComplete?: (result: SessionLoadResult) => void;  // Fired after initial REST load
  onLoadError?: (error: Error) => void;                   // Fired on load failure
}): {
  messages: Message[];
  agentContent: AgentContentMap;         // Subagent messages by agentId
  toolUseToAgent: Map<string, string>;   // toolUseId → agentId mapping
  loading: boolean;
  session: Session | null;
  setSession: React.Dispatch<React.SetStateAction<Session | null>>;  // Used by stream events
  pagination: PaginationInfo | undefined; // Undefined until initial REST load completes
  loadingOlder: boolean;
  handleStreamingUpdate: (msg: Message, agentId?: string) => void;
  handleStreamMessageEvent: (msg: Message) => void;
  handleStreamSubagentMessage: (msg: Message, agentId: string) => void;
  registerToolUseAgent: (toolUseId: string, agentId: string) => void;
  setAgentContent: React.Dispatch<React.SetStateAction<AgentContentMap>>;     // For provider wiring
  setToolUseToAgent: React.Dispatch<React.SetStateAction<Map<string, string>>>; // For pending agent loading
  setMessages: React.Dispatch<React.SetStateAction<Message[]>>;               // For streaming placeholder cleanup
  fetchNewMessages: () => Promise<void>;
  fetchSessionMetadata: () => Promise<void>;  // Metadata-only refresh (no message reload)
  loadOlderMessages: () => Promise<void>;
}
```

**Initial load flow:**
1. Component mounts → calls `api.getSession(projectId, sessionId, undefined, { tailCompactions: 2 })`
2. Messages tagged with `_source: "jsonl"` (authoritative source)
3. Stream messages buffered until initial load completes
4. Buffer flushed, `onLoadComplete` fires

**Older message loading:**
- `loadOlderMessages()` uses `beforeMessageId` from pagination
- Scroll position preserved via `requestAnimationFrame` double-buffering

**Message merging:**
- JSONL messages are authoritative over SDK (streaming) messages
- Deduplication by message ID (`uuid ?? id`)
- DAG ordering via `parentUuid` chain fixes out-of-order delivery

### 3.3 Subagent (Task) Content

Agent/Task subagent messages are **not** in the main message array. They're loaded separately:

- **GET** `/api/projects/{projectId}/sessions/{sessionId}/agents/{agentId}` — Returns `AgentSession { messages, status }`
- **GET** `/api/projects/{projectId}/sessions/{sessionId}/agents` — Returns `{ mappings: [{ toolUseId, agentId }] }`

The `agentContent` map (keyed by agent ID) holds these messages. The `TaskRenderer` accesses them via the `toolUseToAgent` mapping during rendering.

---

## 4. Real-Time Streaming: WebSocket Protocol {#4-real-time-streaming}

### 4.1 WebSocket Connection

**Local mode:** Connect to `ws[s]://{host}/api/ws?desktop_token={token}`

**Remote/relay mode:** Connect via `SecureConnection` with SRP authentication + NaCl encryption.

### 4.2 Subscription Model

The WebSocket multiplexes three subscription channels:

| Channel | Purpose | Subscribe Message |
|---------|---------|------------------|
| `session` | Stream events for one session | `{ type: "subscribe", channel: "session", sessionId }` |
| `activity` | Global file/session change events | `{ type: "subscribe", channel: "activity" }` |
| `session-watch` | File changes for non-owned sessions | `{ type: "subscribe", channel: "session-watch", sessionId }` |

### 4.3 Session Stream Events

When subscribed to a session, the server sends these event types:

| `eventType` | When | Data Shape |
|-------------|------|------------|
| `connected` | On subscription start | Sync state: ownership, pending input, process state |
| `message` | SDK message (user/assistant/system) | Full message object with content blocks |
| `stream_event` | Streaming delta | `{ eventType: "content_block_start" \| "content_block_delta" \| "message_start" \| "message_stop" \| ... }` |
| `status` | Process state change | `{ state: "in-turn" \| "idle" \| "waiting-input" \| "hold" }` |
| `markdown-augment` | Server-rendered HTML | `{ messageId, blockIndex?, html, type? }` |
| `pending` | Partial streaming text | `{ html }` |
| `deferred-queue` | Queued message info | Message queued for delivery |
| `complete` | Session ended | Completion signal |
| `mode-change` | Permission mode changed | New mode info |
| `session-id-changed` | Temp → real ID | `{ newSessionId }` |
| `heartbeat` | Keep-alive | Empty |

### 4.4 Stream Event Processing

**Hook:** `useStreamingContent` (`packages/client/src/hooks/useStreamingContent.ts`)

Handles the `stream_event` sub-protocol:

1. **`message_start`** — Captures streaming message ID, initializes accumulator
2. **`content_block_start`** — Initializes new content block (text, thinking, tool_use)
3. **`content_block_delta`** — Accumulates text/thinking deltas (throttled to 50ms batches)
4. **`content_block_stop`** — Block complete
5. **`message_stop`** — Cleans up streaming state

**Throttling:** Trailing-edge batching at 50ms intervals prevents React from being overwhelmed during rapid streaming. Deltas accumulate into a pending set; a `setTimeout(50ms)` fires a single React state update per batch. Note that `content_block_start` events bypass throttling and fire immediately to open new blocks without delay.

**Streaming message shape:**
```typescript
const streamingMessage: Message = {
  id: messageId,
  type: "assistant",
  role: "assistant",
  message: {
    role: "assistant",
    content: partialBlocks  // Array of partially-accumulated blocks
  },
  _isStreaming: true,
  _source: "sdk"
}
```

### 4.5 Hook: `useSessionStream`

**File:** `packages/client/src/hooks/useSessionStream.ts`

```typescript
function useSessionStream(
  sessionId: string | null,
  options: {
    onMessage: (data: { eventType: string; [key: string]: unknown }) => void;
    onError?: (error: Event) => void;
    onOpen?: () => void;
  }
): { connected: boolean; reconnect: () => void }
```

Manages the WebSocket subscription for a single session. Handles:
- Auto-reconnection via ConnectionManager
- Staleness tracking to prevent ghost handlers
- SecureConnection (remote) vs WebSocketConnection (local) routing

### 4.6 Top-Level Orchestrator: `useSession`

**File:** `packages/client/src/hooks/useSession.ts`

Composes all session hooks into a single interface:

```typescript
function useSession(
  projectId: string,
  sessionId: string,
  initialStatus?: { owner: "self"; processId: string },
  streamingMarkdownCallbacks?: StreamingMarkdownCallbacks
): {
  // Session data
  session: Session | null;
  messages: Message[];
  agentContent: AgentContentMap;     // Subagent messages keyed by agentId (Task tool)
  setAgentContent: (updater: (prev: AgentContentMap) => AgentContentMap) => void; // For AgentContentProvider
  toolUseToAgent: Map<string, string>; // Maps Task tool_use_id → agentId (for streaming)
  markdownAugments: Record<string, MarkdownAugment>; // Pre-rendered HTML (keyed by blockId)

  // Process state
  status: SessionStatus;             // { owner: "self" | "external" | "none"; processId?: string }
  processState: ProcessState;        // "idle" | "in-turn" | "waiting-input" | "hold"
  isCompacting: boolean;             // True when context is being compressed
  isHeld: boolean;                   // Derived: processState === "hold"
  pendingInputRequest: InputRequest | null;

  // Session identity
  actualSessionId: string;           // Real server ID — may differ from URL during temp→real
                                     // transition. ALWAYS use this for API calls & InputRequest matching

  // Permission mode
  permissionMode: PermissionMode;    // UI-selected mode (sent with next message)
  modeVersion: number;               // Bumped on each mode change for optimistic update tracking

  // Connection
  connected: boolean;
  sessionWatchConnected: boolean;
  sessionUpdatesConnected: boolean;
  loading: boolean;
  error: Error | null;
  lastStreamActivityAt: string | null; // Last stream activity (ISO 8601 timestamp string)

  // Pending/deferred messages
  pendingMessages: PendingMessage[];       // Awaiting server confirmation ("Sending...")
  deferredMessages: DeferredMessage[];     // Queued server-side ("Queued #n")
  addPendingMessage: (content: string) => string;   // Returns tempId
  removePendingMessage: (tempId: string) => void;
  updatePendingMessage: (tempId: string, fields: Partial<PendingMessage>) => void;

  // Slash commands & tools from session init
  slashCommands: string[];           // Available "/" commands (from init message)
  sessionTools: string[];            // Available tool names
  mcpServers: string[];               // Available MCP server names

  // Pagination
  pagination: PaginationInfo | undefined; // Undefined until initial load completes
  loadingOlder: boolean;
  loadOlderMessages: () => Promise<void>;

  // Actions
  setStatus: React.Dispatch<React.SetStateAction<SessionStatus>>;
  setProcessState: React.Dispatch<React.SetStateAction<ProcessState>>;
  setPermissionMode: (mode: PermissionMode) => Promise<void>;
  setHold: (hold: boolean) => Promise<void>;
  reconnectStream: () => void;       // Force session stream reconnection
}

// Note: UseSessionReturn is NOT a named export. To type it:
type UseSessionReturn = ReturnType<typeof useSession>;
```

> **Critical:** Always use `actualSessionId` (not the URL `sessionId`) for all API calls after
> the initial load. When starting a new session via `POST /api/projects/.../sessions`, the server
> creates a temporary ID. The real session ID arrives via a `session-id-changed` stream event.
> Using the stale temporary ID after the transition causes 404 errors. `useSession` tracks this
> internally and exposes the authoritative ID via `actualSessionId`.

**Message routing logic inside `useSession`:**
1. `stream_event` messages → `useStreamingContent.handleStreamEvent()`
2. `message` events → build Message object, handle subagent routing, merge into messages
3. `status` events → update process state, capture pending input requests
4. `connected` events → sync ownership, fetch incremental messages
5. `markdown-augment` events → dispatch to StreamingMarkdownContext or store as augment
6. `pending` events → dispatch to StreamingMarkdownContext
7. `session-id-changed` → update actual session ID (for temp→real transitions)

---

## 5. Session Page Layout: Header, Content, Footer {#5-session-page-layout}

### 5.1 Layout Structure

**File:** `packages/client/src/pages/SessionPage.tsx`

The session page uses a 3-part flex layout pinned to `100dvh`:

```
┌─────────────────────────────────────────────────────────────────┐
│  <header class="session-header">                     flex: 0   │
│    Title (editable) │ Provider badge │ Model │ Thinking indicator│
│    Gradient fade-out border (::after pseudo-element)            │
├─────────────────────────────────────────────────────────────────┤
│  <main class="session-messages">                     flex: 1   │
│    <MessageList>                          overflow-y: auto      │
│      Load older messages button (if hasOlderMessages)           │
│      Turn groups:                                               │
│        UserPromptBlock (standalone)                              │
│        <div class="assistant-turn">                             │
│          TextBlock, ThinkingBlock, ToolCallRow (grouped)        │
│        </div>                                                   │
│      Pending messages ("Sending...")                             │
│      Deferred messages ("Queued #n")                            │
│      Compacting indicator                                       │
│    </MessageList>                                               │
├─────────────────────────────────────────────────────────────────┤
│  <footer class="session-input">                      flex: 0   │
│    Connection status bar                                        │
│    QuestionAnswerPanel (if pending user-question)               │
│    ToolApprovalPanel (if pending tool-approval)                 │
│    MessageInput (textarea + toolbar)                            │
│    MessageInputToolbar (mode, attach, voice, send)              │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 CSS Layout

```css
.session-page {
  display: flex;
  flex-direction: column;
  height: 100dvh;           /* Dynamic viewport height for mobile */
  overflow: hidden;
  background: var(--bg-surface);
}

.session-header {
  flex-shrink: 0;
  position: relative;
  z-index: 10;
}

.session-header::after {
  /* 24px gradient fade below header */
  content: "";
  position: absolute;
  bottom: -24px;
  height: 24px;
  background: linear-gradient(var(--bg-surface), transparent);
}

.session-messages {
  flex: 1;
  min-height: 0;             /* Critical: allows flex child overflow */
  overflow-y: auto;
  -webkit-overflow-scrolling: touch;
}

.session-input {
  flex-shrink: 0;
  padding-bottom: env(safe-area-inset-bottom, 0px);  /* Mobile safe area */
}
```

### 5.3 Header Details

The header renders:
- **Back button** (sidebar toggle on mobile)
- **Session title** — inline editable, with `RecentSessionsDropdown` for switching
- **Archived badge** — if session is archived
- **Session menu** (three-dot) — rename, star, archive, clone, delete
- **Provider badge** — "Claude", "Codex", "Gemini" with colored icon
- **Model name** — e.g., "opus", "sonnet"
- **Thinking indicator** — animated pulse when agent is thinking

### 5.4 Scroll Management (MessageList)

**File:** `packages/client/src/components/MessageList.tsx`

- **ResizeObserver** detects content height changes (handles async markdown rendering)
- **Auto-scroll** enabled when user is within 100px of bottom
- **`shouldAutoScrollRef`** tracks scroll intent; disabled on user scroll-up
- **`isProgrammaticScrollRef`** distinguishes user scrolls from code-triggered scrolls
- **Follow-up scroll** after 50ms catches late-rendered content (syntax highlighting, images)
- **Older message loading** preserves scroll position via double `requestAnimationFrame`

---

## 6. Message Preprocessing & Render Pipeline {#6-message-preprocessing}

### 6.1 `preprocessMessages()`

**File:** `packages/client/src/lib/preprocessMessages.ts`

```typescript
function preprocessMessages(
  messages: Message[],
  augments?: PreprocessAugments
): RenderItem[]
```

Converts raw `Message[]` into a unified `RenderItem[]` array:

1. Iterates messages, routing by type (user/assistant/system)
2. Extracts content blocks from assistant messages
3. **Pairs `tool_use` with `tool_result`** via `pendingToolCalls` map (keyed by `tool_use.id`)
4. Marks orphaned tool calls as `"aborted"` (from `msg.orphanedToolUseIds`)
5. Collapses consecutive session-setup user prompts into a single collapsible item
6. Enriches augment HTML from server-rendered markdown

### 6.2 RenderItem Types

```typescript
type RenderItem =
  | TextItem          // Assistant text content
  | ThinkingItem      // Extended thinking block
  | ToolCallItem      // tool_use + tool_result paired
  | UserPromptItem    // User message
  | SessionSetupItem  // Collapsed setup prompts
  | SystemItem        // Errors, compaction boundaries, turn_aborted, api_error

// ToolCallItem is the most complex:
interface ToolCallItem {
  type: "tool_call";
  id: string;           // tool_use.id
  toolName: string;     // tool_use.name (e.g., "Bash", "Edit", "Read")
  toolInput: unknown;   // tool_use.input
  toolResult?: {
    content: string;
    isError: boolean;
    structured?: unknown;  // From toolUseResult field
  };
  status: "pending" | "complete" | "error" | "aborted";
  sourceMessages: Message[];
}
```

### 6.3 Turn Grouping

**Function:** `groupItemsIntoTurns()` in `MessageList.tsx`

Groups consecutive assistant items (text → thinking → tool_call) into "turns" for timeline rendering:

```typescript
function groupItemsIntoTurns(items: RenderItem[]): Array<{
  isUserPrompt: boolean;
  items: RenderItem[];
}>
```

- User prompts → standalone group
- Consecutive assistant items → grouped into `<div class="assistant-turn">` with timeline indent

### 6.4 Component Dispatch

**File:** `packages/client/src/components/RenderItemComponent.tsx`

Switch statement maps RenderItem types to components:

| RenderItem Type | Component |
|----------------|-----------|
| `text` | `TextBlock` |
| `thinking` | `ThinkingBlock` |
| `tool_call` | `ToolCallRow` |
| `user_prompt` | `UserPromptBlock` |
| `session_setup` | `SessionSetupBlock` |
| `system` | Inline system message div |

---

## 7. Content Rendering: Text, Tools, Thinking {#7-content-rendering}

### 7.1 TextBlock

**File:** `packages/client/src/components/blocks/TextBlock.tsx`

Three rendering modes:
1. **Streaming** — Server-rendered HTML blocks appended to DOM refs (bypasses React)
2. **Completed with augment** — Pre-rendered HTML from `augmentHtml` prop via `dangerouslySetInnerHTML`
3. **Plain text fallback** — Raw `<p>` tag when no augment available

### 7.2 ToolCallRow

**File:** `packages/client/src/components/blocks/ToolCallRow.tsx`

Expandable row with status indicator:

```
┌─ [spinner/check/X] ToolName: summary text ─── [▸/▾] ─┐
│  Expanded content (tool input + result)                 │
└─────────────────────────────────────────────────────────┘
```

Uses `ToolRendererRegistry` to dispatch to specialized renderers by tool name.

### 7.3 Tool Renderer Registry

**File:** `packages/client/src/components/renderers/tools/index.tsx`

17 registered tool renderers with name aliases for SDK compatibility:

| Tool | Renderer | Aliases |
|------|----------|---------|
| Bash | `BashRenderer` | `shell_command`, `exec_command` |
| Read | `ReadRenderer` | — |
| Edit | `EditRenderer` | `apply_patch` |
| Write | `WriteRenderer` | — |
| Glob | `GlobRenderer` | — |
| Grep | `GrepRenderer` | — |
| TodoWrite | `TodoWriteRenderer` | — |
| Task | `TaskRenderer` | — |
| WebSearch | `WebSearchRenderer` | `web_search_call`, `search_query` |
| WebFetch | `WebFetchRenderer` | — |
| AskUserQuestion | `AskUserQuestionRenderer` | — |
| ExitPlanMode | `ExitPlanModeRenderer` | — |
| UpdatePlan | `UpdatePlanRenderer` | `update_plan` |
| WriteStdin | `WriteStdinRenderer` | `write_stdin` |
| BashOutput | `BashOutputRenderer` | — |
| TaskOutput | `TaskOutputRenderer` | — |
| KillShell | `KillShellRenderer` | — |

Each renderer provides multiple render paths:
- `renderToolUse()` — Input display (expanded)
- `renderToolResult()` — Result display (expanded)
- `renderCollapsedPreview()` — Compact preview when collapsed
- `renderInteractiveSummary()` — Rich interactive view (e.g., Edit shows diff inline)
- `renderInline()` — Bypass tool-row wrapper entirely

### 7.3a RenderContext

Every `ToolRenderer` method receives a `RenderContext` as its second argument. Integrators building custom tool renderers need this type, and `TaskRenderer` uses `toolUseId` to look up agent content during streaming:

```typescript
// File: packages/client/src/components/renderers/types.ts
interface RenderContext {
  isStreaming: boolean;             // True if message is still streaming
  theme: "light" | "dark";         // Current theme
  toolUseId?: string;               // Used by TaskRenderer to look up agentId during streaming
  getToolUse?: (id: string) => { name: string; input: unknown } | undefined;
  toolUseResult?: unknown;          // Structured tool result from message.toolUseResult field
  thinkingExpanded?: boolean;       // Shared thinking-block expansion state
  toggleThinkingExpanded?: () => void;
  provider?: string;                // Provider type — some renderers have provider-specific fallbacks
}
```

### 7.4 ThinkingBlock

**File:** `packages/client/src/components/blocks/ThinkingBlock.tsx`

Collapsible extended thinking content. Shows streaming indicator when `status === "streaming"`.

### 7.5 UserPromptBlock

**File:** `packages/client/src/components/blocks/UserPromptBlock.tsx`

User messages with:
- `CollapsibleText` — Truncates at 12 lines / ~1200 chars with "Show more"
- File attachment chips (clickable for image preview)
- Opened files metadata list

---

## 8. Streaming Markdown: Server-Rendered HTML {#8-streaming-markdown}

### 8.1 Architecture

The server renders markdown to HTML (with Shiki syntax highlighting) and streams it to the client. This avoids client-side markdown parsing entirely.

**Flow:**
```
Server: markdown → HTML (shiki)
  ↓ WebSocket
Client: useSession handles "markdown-augment" / "pending" events
  ↓
StreamingMarkdownContext dispatches to registered handler
  ↓
useStreamingMarkdown hook mutates DOM refs directly (no React re-render)
  ↓
TextBlock component shows streaming container or completed augment
```

### 8.2 Hook: `useStreamingMarkdown`

**File:** `packages/client/src/hooks/useStreamingMarkdown.ts`

```typescript
function useStreamingMarkdown(): {
  containerRef: RefObject<HTMLDivElement | null>;  // Completed blocks
  pendingRef: RefObject<HTMLSpanElement | null>;   // Trailing partial text
  isStreaming: boolean;
  onAugment: (augment: { blockIndex: number; html: string; type: string }) => void;
  onPending: (pending: { html: string }) => void;
  onStreamEnd: () => void;
  reset: () => void;
  captureHtml: () => string | null;
}
```

- **`containerRef`** — DOM node holding completed streaming blocks as `<div class="streaming-block" data-blockIndex="N">`
- **`pendingRef`** — DOM node showing incomplete/trailing text (updated on every `pending` event)
- **Out-of-order handling** — Tracks `maxBlockIndexRef` and inserts blocks at correct DOM position
- **Direct DOM mutation** — Bypasses React rendering for streaming performance

### 8.3 Context: `StreamingMarkdownContext`

**File:** `packages/client/src/contexts/StreamingMarkdownContext.tsx`

Event bus connecting WebSocket events to the active TextBlock:

```typescript
interface StreamingMarkdownContextValue {
  registerStreamingHandler: (handlers: StreamingHandlers) => () => void;
  dispatchAugment: (augment) => void;
  dispatchPending: (pending) => void;
  dispatchStreamEnd: () => void;
  setCurrentMessageId: (messageId: string | null) => void;
  captureStreamingHtml: () => string | null;
}
```

Only one handler is active at a time (the currently-streaming TextBlock).

### 8.4 Edit Augments

Edit tool results include server-computed diffs:

```typescript
interface EditInputWithAugment {
  // Standard edit fields
  file_path: string;
  old_string: string;
  new_string: string;
  // Server-injected augments
  _structuredPatch?: PatchHunk[];  // Parsed unified diff
  _diffHtml?: string;              // Shiki-highlighted diff HTML
  _rawPatch?: string;              // Raw patch text
}
```

The `EditRenderer` uses `_diffHtml` (preferred) or `_structuredPatch` (fallback) to show colored diffs.

---

## 9. Input Area: Prompts, Approvals, Questions {#9-input-area}

### 9.1 MessageInput

**File:** `packages/client/src/components/MessageInput.tsx`

Multi-line textarea with:
- **Collapse/expand toggle** — Floating button above input
- **Auto-resize** — 3 rows expanded, 1 row collapsed
- **Keyboard shortcuts** — Enter sends (desktop), Ctrl+Enter queues deferred message
- **File attachments** — Paste images or click attach button
- **Voice input** — Via `VoiceInputButton` component
- **Draft persistence** — Saves to localStorage as `draft-message-{sessionId}`

### 9.2 MessageInputToolbar

**File:** `packages/client/src/components/MessageInputToolbar.tsx`

Toolbar below textarea with:
- Permission mode selector (default / acceptEdits / plan / bypassPermissions)
- Thinking toggle (off / auto / on with effort level)
- Attach button
- Voice input button
- Send / Queue buttons

### 9.3 ToolApprovalPanel

**File:** `packages/client/src/components/ToolApprovalPanel.tsx`

Shown when `pendingInputRequest.type === "tool-approval"`:
- Displays tool name and input preview
- Approve / Deny buttons
- "Always allow" option
- Collapsible details view

**Props interface:**
```typescript
interface ToolApprovalPanelProps {
  request: InputRequest;                                    // The pending input request
  sessionId: string;                                        // Use actualSessionId
  onApprove: () => Promise<void>;                          // Required — approve the tool call
  onDeny: () => Promise<void>;                             // Required — deny the tool call
  onApproveAcceptEdits?: () => Promise<void>;              // Approve + switch to acceptEdits mode
  onDenyWithFeedback?: (feedback: string) => Promise<void>; // Deny with explanation
  collapsed?: boolean;                                      // Externally-controlled collapse
  onCollapsedChange?: (collapsed: boolean) => void;        // Collapse state callback
}
```

The component does NOT make API calls itself — it delegates to callback props. The caller must wire up the approve/deny handlers, typically via `api.respondToInput()`:

```typescript
// respondToInput takes positional args: (sessionId, requestId, response, answers?, feedback?)
const handleApprove = () => api.respondToInput(actualSessionId, pendingInputRequest.id, 'approve');
const handleDeny = () => api.respondToInput(actualSessionId, pendingInputRequest.id, 'deny');
```

### 9.4 QuestionAnswerPanel

**File:** `packages/client/src/components/QuestionAnswerPanel.tsx`

Shown when `pendingInputRequest.type === "user-question"`:
- Displays the agent's question
- Text input for answer
- Option buttons if `options` provided

**Props interface:**
```typescript
interface QuestionAnswerPanelProps {
  request: InputRequest;                                         // The pending input request
  sessionId: string;                                             // Use actualSessionId
  onSubmit: (answers: Record<string, string>) => Promise<void>; // Required — submit answers
  onDeny: () => Promise<void>;                                  // Required — dismiss question
}
```

Like `ToolApprovalPanel`, this component is callback-driven. Wire up the handlers:

```typescript
// respondToInput takes positional args: (sessionId, requestId, response, answers?, feedback?)
const handleSubmit = (answers: Record<string, string>) =>
  api.respondToInput(actualSessionId, pendingInputRequest.id, 'approve', answers);
const handleDenyQuestion = () =>
  api.respondToInput(actualSessionId, pendingInputRequest.id, 'deny');
```

### 9.5 Sending Messages

**New session:**
```
POST /api/projects/{projectId}/sessions
Body: { message, mode?, model?, thinking?, provider?, executor?, attachments? }
Response: { sessionId, processId, permissionMode, modeVersion }
```

**Existing session (resume):**
```
POST /api/projects/{projectId}/sessions/{sessionId}/resume
Body: { message, mode?, model?, thinking?, provider?, executor?, attachments?, tempId? }
Response: { processId, permissionMode, modeVersion }
```

**Queue message to running session:**
```
POST /api/sessions/{sessionId}/messages
Body: { message, mode?, attachments?, tempId?, thinking?, deferred? }
Response: { queued, restarted?, processId?, deferred? }
```

---

## 10. Connection Infrastructure {#10-connection-infrastructure}

### 10.1 Connection Interface

**File:** `packages/client/src/lib/connection/types.ts`

```typescript
interface Connection {
  mode: "direct" | "secure";
  fetch<T>(path: string, init?: RequestInit): Promise<T>;
  fetchBlob(path: string): Promise<Blob>;
  subscribeSession(sessionId, handlers, lastEventId?): Subscription;
  subscribeActivity(handlers): Subscription;
  subscribeSessionWatch(sessionId, handlers, options?): Subscription;
  upload(projectId, sessionId, file, options?): Promise<UploadedFile>;
  forceReconnect?(): Promise<void>;
}
```

### 10.2 Two Connection Implementations

| Class | Mode | Auth | Use Case |
|-------|------|------|----------|
| `WebSocketConnection` | `direct` | Desktop token in URL | Local / LAN access |
| `SecureConnection` | `secure` | SRP-6a + NaCl encryption | Remote / relay access |

Both use `RelayProtocol` internally to multiplex HTTP-like requests and subscriptions over WebSocket.

### 10.3 ConnectionManager

**File:** `packages/client/src/lib/connection/ConnectionManager.ts`

State machine managing WebSocket lifecycle:
- **Exponential backoff** — 1s → 30s max with jitter
- **Stale detection** — Reconnects if no events for 45s
- **Visibility-based health** — Pings on tab restore, reconnects on pong timeout (2s)
- **Non-retryable errors** — Auth failures (4001, 4003) stop reconnection

### 10.4 ActivityBus

**File:** `packages/client/src/lib/activityBus.ts`

Singleton event hub for global events:

```typescript
class ActivityBus {
  connect(): void;
  disconnect(): void;
  on<K>(eventType: K, callback: (event) => void): () => void;
  connected: boolean;
}
```

Subscribes to the `activity` channel and distributes events to all listeners (session list, inbox, sidebar).

### 10.5 RelayProtocol

**File:** `packages/client/src/lib/connection/RelayProtocol.ts`

HTTP-over-WebSocket multiplexer:
- **Requests:** `{ type: "request", id, method, path, body }` → `{ type: "response", id, status, body }`
- **Subscriptions:** `{ type: "subscribe", subscriptionId, channel }` → `{ type: "event", subscriptionId, eventType, data }`
- **Uploads:** Chunked binary upload with progress callbacks
- **Keepalive:** Ping/pong for liveness detection

---

## 11. Files to Copy {#11-files-to-copy}

### 11.0 Integration Strategy

The session chat files fall into two categories:

1. **Copy wholesale** (§11.1) — Files that are 100% session chat UI. These exist solely to render and interact with an agent conversation. Copy them directly into your project.

2. **Extract what you need** (§11.2) — Files that are general YA infrastructure (API client, connection layer, type definitions, toast/modal primitives). Your app likely has its own versions of these. Don't copy them — extract the specific functions, types, and endpoints the session chat needs and wire them into your own infrastructure.

After copying §11.1 files and stubbing/adapting §11.2 dependencies, run `tsc --noEmit` to catch anything missed.

### 11.1 Copy Wholesale — Session Chat Files

These files exist solely for the agent session chat. Copy them as-is (adjusting import paths).

**Components:**

| File | Purpose |
|------|---------|
| `components/MessageList.tsx` | Scrollable message container with turn grouping, scroll management |
| `components/MessageInput.tsx` | Prompt textarea with drafts, keyboard shortcuts, file attachments |
| `components/MessageInputToolbar.tsx` | Toolbar: mode selector, thinking toggle, attach, send/stop |
| `components/RenderItemComponent.tsx` | Dispatches RenderItem types to block components |
| `components/ProcessingIndicator.tsx` | Animated "Thinking..." indicator with fun phrases |
| `components/ToolApprovalPanel.tsx` | Tool approval UI: Yes/No/Accept Edits, keyboard shortcuts |
| `components/QuestionAnswerPanel.tsx` | Agent question UI: tabs, options, text input |
| `components/ProviderBadge.tsx` | Provider icon/label (Claude, Codex, Gemini) |
| `components/StatusBadge.tsx` | Session status badges (External, Approval Needed, Thinking) |
| `components/ContextUsageIndicator.tsx` | SVG pie chart for context window usage |
| `components/ThinkingIndicator.tsx` | Pulsing dot indicator |
| `components/SchemaWarning.tsx` | Schema validation warning badge (used by 14/17 tool renderers) |
| `components/ModeSelector.tsx` | Permission mode selector (bottom sheet / dropdown) |
| `components/SlashCommandButton.tsx` | Slash command dropdown menu |
| `components/tools/summaries.ts` | Tool summary text for collapsed tool call view |
| `components/blocks/TextBlock.tsx` | Text/markdown rendering with streaming |
| `components/blocks/ToolCallRow.tsx` | Tool use/result display |
| `components/blocks/UserPromptBlock.tsx` | User message bubble |
| `components/blocks/ThinkingBlock.tsx` | Extended thinking display |
| `components/blocks/SessionSetupBlock.tsx` | Collapsed session setup |
| `components/renderers/ContentBlockRenderer.tsx` | Subagent content rendering |
| `components/renderers/registry.ts` | Content block type dispatch registry |
| `components/renderers/types.ts` | `ContentBlock`, `RenderContext`, `ContentRenderer` interfaces |
| `components/renderers/index.ts` | Registry initialization |
| `components/renderers/tools/index.tsx` | Tool renderer registry + all 17 registrations |
| `components/renderers/tools/types.ts` | Tool-specific types (`EditInputWithAugment`, etc.) |
| `components/renderers/tools/*.tsx` | All 17 tool renderers (Bash, Edit, Read, Write, Glob, Grep, TodoWrite, Task, WebSearch, WebFetch, AskUserQuestion, ExitPlanMode, UpdatePlan, WriteStdin, BashOutput, TaskOutput, KillShell) |

**Contexts:**

| File | Purpose |
|------|---------|
| `contexts/StreamingMarkdownContext.tsx` | Event bus connecting WebSocket streaming to active TextBlock |
| `contexts/AgentContentContext.tsx` | Subagent content for Task renderer; lazy-loads via API |
| `contexts/SchemaValidationContext.tsx` | Schema validation error reporting |
| `contexts/SessionMetadataContext.tsx` | Provides `projectId`, `projectPath`, `sessionId` |

**Hooks:**

| File | Purpose |
|------|---------|
| `hooks/useSession.ts` | Top-level orchestrator composing all session hooks |
| `hooks/useSessionMessages.ts` | Message loading/merging from REST with DAG ordering |
| `hooks/useSessionStream.ts` | WebSocket subscription for live session |
| `hooks/useSessionWatchStream.ts` | File change subscription for non-owned sessions |
| `hooks/useStreamingContent.ts` | Stream delta accumulation with 50ms throttled batching |
| `hooks/useStreamingMarkdown.ts` | DOM-based streaming rendering (bypasses React state) |
| `hooks/useStreamingEnabled.ts` | localStorage toggle for streaming preference |
| `hooks/useFileActivity.ts` | SSE event subscription |
| `hooks/useDraftPersistence.ts` | Draft text persistence to localStorage |
| `hooks/useDrafts.ts` | Draft tracking for approval feedback and question answers |
| `hooks/useFunPhrases.ts` | Fun phrases toggle for ProcessingIndicator |
| `hooks/useRemoteImage.ts` | Image loading via relay connection |
| `hooks/useModelSettings.ts` | Model/thinking/voice preferences |
| `hooks/useExpandedDiff.ts` | Expanded diff context fetching |
| `hooks/useSchemaValidation.ts` | Schema validation settings |

**Libs:**

| File | Purpose |
|------|---------|
| `lib/preprocessMessages.ts` | `Message[]` → `RenderItem[]` conversion |
| `lib/mergeMessages.ts` | Message merging/deduplication with DAG ordering |
| `lib/pendingTasks.ts` | Finds pending Task tool_use blocks |
| `lib/sessionFile.ts` | Extracts session ID from file change events |
| `lib/validateToolResult.ts` | Validates tool results against Zod schemas |
| `lib/classifyToolError.ts` | Error classification (user_rejection, command_failure, etc.) |
| `lib/parseUserPrompt.ts` | Parses user prompts, extracts IDE metadata |
| `types/renderItems.ts` | `RenderItem` union type and all item interfaces |
| `constants.ts` | `ENTER_SENDS_MESSAGE` constant |

**Shared package (copy wholesale):**

| File | Purpose |
|------|---------|
| `shared/src/dag.ts` | `orderByParentChain()` for parentUuid-linked messages |
| `shared/src/ideMetadata.ts` | IDE metadata tag parsing |
| `shared/src/session/SessionView.ts` | Session display title utilities |
| `shared/src/session/UnifiedSession.ts` | Multi-provider session abstraction |
| `shared/src/session/index.ts` | Barrel exports |
| `shared/src/claude-sdk-schema/types.ts` | SDK message & content block type definitions |

**Styles:**

| File | Purpose |
|------|---------|
| `styles/index.css` | Main styles (imports renderers.css and tool-rows.css) |
| `styles/renderers.css` | Content block styles |
| `styles/tool-rows.css` | Tool-specific styles |

### 11.2 Extract What You Need — App Infrastructure

These files are general YA app infrastructure. **Don't copy them whole** — extract the specific functions, types, and endpoints the session chat needs and integrate them into your own app's equivalent.

#### `api/client.ts` — API Client

The YA API client has ~90 endpoints. The session chat only needs these:

**Session CRUD:**
- `getSession(projectId, sessionId, beforeMessageId?, options?)` — Load session + messages
- `getSessionMetadata(projectId, sessionId)` — Metadata-only refresh
- `startSession(projectId, message, options?)` — Create new session
- `resumeSession(projectId, sessionId, message, options?)` — Send message to existing session
- `cloneSession(projectId, sessionId)` — Clone a session

**Session interaction:**
- `respondToInput(sessionId, requestId, response, answers?, feedback?)` — Tool approval / question answer
- `setPermissionMode(projectId, sessionId, mode, modeVersion)` — Change permission mode
- `setHold(projectId, sessionId, hold)` — Pause/resume agent
- `abortProcess(projectId, sessionId, processId)` — Stop the agent
- `cancelDeferredMessage(projectId, sessionId, queueId)` — Cancel queued message

**Session list:**
- `getSessions(params?)` — Global session list with filters
- `getSessionStats()` — Counts by status, provider
- `getRecentSessions()` — Recently visited
- `markSessionSeen(projectId, sessionId)` — Mark as read
- `updateSessionMetadata(projectId, sessionId, metadata)` — Rename, star, archive

**Subagent content:**
- `getAgentSession(projectId, sessionId, agentId)` — Load subagent messages
- `getAgentMappings(projectId, sessionId)` — toolUseId → agentId mappings

**Support:**
- `expandDiffContext(projectId, sessionId, params)` — Expand diff hunks
- `getProject(projectId)` — Returns `{ project: Project }` (for projectPath)
- `getVersion()` — Server version

**Types to extract:** `PaginationInfo`, `GlobalSessionItem`, `GlobalSessionStats`, `ProjectOption`, `SessionOptions`

**Infrastructure:** The `fetchJSON()` helper routes through the global `Connection` in remote mode or uses native `fetch` for local. You'll need equivalent routing logic.

#### `api/upload.ts` — File Upload

Chunked upload over WebSocket. Extract `uploadFile()` if you support file attachments in messages.

#### `types.ts` — Client Types

Extract these types (the rest are app-level):
- `Message`, `Session`, `SessionSummary`, `SessionStatus`, `Project`
- `InputRequest`, `PermissionMode`, `ContentBlock`
- Re-exported shared types: `ProviderName`, `ContextUsage`, `AgentActivity`, `PendingInputType`

#### `shared/src/types.ts` — Shared Types

Extract these (the rest are server/file-viewer infrastructure):
- `ProviderName`, `ALL_PROVIDERS`, `ModelInfo`, `SlashCommand`, `ProviderInfo`
- `PermissionMode`, `ModelOption`, `ThinkingOption`, `EffortLevel`
- `SessionOwnership`, `EditAugment`, `PatchHunk`, `MarkdownAugment`
- `thinkingOptionToConfig()`, `resolveModel()`, `getModelContextWindow()`

#### `shared/src/app-types.ts` — App Content Types

Extract these (skip server-side types like `SessionStartOptions`, `ServerStatus`):
- `AppContentBlock` and constituent types
- `AppMessage`, `AppAssistantMessage`, `AppUserMessage`
- `AppSessionSummary`
- `ContextUsage`, `AgentActivity`, `PendingInputType`
- Type guards: `isToolUseBlock`, `isToolResultBlock`, `isTextBlock`, `isThinkingBlock`

#### `shared/src/projectId.ts` — Project ID Encoding

Extract `UrlProjectId`, `toUrlProjectId()`, `fromUrlProjectId()` — used in all API calls.

#### `shared/src/relay.ts` + `shared/src/binary-framing.ts` — Relay Protocol

Only needed if supporting remote/relay connections. Skip for direct-only integrations.

#### `lib/connection/` — Connection Layer

The connection layer manages WebSocket transport, reconnection, and relay multiplexing. Your app likely has its own transport. What the session chat needs:

- **`Connection` interface** (from `types.ts`): `fetch()`, `subscribe()`, `upload()`, `close()`
- **`getGlobalConnection()`** / **`setGlobalConnection()`**: Global singleton — hooks use this to route requests
- **`connectionManager`**: Reconnection state machine with backoff
- **`isRemoteClient()`**: Determines direct vs relay mode

Implement the `Connection` interface with your own transport, or copy `DirectConnection.ts` / `WebSocketConnection.ts` for local connections.

#### `lib/activityBus.ts` — SSE Event Hub

Singleton connecting to `/api/activity/events` for file change notifications. Used by `useFileActivity` and `useSessionWatchStream`. Extract the event types and SSE subscription pattern, or replace with your own real-time event system.

#### `lib/deviceDetection.ts` — Device Detection

Extract `hasCoarsePointer()` (Enter vs Shift+Enter behavior in MessageInput) and `isMobileDevice()` (layout decisions). Most apps have their own device detection.

#### `lib/storageKeys.ts` — localStorage Keys

Extract the `UI_KEYS` object and `getServerScopedKey()`. Skip the migration logic.

#### `lib/uuid.ts` — UUID Generation

Extract `generateUUID()`, or use your own UUID utility. Trivial to replace.

#### `providers/registry.ts` — Provider Registry

Extract `getProvider()` (used for DAG ordering support check) and `getModelContextWindow()` (used for context usage calculation). The full registry covers all YA providers — you only need entries for the providers you support.

#### UI Primitives You Probably Already Have

These files provide generic UI infrastructure. **Use your own app's equivalents instead:**

| YA File | What session chat needs from it | Your replacement |
|---------|-------------------------------|------------------|
| `contexts/ToastContext.tsx` | `useToastContext()` for SchemaValidation error toasts | Your toast system |
| `components/Toast.tsx` | Toast container rendering | Your toast component |
| `hooks/useToast.ts` | Toast state management | Your toast hook |
| `components/ui/Modal.tsx` | Modal for file viewer, schema warnings | Your modal component |
| `components/ConnectionBar.tsx` | Connection status bar | Your status indicator |
| `hooks/useActivityBusState.ts` | Transport connection state | Your connection state |
| `hooks/useDeveloperMode.ts` | `holdEnabled` flag, `connectionBarsEnabled` | Your settings system |
| `components/SessionListItem.tsx` | Session card rendering (uses React Router `Link`) | Your list item with your router |
| `components/RecentSessionsDropdown.tsx` | Session switcher (uses React Router) | Your dropdown with your router |
| `components/VoiceInputButton.tsx` | Speech-to-text (uses Web Speech API) | Optional — omit if not needed |
| `components/FilePathLink.tsx` | Clickable file paths | Optional — omit if not needed |
| `hooks/useGlobalSessions.ts` | Session list data with SSE updates | Your data fetching pattern |
| `hooks/useRecentSessions.ts` | Recent sessions list | Simple API wrapper — rewrite |

### 11.3 Reference Files

These are NOT needed for the integration but are useful as working examples:

| File | Why it's useful |
|------|----------------|
| `pages/SessionPage.tsx` | Complete reference for how all hooks and providers wire together |
| `pages/GlobalSessionsPage.tsx` | Reference for session list with filtering, bulk actions |

---

## 12. Integration Guide {#12-integration-guide}

### 12.1 Prerequisites

- React 18+
- React Router v6 (for navigation links in `SessionListItem`)
- `zod` — Used by tool result validators (`SchemaValidationContext`)
- `@yep-anywhere/shared` — Shared types, schemas, and utilities (the `packages/shared` package)
- A running YepAnywhere server instance

### 12.2 Minimal Session Viewer

To embed a read-only session viewer in your app:

**Step 1: Configure the API client**

The `api` object in `client.ts` uses `fetchJSON()` which routes through the global connection or falls back to native `fetch`. For local access:

```typescript
// Set your YA server URL
// The api client uses window.location by default.
// For a different host, you'll need to modify fetchJSON's base URL
// or set up a proxy in your dev server.
```

**Step 2: Load a session**

```tsx
import { useSession } from './hooks/useSession';
import type { StreamingMarkdownCallbacks } from './hooks/useSession';
import { useStreamingMarkdownContext } from './contexts/StreamingMarkdownContext';

// UseSessionReturn is NOT a named export — derive it locally:
type UseSessionReturn = ReturnType<typeof useSession>;

// MessageList calls preprocessMessages internally — do NOT do it yourself
// NOTE: useSession is called in SessionWrapper (Step 3) and passed as a prop
// to avoid duplicate WebSocket connections.
function MySessionView({ projectId, sessionId, sessionState }: {
  projectId: string;
  sessionId: string;
  sessionState: UseSessionReturn;
}) {
  const {
    session,
    messages,
    markdownAugments,
    actualSessionId,       // Use this for all API calls, not the URL sessionId
    status,
    processState,
    isCompacting,
    loading,
    connected,
    pendingInputRequest,
    pendingMessages,
    deferredMessages,
    pagination,
    loadingOlder,
    loadOlderMessages,
    setPermissionMode,
    permissionMode,
    slashCommands,
  } = sessionState;

  const handleSend = (text: string) => {
    api.resumeSession(projectId, actualSessionId, text, { mode: permissionMode });
  };

  // ToolApprovalPanel requires callback props (it does NOT make API calls itself)
  // respondToInput takes positional args: (sessionId, requestId, response, answers?, feedback?)
  const handleApprove = () =>
    api.respondToInput(actualSessionId, pendingInputRequest!.id, 'approve');
  const handleDeny = () =>
    api.respondToInput(actualSessionId, pendingInputRequest!.id, 'deny');

  // QuestionAnswerPanel callback wiring
  const handleSubmitAnswer = (answers: Record<string, string>) =>
    api.respondToInput(actualSessionId, pendingInputRequest!.id, 'approve', answers);

  if (loading) return <div>Loading...</div>;

  return (
    <div className="session-page">
      <header className="session-header">
        <h1>{session?.title ?? 'Untitled'}</h1>
      </header>

      <main className="session-messages">
        {/* MessageList calls preprocessMessages internally — pass raw messages */}
        <MessageList
          messages={messages}
          provider={session?.provider}
          markdownAugments={markdownAugments}
          isProcessing={processState === 'in-turn'}
          isCompacting={isCompacting}
          pendingMessages={pendingMessages}
          deferredMessages={deferredMessages}
          hasOlderMessages={pagination?.hasOlderMessages ?? false}
          onLoadOlderMessages={loadOlderMessages}
          loadingOlder={loadingOlder}
        />
      </main>

      <footer className="session-input">
        {pendingInputRequest?.type === 'tool-approval' && (
          <ToolApprovalPanel
            request={pendingInputRequest}
            sessionId={actualSessionId}
            onApprove={handleApprove}
            onDeny={handleDeny}
          />
        )}
        {pendingInputRequest?.type === 'user-question' && (
          <QuestionAnswerPanel
            request={pendingInputRequest}
            sessionId={actualSessionId}
            onSubmit={handleSubmitAnswer}
            onDeny={handleDeny}
          />
        )}
        <MessageInput
          draftKey={`draft-message-${sessionId}`}
          isRunning={status.owner === 'self'}
          isThinking={processState === 'in-turn'}
          collapsed={!!pendingInputRequest}  // Collapse input when approval panel showing
          mode={permissionMode}
          onModeChange={setPermissionMode}
          slashCommands={slashCommands}
          sessionId={actualSessionId}
          projectId={projectId}
          onSend={handleSend}
        />
      </footer>
    </div>
  );
}
```

**Step 3: Wrap with required context providers**

Tool renderers will crash without their context providers. You need **all five**:

```tsx
import { useSession } from './hooks/useSession';
import { useStreamingMarkdownContext, StreamingMarkdownProvider } from './contexts/StreamingMarkdownContext';
import { AgentContentProvider } from './contexts/AgentContentContext';
import { SessionMetadataProvider } from './contexts/SessionMetadataContext';
import { SchemaValidationProvider } from './contexts/SchemaValidationContext';
import { ToastProvider } from './contexts/ToastContext';
import { api } from './api/client';
import { useMemo, useEffect, useState } from 'react';

// Outer wrapper: StreamingMarkdownProvider MUST wrap the component that calls useSession,
// because useSession needs streaming markdown callbacks from the context.
function SessionWrapper({ projectId, sessionId }: Props) {
  return (
    <ToastProvider>
      <SchemaValidationProvider>
        <StreamingMarkdownProvider>
          <SessionInner projectId={projectId} sessionId={sessionId} />
        </StreamingMarkdownProvider>
      </SchemaValidationProvider>
    </ToastProvider>
  );
}

// Inner wrapper: calls useSession with streaming callbacks, then sets up remaining providers.
function SessionInner({ projectId, sessionId }: Props) {
  // Get streaming markdown callbacks from context — these are passed to useSession
  // so it can dispatch augment/pending events to TextBlock components.
  // The context returns dispatchAugment/dispatchPending/etc. which must be mapped
  // to the StreamingMarkdownCallbacks interface (onAugment/onPending/etc.).
  const streamingMarkdownContext = useStreamingMarkdownContext();
  const streamingCallbacks = useMemo<StreamingMarkdownCallbacks | undefined>(() => {
    if (!streamingMarkdownContext) return undefined;
    return {
      onAugment: streamingMarkdownContext.dispatchAugment,
      onPending: streamingMarkdownContext.dispatchPending,
      onStreamEnd: streamingMarkdownContext.dispatchStreamEnd,
      setCurrentMessageId: streamingMarkdownContext.setCurrentMessageId,
      captureHtml: streamingMarkdownContext.captureStreamingHtml,
    };
  }, [streamingMarkdownContext]);

  // useSession is called HERE (not in MySessionView) so we can pass
  // agentContent to providers. Pass the entire return as a prop.
  const sessionState = useSession(projectId, sessionId, undefined, streamingCallbacks);
  const { agentContent, setAgentContent, toolUseToAgent } = sessionState;

  // projectPath comes from the Project API, NOT from session.
  // The Session type does not have projectPath.
  const [projectPath, setProjectPath] = useState<string | null>(null);
  useEffect(() => {
    api.getProject(projectId).then(data => setProjectPath(data.project.path));
  }, [projectId]);

  return (
    <SessionMetadataProvider
      projectId={projectId}
      projectPath={projectPath}
      sessionId={sessionId}
    >
      <AgentContentProvider
        agentContent={agentContent}
        setAgentContent={setAgentContent}
        toolUseToAgent={toolUseToAgent}
        projectId={projectId}
        sessionId={sessionId}
      >
        <MySessionView
          projectId={projectId}
          sessionId={sessionId}
          sessionState={sessionState}
        />
      </AgentContentProvider>
    </SessionMetadataProvider>
  );
}
```

> **Why five providers?** `SchemaValidationContext` calls `useToastContext()` internally, so
> `ToastProvider` must be an ancestor. Every tool renderer calls `useSchemaValidationContext()`
> to validate tool results. `TaskRenderer` additionally uses `AgentContentContext` (which needs
> `projectId`/`sessionId` for subagent API calls) and `SessionMetadataContext` for project paths.
> `StreamingMarkdownProvider` must wrap the component that calls `useSession` — the hook needs
> its callbacks to dispatch streaming HTML events to `TextBlock` components.
> Without these, the app works for text-only sessions but **crashes the moment a tool call
> renders** — which is virtually every real session.

**Step 4: Import styles**

```tsx
import './styles/index.css';  // Includes renderers.css and tool-rows.css via @import
```

### 12.3 Session List Integration

```tsx
import { useGlobalSessions } from './hooks/useGlobalSessions';
import { SessionListItem } from './components/SessionListItem';

function MySessionList() {
  const { sessions, loading, hasMore, loadMore } = useGlobalSessions({
    limit: 25,
    includeStats: true,
  });

  return (
    <div>
      {sessions.map(s => (
        <SessionListItem
          key={s.id}
          sessionId={s.id}
          projectId={s.projectId}
          title={s.title}
          mode="card"
          activity={s.activity}
          pendingInputType={s.pendingInputType}
          hasUnread={s.hasUnread}
          provider={s.provider}
          showProjectName
          showTimestamp
          basePath=""  // Your app's base path
        />
      ))}
      {hasMore && <button onClick={loadMore}>Load More</button>}
    </div>
  );
}
```

### 12.4 Connection Setup

**Local access (same network):**
```typescript
// API client defaults to window.location.origin
// If your app is on a different origin, configure a proxy:
// vite.config.ts:
export default {
  server: {
    proxy: {
      '/api': 'http://your-ya-server:3400',
    }
  }
}
```

**Remote access (via relay):**
```typescript
// ⚠️ Import SecureConnection DIRECTLY — connection/index.ts uses lazy loading
// to avoid eagerly bundling tssrp6a (heavy SRP crypto library) in local mode.
import { SecureConnection } from './lib/connection/SecureConnection';
import { setGlobalConnection } from './lib/connection';

const conn = new SecureConnection(wsUrl, username, password);
setGlobalConnection(conn);
// Connection + SRP authentication happen lazily on first api.* call or subscription.
// No explicit connect call needed — SecureConnection handles this internally.
```

### 12.5 CSS Custom Properties (Theming)

The styles rely on CSS custom properties. Define these in your app's root:

```css
:root {
  --bg-surface: #1a1a1a;
  --bg-input: #2a2a2a;
  --bg-code: #0d1117;
  --bg-user-message: #1e3a5f;
  --text-primary: #e0e0e0;
  --text-muted: #888;
  --border-color: #333;
  --border-input: #444;
  --font-sans: system-ui, -apple-system, sans-serif;
  --font-mono: 'SF Mono', 'Fira Code', monospace;
  --radius-sm: 4px;
  --radius-md: 8px;
  --radius-lg: 12px;
  --primary-color: #4a9eff;
  --success-color: #4caf50;
  --warning-color: #ff9800;
  --error-color: #f44336;
}
```

### 12.6 Responsive Breakpoints

The CSS uses these breakpoints:
- **Desktop:** >1099px — Sidebar visible, wider layout
- **Tablet:** 600px–1099px — Sidebar overlays, adjusted padding
- **Mobile:** <600px — Compact everything, smaller fonts

---

## 13. API Reference {#13-api-reference}

### 13.1 Session Lifecycle APIs

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/projects/{projectId}/sessions` | Start new session with initial message |
| POST | `/api/projects/{projectId}/sessions/create` | Create session without message |
| POST | `/api/projects/{projectId}/sessions/{sessionId}/resume` | Resume existing session |
| POST | `/api/sessions/{sessionId}/messages` | Queue message to running session |
| DELETE | `/api/sessions/{sessionId}/deferred/{tempId}` | Cancel queued message |

### 13.2 Session Data APIs

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/projects/{projectId}/sessions/{sessionId}` | Full session with messages |
| GET | `/api/projects/{projectId}/sessions/{sessionId}/metadata` | Metadata only |
| GET | `/api/projects/{projectId}/sessions/{sessionId}/agents/{agentId}` | Subagent content |
| GET | `/api/projects/{projectId}/sessions/{sessionId}/agents` | Agent ID mappings |

### 13.3 Session Control APIs

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/sessions/{sessionId}/input` | Respond to tool approval / question |
| PUT | `/api/sessions/{sessionId}/mode` | Change permission mode |
| PUT | `/api/sessions/{sessionId}/hold` | Hold/resume agent |
| POST | `/api/processes/{processId}/abort` | Abort running process |
| POST | `/api/processes/{processId}/interrupt` | Interrupt (Ctrl+C) |

### 13.4 Session Metadata APIs

| Method | Path | Purpose |
|--------|------|---------|
| PUT | `/api/projects/{projectId}/sessions/{sessionId}/metadata` | Update title/star/archive |
| POST | `/api/sessions/{sessionId}/mark-seen` | Mark as read |
| DELETE | `/api/sessions/{sessionId}/mark-seen` | Mark as unread |

### 13.5 List & Discovery APIs

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/sessions` | Global session list (filtered, paginated) |
| GET | `/api/sessions/stats` | Session counts |
| GET | `/api/inbox` | Priority-tiered inbox |
| GET | `/api/recents` | Recently visited |
| GET | `/api/projects` | All projects |
| GET | `/api/providers` | Available AI providers |

### 13.6 WebSocket API

**Endpoint:** `ws[s]://{host}/api/ws`

**Subscribe:**
```json
{ "type": "subscribe", "subscriptionId": "uuid", "channel": "session", "sessionId": "..." }
```

**Unsubscribe:**
```json
{ "type": "unsubscribe", "subscriptionId": "uuid" }
```

**HTTP-over-WS Request:**
```json
{ "type": "request", "id": "uuid", "method": "GET", "path": "/api/sessions/..." }
```

**HTTP-over-WS Response:**
```json
{ "type": "response", "id": "uuid", "status": 200, "body": { ... } }
```

**Event:**
```json
{ "type": "event", "subscriptionId": "uuid", "eventType": "message", "data": { ... } }
```
