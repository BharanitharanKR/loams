//! The params and the small replies of the sessions engine's methods (plan
//! DD1 Task 9). The engine parses every call into the type its method-table
//! entry names ([`crate::rpc::METHODS`]), so the Rust and TypeScript sides
//! read one definition.
//!
//! A list reply has a named type here (`ChatList`, `RepoList`, …): a
//! newtype over the `Vec` the engine returns, which serializes as the same
//! JSON array and gives the method table and TypeScript a name to use.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::chat::{QueuedMessage, SessionCommandPayload};
use crate::{
    ChatConfig, CheckoutDiff, FileSearchMatch, HarnessDescriptor, HarnessId, HarnessUpdateStatus,
    Model, ProjectActionDraft, ProjectActionRun, Repo, RepoRef, Session, SidebarPinChange,
    SidebarPreferencesState, SlashCommand, Space, WorkspaceFileSearchMatch, invocation::Skill,
};

/// Default page size of `ListGitHistory`, `SearchGitHistory` and `ResolveGitAvatars`.
pub const GIT_HISTORY_DEFAULT_LIMIT: usize = 100;

fn default_git_history_limit() -> usize {
    GIT_HISTORY_DEFAULT_LIMIT
}

/// No params, or no reply fields: `{}`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct Empty {}

/// `{ ok: true }`: the reply of a call whose only result is that it worked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct OkReply {
    pub ok: bool,
}

impl OkReply {
    pub const OK: Self = Self { ok: true };
}

/// One chat (session), by the fork's `chatId`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ChatIdParams {
    pub chat_id: String,
}

/// One harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct HarnessParams {
    pub harness: HarnessId,
}

// ---------------------------------------------------------------------------
// Harnesses, models, skills and commands
// ---------------------------------------------------------------------------

/// `ListHarnesses`, `InstallHarness` and `SetHarnessEnabled`: the device's catalog.
#[derive(Debug, Clone, Default, Serialize, Deserialize, TS)]
pub struct HarnessCatalog(pub Vec<HarnessDescriptor>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ListModelsParams {
    pub harness: HarnessId,
    /// Skip the catalog cache.
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, TS)]
pub struct ModelList(pub Vec<Model>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SetHarnessEnabledParams {
    pub harness: HarnessId,
    pub enabled: bool,
}

/// `ListSkills` and `ListCommands`: the harness, and the checkout whose
/// project-level skills and commands to include (a chat, a space or a path).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct HarnessCatalogParams {
    pub harness: HarnessId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// `ListSkills`: `null` when the harness has no skills.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, TS)]
pub struct SkillList(pub Option<Vec<Skill>>);

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, TS)]
pub struct SlashCommandList(pub Vec<SlashCommand>);

/// `CheckHarnessUpdates`: one harness, or every harness when absent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct HarnessUpdateParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<HarnessId>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct HarnessUpdateList(pub Vec<HarnessUpdateStatus>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DismissHarnessUpdateParams {
    pub harness: HarnessId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ApplyHarnessUpdateReply {
    pub ok: bool,
    /// The version now installed.
    pub version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CancelledReply {
    pub cancelled: bool,
}

// ---------------------------------------------------------------------------
// Commands and the queue
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QueueCommandParams {
    pub chat_id: String,
    pub command: SessionCommandPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QueueCommandReply {
    pub command_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TakeProjectActionSetupParams {
    pub chat_id: String,
    pub command_id: String,
}

/// `TakeProjectActionSetup`: `ready` once the worktree's setup Action ran;
/// then `setupAction` or `setupError` says how.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ProjectActionSetup {
    pub ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_action: Option<ProjectActionRun>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_error: Option<String>,
}

/// `WatchDocMessages`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WatchDocMessagesParams {
    pub chat_id: String,
    /// Paint the local tail first (`historyPending`), then the full transcript.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub opening_tail: bool,
}

/// `WatchQueue`'s stream item.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QueueSnapshot {
    pub items: Vec<QueuedMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QueueMessageParams {
    pub chat_id: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<String>,
    /// Keep this row visible during the current turn even when the harness
    /// supports mid-turn steering.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub hold_for_turn_end: bool,
}

/// `QueueMessage`'s reply: the new row's id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct IdReply {
    pub id: String,
}

/// One queued row: `RemoveQueuedMessage`, `SendQueuedMessageNow`, `SteerQueuedMessageNow`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QueuedMessageRef {
    pub chat_id: String,
    pub id: String,
}

/// Retype a queued row; empty text deletes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct UpdateQueuedMessageParams {
    pub chat_id: String,
    pub id: String,
    #[serde(default)]
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct MoveQueuedMessageParams {
    pub chat_id: String,
    pub id: String,
    #[serde(default)]
    pub to_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ChangedReply {
    pub changed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RemovedReply {
    pub removed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SentReply {
    pub sent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct BeginQueuedMessageEditParams {
    pub chat_id: String,
    pub id: String,
    pub editor_device_id: String,
    pub editor_instance_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RenewQueuedMessageEditParams {
    pub chat_id: String,
    pub id: String,
    pub lease_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct FinishQueuedMessageEditParams {
    pub chat_id: String,
    pub id: String,
    pub lease_id: String,
    pub action: crate::chat::FinishQueuedMessageEditAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_text_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ForkSideChatParams {
    /// The new chat's id.
    pub chat_id: String,
    pub source_chat_id: String,
    /// Where the fork hangs in the tree. Defaults to the source; a side
    /// chat's own fork button passes the side chat's parent so the copy
    /// lists as a sibling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_chat_id: Option<String>,
}

// ---------------------------------------------------------------------------
// The workspace doc
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, TS)]
pub struct ChatList(pub Vec<crate::Chat>);

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, TS)]
pub struct SpaceList(pub Vec<Space>);

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, TS)]
pub struct SessionList(pub Vec<Session>);

/// `LocalDevice`'s reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct LocalDeviceReply {
    pub device_id: String,
}

/// `EngineReady`'s reply, once the engine is assembled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct EngineReadyReply {
    pub ready: bool,
}

/// The `Mutate` surface, tagged by `op`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "op", rename_all = "camelCase")]
// Built once per user action; boxing `CreateChat` would buy nothing.
#[allow(clippy::large_enum_variant)]
pub enum MutateParams {
    #[serde(rename_all = "camelCase")]
    CreateChat {
        chat_id: String,
        /// The project the chat is created in; it fixes the host device and
        /// the base cwd. `None` mints a project-less chat: `deviceId` picks
        /// the host and the cwd defaults to `~` (expanded on the host at run time).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space_id: Option<String>,
        /// Host device for a project-less chat; ignored when `spaceId` is set.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        device_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        config: Option<ChatConfig>,
        /// The picked ref, named on the row from the first frame.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Cwd override (isolated-worktree path); default = the space's folder.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
        /// The chat whose agent is creating this one (the MCP shim); recorded
        /// on the row as `parentChatId` for orchestration trees.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_chat_id: Option<String>,
    },
    /// Create a space (device + folder pair). Idempotent by id; a live
    /// duplicate `(deviceId, path)` no-ops.
    #[serde(rename_all = "camelCase")]
    CreateSpace {
        space_id: String,
        device_id: String,
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(default)]
        git_detected: bool,
    },
    /// Display-name set; `name: None` clears back to basename(path).
    #[serde(rename_all = "camelCase")]
    RenameSpace {
        space_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
    /// Hard delete: cascades to every chat (and session row) in the space.
    #[serde(rename_all = "camelCase")]
    DeleteSpace { space_id: String },
    #[serde(rename_all = "camelCase")]
    RenameChat { chat_id: String, title: String },
    /// Set the chat's checkout branch label.
    #[serde(rename_all = "camelCase")]
    SetChatBranch { chat_id: String, branch: String },
    /// Retarget a chat onto another existing folder (a worktree).
    #[serde(rename_all = "camelCase")]
    SetChatCwd { chat_id: String, cwd: String },
    /// Backdate a chat's activity timestamps (epoch ms), for tooling and seeds.
    #[serde(rename_all = "camelCase")]
    SetChatActivity {
        chat_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last_message_at: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        created_at: Option<i64>,
    },
    /// Re-home a chat to another device (tooling and seeds).
    #[serde(rename_all = "camelCase")]
    SetChatHost { chat_id: String, device_id: String },
    #[serde(rename_all = "camelCase")]
    SetChatArchived { chat_id: String, archived: bool },
    /// Change one pin without replacing another device's edits.
    #[serde(rename_all = "camelCase")]
    ChangeSidebarPin { change: SidebarPinChange },
    /// Full-config replace on the chat row: the composer's mid-session model,
    /// reasoning and option changes.
    #[serde(rename_all = "camelCase")]
    SetChatConfig { chat_id: String, config: ChatConfig },
    /// Tombstone: removes the chats-map row; the session doc remains.
    #[serde(rename_all = "camelCase")]
    DeleteChat { chat_id: String },
    /// Seen marker: clears the "completed" badge. `at` is epoch ms; default now.
    #[serde(rename_all = "camelCase")]
    MarkChatSeen {
        chat_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        at: Option<i64>,
    },
}

/// `Mutate`'s reply; a pin change also returns the new sidebar preferences.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct MutateReply {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidebar_preferences: Option<SidebarPreferencesState>,
}

// ---------------------------------------------------------------------------
// Repositories, checkouts and diffs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, TS)]
pub struct RepoList(pub Vec<Repo>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct AddRepoParams {
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CloneRepoParams {
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CreateRepoParams {
    pub name: String,
}

/// A repository (or checkout) by path: `ListBranches`, `ListRefs`, `FetchAll`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RepoPathParams {
    /// `repoPath` (the shorthand `repo` is accepted as an alias).
    #[serde(alias = "repo")]
    pub repo_path: String,
}

/// `ListBranches`: local branch names, the default branch first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct BranchList(pub Vec<String>);

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct RepoRefList(pub Vec<RepoRef>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ListGitHistoryParams {
    pub cwd: String,
    #[serde(default)]
    pub cursor: usize,
    #[serde(default = "default_git_history_limit")]
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SearchGitHistoryParams {
    pub cwd: String,
    pub query: String,
    #[serde(default)]
    pub cursor: usize,
    #[serde(default = "default_git_history_limit")]
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct GitAvatarAuthor {
    pub sha: String,
    pub email: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ResolveGitAvatarsParams {
    pub cwd: String,
    pub authors: Vec<GitAvatarAuthor>,
    #[serde(default)]
    pub cursor: usize,
    #[serde(default = "default_git_history_limit")]
    pub limit: usize,
}

/// `ResolveGitAvatars`: base64 image bytes by author email.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct GitAvatars(pub HashMap<String, String>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SwitchRefParams {
    /// The checkout to switch: a session's cwd (main folder or worktree).
    pub repo_path: String,
    pub ref_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SwitchRefReply {
    /// The branch now checked out.
    pub branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ListFoldersParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// `SearchFiles`: fuzzy relative-path search rooted in a chat or space checkout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SearchFilesParams {
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_id: Option<String>,
    /// Existing linked worktree selected for a new chat. The engine accepts it
    /// only after verifying it against the space repository's worktree list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct FileSearchMatchList(pub Vec<FileSearchMatch>);

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct WorkspaceFileSearchMatchList(pub Vec<WorkspaceFileSearchMatch>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CreateWorktreeParams {
    #[serde(alias = "repo")]
    pub repo_path: String,
    pub branch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DeleteWorktreeParams {
    #[serde(alias = "repo")]
    pub repo_path: String,
    #[serde(alias = "path")]
    pub worktree_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WatchCheckoutChangeRequestParams {
    pub cwd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

/// `GetCheckoutDiff`: `branch` diffs the working tree against
/// merge-base(`baseRef`, HEAD), `commit` one commit's own changes, `turn` the
/// chat's turn-start snapshot against the tree, anything else the working tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct GetCheckoutDiffParams {
    pub cwd: String,
    #[serde(default)]
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, TS)]
pub struct CheckoutDiffList(pub Vec<CheckoutDiff>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DiscardWorkingTreeParams {
    pub chat_id: String,
    pub checkout_id: String,
    pub expected_checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DiscardWorkingTreeReply {
    pub ok: bool,
    /// The checkout's diff checksum after the discard.
    pub checksum: String,
}

// ---------------------------------------------------------------------------
// Project actions and terminals
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ListProjectActionsParams {
    pub space_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct UpsertProjectActionParams {
    pub space_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_id: Option<String>,
    pub action: ProjectActionDraft,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DeleteProjectActionParams {
    pub space_id: String,
    pub action_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RunProjectActionParams {
    pub space_id: String,
    pub chat_id: String,
    pub action_id: String,
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct OpenTerminalParams {
    pub chat_id: String,
    pub cols: u16,
    pub rows: u16,
    /// Explicit working directory (the new-chat canvas: the selected project
    /// folder, or `~`). When omitted, the chat row's cwd is used, then the
    /// space named by a `space-canvas:{spaceId}` chat id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TerminalIdParams {
    pub terminal_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeTerminalParams {
    pub terminal_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_seq: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WriteTerminalParams {
    pub terminal_id: String,
    /// Base64 input bytes (plain UTF-8 is accepted too).
    pub data: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ResizeTerminalParams {
    pub terminal_id: String,
    pub cols: u16,
    pub rows: u16,
}

// ---------------------------------------------------------------------------
// Agent accounts, uploads and tool outputs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ListAgentAccountsParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_usage: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct AgentAccountParams {
    pub harness: HarnessId,
    pub account_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct StartAgentLoginParams {
    pub harness: HarnessId,
    /// For agents that keep a login per model provider (OpenCode, Pi,
    /// Hermes): which provider to sign in to; absent = the agent's default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct LoginIdParams {
    pub login_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CompleteAgentLoginParams {
    pub login_id: String,
    pub code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct UploadChunkParams {
    pub upload_id: String,
    /// Base64 payload chunk.
    pub data: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct UploadCommitParams {
    pub upload_id: String,
    pub file_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct UploadCommitReply {
    /// The committed attachment's durable path.
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ReadAttachmentChunkParams {
    pub path: String,
    #[serde(default)]
    pub offset: u64,
}

/// `FetchToolBlob`: one page of a tool result's full output (plan DD1 T2-13).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct FetchToolBlobParams {
    /// Doc-resident sidecar ref (`{chatId}/{partId}` or `{chatId}/{partId}.diff`).
    pub blob_ref: String,
    /// Body offset of the page.
    #[serde(default)]
    pub offset: u64,
    /// Page length; at most (and by default) 256 KiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub len: Option<u64>,
}
