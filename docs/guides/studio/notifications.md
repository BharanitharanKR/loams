# Notifications

`loams-agentd` posts a notification when something in a thread or a run group needs your attention (SD1 Tasks 14 and 28).

## What the daemon posts

| Kind | When |
|---|---|
| `turn_done` | A turn finished |
| `error` | A turn ended in an error |
| `question` | The agent asked you a question |
| `approval` | A tool call in a supervised thread waits for approval (one per request) |
| `group_ready` | A [run group](parallel-runs.md)'s variants have finished |
| `check_failed` | A run group's check exited non-zero |

An interrupted turn posts nothing, since you stopped it, and a subagent's events count as its parent turn's. A thread's notification is titled with the thread's title (at most 160 characters).

The daemon keeps the last 200 notifications in memory and replays them to each client that connects, oldest first. Nothing is stored on disk, so a daemon restart starts empty.

## The bell

Studio's header has a bell on every Studio page. It lists the notifications newest first, each leading to its thread or run group. "Unread" means arrived since this page opened Studio, and closing the list clears it. The daemon keeps no read state, and Studio adds no storage for it, so a reload resets it. The bell is hidden when the daemon doesn't serve notifications (an older daemon).

## On the desktop

Loams Desktop shows OS notifications, from Electron main rather than the page:
- **Only while the window is not focused.**
- **Once per notification**, and none from before the app started watching (the replayed buffer).
- **Clicking one opens** its thread (`/studio/t/<id>`), else its run group (`/studio/g/<id>`), else Studio's home.

**Settings › Notifications** has two choices, mirrored to Electron main:
- **When a turn finishes** covers `turn_done`, `error`, `group_ready` and `check_failed`;
- **When an approval is waiting** covers `approval` and `question`.

Studio sends these choices to Electron main from the Settings page (main can't read the page's storage), so they apply from the first time you open Studio's Settings.

The tray shows up to five threads that are running or waiting for an approval, then **Open Studio**.

**Not built:** notifications while no app is running. The daemon would need a native notification library, which needs its own pin and licence review.
