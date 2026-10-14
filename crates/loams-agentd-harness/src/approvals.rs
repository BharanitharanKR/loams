//! Supervised approvals, the drivers' shared half (design §53 §8.1; plan
//! SD1 Task 13): how risky a tool call looks, and how to wait for the
//! person's decision. Each driver's own `approvals.rs` maps its agent's
//! permission request to an [`ApprovalAsk`] and the decision back to the
//! agent's answer.

use loams_agentd_proto::approvals::ToolRisk;
use serde_json::Value;

/// Shell fragments that remove or rewrite what may not come back. Matched on
/// the command with runs of whitespace folded to one space.
const DESTRUCTIVE: &[&str] = &[
    "rm -r",
    "rm -f",
    "rm -rf",
    "rm -fr",
    "rm --recursive",
    "rm --force",
    "rmdir ",
    "git push -f",
    "git push --force",
    "git push --delete",
    "git push origin --delete",
    "git push origin :",
    "git reset --hard",
    "git clean -",
    "git checkout -- ",
    "git checkout .",
    "git restore ",
    "git branch -d",
    "git branch -D",
    "git branch --delete",
    "git rebase",
    "git filter-branch",
    "git filter-repo",
    "git worktree remove",
    "git stash drop",
    "git stash clear",
    "git tag -d",
    "find . -delete",
    " -delete",
    "truncate ",
    "shred ",
    "mkfs",
    "dd if=",
    "drop table",
    "drop database",
    "drop schema",
    "truncate table",
    "delete from",
    "kubectl delete",
    "docker rm",
    "docker system prune",
    "terraform destroy",
];

/// How risky running `command` looks: [`ToolRisk::Destructive`] when it
/// contains one of the fragments above, else [`ToolRisk::Write`].
pub fn command_risk(command: &str) -> ToolRisk {
    let folded = command.split_whitespace().collect::<Vec<_>>().join(" ");
    let lower = folded.to_ascii_lowercase();
    let destructive = DESTRUCTIVE.iter().any(|fragment| {
        // `git branch -D` is case-sensitive (`-d` is the safe delete).
        if fragment.chars().any(|c| c.is_ascii_uppercase()) {
            folded.contains(fragment)
        } else {
            lower.contains(fragment)
        }
    });
    if destructive {
        ToolRisk::Destructive
    } else {
        ToolRisk::Write
    }
}

/// A command found in a tool's arguments: `command` (a string or an argv
/// array), `cmd`, or `metadata.command`.
pub fn command_of(args: &Value) -> Option<String> {
    let field = args
        .get("command")
        .or_else(|| args.get("cmd"))
        .or_else(|| args.pointer("/metadata/command"))
        .or_else(|| args.pointer("/rawInput/command"))?;
    match field {
        Value::String(s) => Some(s.clone()),
        Value::Array(parts) => Some(
            parts
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(" "),
        ),
        _ => None,
    }
}

/// The risk of a tool named in the common vocabulary of the agents (Claude's
/// `Read`/`Edit`/`Bash`, opencode's `read`/`edit`/`bash`, ACP's tool kinds):
/// reads and searches are [`ToolRisk::Read`]; a deletion is
/// [`ToolRisk::Destructive`]; a command is judged by [`command_risk`];
/// anything else (edits, writes, unknown tools) is [`ToolRisk::Write`].
pub fn tool_risk(tool: &str, args: &Value) -> ToolRisk {
    let name = tool.to_ascii_lowercase();
    match name.as_str() {
        "read" | "glob" | "grep" | "ls" | "list" | "search" | "fetch" | "think" | "webfetch"
        | "websearch" | "notebookread" | "todowrite" | "todoread" | "codesearch" => {
            ToolRisk::Read
        }
        "delete" => ToolRisk::Destructive,
        "bash" | "execute" | "exec" | "shell" | "commandexecution" | "local_shell" => {
            command_of(args).map_or(ToolRisk::Write, |command| command_risk(&command))
        }
        _ => match command_of(args) {
            Some(command) => command_risk(&command),
            None => ToolRisk::Write,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn risks() {
        assert_eq!(command_risk("cargo test"), ToolRisk::Write);
        assert_eq!(command_risk("rm  -rf build"), ToolRisk::Destructive);
        assert_eq!(command_risk("git push --force origin main"), ToolRisk::Destructive);
        assert_eq!(command_risk("git branch -d merged"), ToolRisk::Destructive);
        assert_eq!(command_risk("git status"), ToolRisk::Write);
        assert_eq!(tool_risk("Read", &json!({"file_path": "a"})), ToolRisk::Read);
        assert_eq!(tool_risk("Edit", &json!({"file_path": "a"})), ToolRisk::Write);
        assert_eq!(
            tool_risk("Bash", &json!({"command": "git reset --hard"})),
            ToolRisk::Destructive
        );
        assert_eq!(tool_risk("delete", &json!({})), ToolRisk::Destructive);
        assert_eq!(
            tool_risk("commandExecution", &json!({"command": ["rm", "-rf", "x"]})),
            ToolRisk::Destructive
        );
    }
}
