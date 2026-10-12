# Third-party notices

This file began as Zeron's third-party notice. That notice covered components of Zeron's GUI and editor:
- the tree-sitter grammars and queries;
- the gpui-component foundations, mermaid-rs-renderer and Ropey;
- the Symbols icons and the theme palette adaptations;
- the Wry browser host and the GPUI overlay code;
- the optional dictation model and its runtime.

`loams-agentd` is headless (D782). The crates that carried those components were deleted in DD1 Task 1, and `cargo tree -p loams-agentd --target all` contains none of them. Their entries were therefore removed in DD1 Task 6. The text as imported is at `git show 6e470a96^:apps/desktop/native/THIRD_PARTY_NOTICES.md`.

Today `loams-agentd` bundles no third-party file, icon, font, theme or model. Its third-party code is the Rust crates pinned in the root `Cargo.lock`. Each crate keeps its own licence, and `cargo deny check licenses` (`deny.toml`) checks them. [`NOTICE`](NOTICE) names the components the Loams Desktop fork added.

If a bundled third-party asset is added to the daemon, list it here with its licence and source.
