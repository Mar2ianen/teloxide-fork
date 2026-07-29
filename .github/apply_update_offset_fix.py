from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / "crates/teloxide-core"
NIGHTLY = "nightly-2025-06-12"

schema_path = CORE / "schema.ron"
schema = schema_path.read_text()
method_start = schema.index('names: ("getUpdates", "GetUpdates", "get_updates")')
offset_start = schema.index('name: "offset",', method_start)
ty_start = schema.index('ty: Option(i32),', offset_start)
next_method = schema.index('\n        Method(', method_start + 1)
if ty_start > next_method:
    raise RuntimeError("getUpdates offset type not found in method")
schema = schema[:ty_start] + 'ty: Option(i64),' + schema[ty_start + len('ty: Option(i32),'):]
schema_path.write_text(schema)

update_path = CORE / "src/types/update.rs"
update = update_path.read_text()
old = '''    /// Returns the offset for the **next** update that can be used for polling.
    ///
    /// I.e. `self.0 + 1`.
    #[must_use]
    pub fn as_offset(self) -> i32 {
        debug_assert!(self.0 < i32::MAX as u32);

        self.0 as i32 + 1
    }
'''
new = '''    /// Returns the offset for the **next** update that can be used for polling.
    ///
    /// I.e. `self.0 + 1`, widened to [`i64`] so every valid [`UpdateId`] can be
    /// advanced without integer overflow.
    #[must_use]
    pub fn as_offset(self) -> i64 {
        i64::from(self.0) + 1
    }
'''
if update.count(old) != 1:
    raise RuntimeError("UpdateId::as_offset implementation changed")
update = update.replace(old, new, 1)
test = '''

    #[test]
    fn update_offset_does_not_overflow() {
        assert_eq!(UpdateId(0).as_offset(), 1);
        assert_eq!(UpdateId(i32::MAX as u32).as_offset(), i64::from(i32::MAX) + 1);
        assert_eq!(UpdateId(u32::MAX).as_offset(), i64::from(u32::MAX) + 1);
    }
'''
head, tail = update.rsplit("\n}", 1)
update_path.write_text(head + test + "\n}" + tail)

polling_path = ROOT / "crates/teloxide/src/update_listeners/polling.rs"
polling = polling_path.read_text()
old_polling = "    /// Offset parameter  for normal `get_updates()` calls.\n    offset: i32,"
if polling.count(old_polling) != 1:
    raise RuntimeError("PollingStream offset field changed")
polling_path.write_text(
    polling.replace(
        old_polling,
        "    /// Offset parameter for normal `get_updates()` calls.\n    offset: i64,",
        1,
    )
)

integration_dir = CORE / "tests"
integration_dir.mkdir(exist_ok=True)
(integration_dir / "update_id_offset.rs").write_text('''use teloxide_core::{
    payloads::{GetUpdates, GetUpdatesSetters},
    types::UpdateId,
};

#[test]
fn maximum_update_id_serializes_as_a_positive_next_offset() {
    let payload = GetUpdates::new().offset(UpdateId(u32::MAX).as_offset());
    let value = serde_json::to_value(payload).unwrap();
    assert_eq!(value, serde_json::json!({ "offset": 4_294_967_296_i64 }));
}

#[test]
fn widened_offset_remains_signed() {
    let payload = GetUpdates::new().offset(-1_i64);
    let value = serde_json::to_value(payload).unwrap();
    assert_eq!(value, serde_json::json!({ "offset": -1 }));
}
''')

for require_clean in (False, True):
    result = subprocess.run(
        ["cargo", f"+{NIGHTLY}", "test", "-p", "teloxide-core", "--lib", "--features", "full nightly", "codegen", "--", "--nocapture"],
        cwd=ROOT,
    )
    if require_clean and result.returncode != 0:
        raise RuntimeError("second codegen pass was not clean")

subprocess.run(
    ["cargo", f"+{NIGHTLY}", "test", "-p", "teloxide-core", "--features", "full nightly", "update_offset", "--", "--nocapture"],
    cwd=ROOT,
    check=True,
)
subprocess.run(
    ["cargo", f"+{NIGHTLY}", "check", "-p", "teloxide", "--features", "full nightly"],
    cwd=ROOT,
    check=True,
)

subprocess.run(["git", "fetch", "origin", "next"], cwd=ROOT, check=True)
workflow = subprocess.run(
    ["git", "show", "origin/next:.github/workflows/ci.yml"],
    cwd=ROOT,
    check=True,
    capture_output=True,
    text=True,
).stdout
(ROOT / ".github/workflows/ci.yml").write_text(workflow)
Path(__file__).unlink()
