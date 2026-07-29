from pathlib import Path

wrapper = Path(__file__)
script = wrapper.with_name("apply_type_audit.py")
source = script.read_text()
start = source.index("text = text.replace(\n    \"\"\"        let i5 = |x| R(R(x));")
end = source.index("update_tests =", start)
replacement = '''text = text.replace(
    "use std::iter::{empty, once};",
    "use std::iter::once;",
    1,
)
text = text.replace(
    """        let i5 = |x| R(R(x));
""",
    """        let i5 = |users: [Option<&User>; 2]| R(R(users.into_iter().flatten()));
""",
    1,
)
text = text.replace(
    """            UpdateKind::ChatJoinRequest(_)
            | UpdateKind::MessageReactionCount(_)
            | UpdateKind::BusinessConnection(_)
            | UpdateKind::ManagedBot(_)
            | UpdateKind::DeletedBusinessMessages(_)
            | UpdateKind::Error(_) => i5(empty()),
""",
    """            UpdateKind::ChatJoinRequest(request) => i1(once(&request.from)),
            UpdateKind::BusinessConnection(connection) => i1(once(&connection.user)),
            UpdateKind::ManagedBot(update) => i5([Some(&update.user), Some(&update.bot)]),

            UpdateKind::MessageReactionCount(_)
            | UpdateKind::DeletedBusinessMessages(_)
            | UpdateKind::Error(_) => i5([None, None]),
""",
    1,
)
if "[Some(&update.user), Some(&update.bot)]" not in text:
    raise RuntimeError("failed to extend mentioned_users iterator tree")
'''
source = source[:start] + replacement + source[end:]
exec(compile(source, str(script), "exec"), {"__file__": str(script)})
wrapper.unlink()
