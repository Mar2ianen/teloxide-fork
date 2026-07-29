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
    """        fn direct_users(users: [Option<&User>; 2]) -> impl Iterator<Item = &User> {
            users.into_iter().flatten()
        }

        let i5 = |x| R(R(x));
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
            UpdateKind::ManagedBot(update) => {
                i5(direct_users([Some(&update.user), Some(&update.bot)]))
            }

            UpdateKind::MessageReactionCount(_)
            | UpdateKind::DeletedBusinessMessages(_)
            | UpdateKind::Error(_) => i5(direct_users([None, None])),
""",
    1,
)
remaining_empty_leaves = text.count("i5(empty())")
if remaining_empty_leaves != 4:
    raise RuntimeError(f"expected four empty iterator leaves, found {remaining_empty_leaves}")
text = text.replace("i5(empty())", "i5(direct_users([None, None]))")
if "direct_users([Some(&update.user), Some(&update.bot)])" not in text:
    raise RuntimeError("failed to extend mentioned_users iterator tree")
'''
source = source[:start] + replacement + source[end:]
source = source.replace(
    '    assert_eq!(error, ParseError::WrongBotName("other_bot".to_owned()));',
    '''    match error {
        ParseError::WrongBotName(name) => assert_eq!(name, "other_bot"),
        other => panic!("expected WrongBotName, got {other:?}"),
    }''',
)
exec(compile(source, str(script), "exec"), {"__file__": str(script)})
wrapper.unlink()
