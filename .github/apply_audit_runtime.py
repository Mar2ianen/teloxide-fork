from __future__ import annotations

from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: str, old: str, new: str) -> None:
    file = ROOT / path
    text = file.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise RuntimeError(f"expected exactly one match in {path}, found {text.count(old)}")
    file.write_text(text.replace(old, new, 1), encoding="utf-8")


# Nested story uploads must expose their InputFile to multipart traversal.
replace_once(
    "crates/teloxide-core/src/types/input_story_content.rs",
    """pub enum InputStoryContent {
    Photo(InputStoryContentPhoto),
    Video(InputStoryContentVideo),
}
""",
    """pub enum InputStoryContent {
    Photo(InputStoryContentPhoto),
    Video(InputStoryContentVideo),
}

impl InputStoryContent {
    /// Returns the file contained in this story content.
    pub(crate) fn files(&self) -> impl Iterator<Item = &InputFile> {
        std::iter::once(match self {
            Self::Photo(content) => &content.photo,
            Self::Video(content) => &content.video,
        })
    }

    /// Returns the mutable file contained in this story content.
    pub(crate) fn files_mut(&mut self) -> impl Iterator<Item = &mut InputFile> {
        std::iter::once(match self {
            Self::Photo(content) => &mut content.photo,
            Self::Video(content) => &mut content.video,
        })
    }
}
""",
)

# PostStory and EditStory must be sent through MultipartRequest.
replace_once(
    "crates/teloxide-core/src/bot/api.rs",
    "type PostStory = JsonRequest<payloads::PostStory>;",
    "type PostStory = MultipartRequest<payloads::PostStory>;",
)
replace_once(
    "crates/teloxide-core/src/bot/api.rs",
    "type EditStory = JsonRequest<payloads::EditStory>;",
    "type EditStory = MultipartRequest<payloads::EditStory>;",
)

multipart_path = ROOT / "crates/teloxide-core/src/requests/multipart_payload.rs"
multipart = multipart_path.read_text(encoding="utf-8")
multipart = multipart.replace(
    "types::{InputFile, InputFileLike, InputMedia, InputPaidMedia, InputSticker},",
    "types::{\n        InputFile, InputFileLike, InputMedia, InputPaidMedia, InputSticker, InputStoryContent,\n    },",
    1,
)
insert = """
impl MultipartPayload for payloads::PostStory {
    fn copy_files(&self, into: &mut dyn FnMut(InputFile)) {
        self.content.files().for_each(|file| file.copy_into(into));
    }

    fn move_files(&mut self, into: &mut dyn FnMut(InputFile)) {
        self.content.files_mut().for_each(|file| file.move_into(into));
    }
}

impl MultipartPayload for payloads::EditStory {
    fn copy_files(&self, into: &mut dyn FnMut(InputFile)) {
        self.content.files().for_each(|file| file.copy_into(into));
    }

    fn move_files(&mut self, into: &mut dyn FnMut(InputFile)) {
        self.content.files_mut().for_each(|file| file.move_into(into));
    }
}

"""
marker = "#[cfg(test)]\nmod tests {"
if multipart.count(marker) != 1:
    raise RuntimeError("multipart tests marker changed")
multipart = multipart.replace(marker, insert + marker, 1)
multipart = multipart.replace(
    "payloads::SendPoll,",
    "payloads::{EditStory, PostStory, SendPoll},",
    1,
)
multipart = multipart.replace(
    "ChatId, InputFile, InputMediaAnimation, InputMediaLivePhoto, InputMediaSticker,\n            InputMediaVideo, InputPollMedia, InputPollOption, InputPollOptionMedia,",
    "BusinessConnectionId, ChatId, InputFile, InputMediaAnimation, InputMediaLivePhoto,\n            InputMediaSticker, InputMediaVideo, InputPollMedia, InputPollOption,\n            InputPollOptionMedia, InputStoryContent, InputStoryContentPhoto,\n            InputStoryContentVideo, Seconds, StoryId,",
    1,
)
extra_tests = """

    #[test]
    fn story_payloads_collect_their_nested_attachment() {
        let mut post = PostStory::new(
            BusinessConnectionId("business".to_owned()),
            InputStoryContent::Photo(InputStoryContentPhoto { photo: file() }),
            Seconds::from_seconds(6 * 3600),
        );
        let mut copied = 0;
        post.copy_files(&mut |_| copied += 1);
        assert_eq!(copied, 1);
        let mut moved = 0;
        post.move_files(&mut |_| moved += 1);
        assert_eq!(moved, 1);

        let mut edit = EditStory::new(
            BusinessConnectionId("business".to_owned()),
            StoryId(1),
            InputStoryContent::Video(InputStoryContentVideo {
                video: file(),
                duration: None,
                cover_frame_timestamp: None,
                is_animation: None,
            }),
        );
        let mut copied = 0;
        edit.copy_files(&mut |_| copied += 1);
        assert_eq!(copied, 1);
        let mut moved = 0;
        edit.move_files(&mut |_| moved += 1);
        assert_eq!(moved, 1);
    }

    #[test]
    fn story_methods_use_multipart_requests() {
        fn assert_post(_: MultipartRequest<PostStory>) {}
        fn assert_edit(_: MultipartRequest<EditStory>) {}

        let bot = Bot::new("token");
        assert_post(bot.post_story(
            BusinessConnectionId("business".to_owned()),
            InputStoryContent::Photo(InputStoryContentPhoto { photo: file() }),
            Seconds::from_seconds(6 * 3600),
        ));
        assert_edit(bot.edit_story(
            BusinessConnectionId("business".to_owned()),
            StoryId(1),
            InputStoryContent::Video(InputStoryContentVideo {
                video: file(),
                duration: None,
                cover_frame_timestamp: None,
                is_animation: None,
            }),
        ));
    }
"""
head, tail = multipart.rsplit("\n}", 1)
multipart_path.write_text(head + extra_tests + "\n}" + tail, encoding="utf-8")

# Malformed external JSON must return a serde error instead of panicking.
replace_once(
    "crates/teloxide-core/src/types/poll_answer.rs",
    "use serde::{Deserialize, Deserializer, Serialize};",
    "use serde::{de::Error as _, Deserialize, Deserializer, Serialize};",
)
replace_once(
    "crates/teloxide-core/src/types/poll_answer.rs",
    """    Ok(voter_chat.map(MaybeAnonymousUser::Chat).or(user.map(MaybeAnonymousUser::User)).unwrap())
""",
    """    voter_chat
        .map(MaybeAnonymousUser::Chat)
        .or_else(|| user.map(MaybeAnonymousUser::User))
        .ok_or_else(|| D::Error::custom("poll answer has neither `voter_chat` nor `user`"))
""",
)
poll_path = ROOT / "crates/teloxide-core/src/types/poll_answer.rs"
poll = poll_path.read_text(encoding="utf-8")
poll_test = """

    #[test]
    fn poll_answer_without_voter_is_rejected() {
        let json = r#"{
            "poll_id": "POLL_ID",
            "option_ids": []
        }"#;

        let error = serde_json::from_str::<PollAnswer>(json).unwrap_err();
        assert!(error.to_string().contains("neither `voter_chat` nor `user`"));
    }
"""
head, tail = poll.rsplit("\n}", 1)
poll_path.write_text(head + poll_test + "\n}" + tail, encoding="utf-8")

# Wire-level coverage: attach:// IDs must match actual multipart file parts.
integration_path = ROOT / "crates/teloxide-core/src/serde_multipart/integration_tests.rs"
integration = integration_path.read_text(encoding="utf-8")
integration = integration.replace(
    "payloads::{SendPaidMedia, SendPoll},",
    "payloads::{EditStory, PostStory, SendPaidMedia, SendPoll},",
    1,
)
integration = integration.replace(
    "ChatId, InputFile, InputMediaAnimation, InputMediaLivePhoto, InputMediaSticker,\n        InputMediaVideo, InputPaidMedia, InputPaidMediaVideo, InputPollMedia, InputPollOption,\n        InputPollOptionMedia,",
    "BusinessConnectionId, ChatId, InputFile, InputMediaAnimation, InputMediaLivePhoto,\n        InputMediaSticker, InputMediaVideo, InputPaidMedia, InputPaidMediaVideo, InputPollMedia,\n        InputPollOption, InputPollOptionMedia, InputStoryContent, InputStoryContentPhoto,\n        InputStoryContentVideo, Seconds, StoryId,",
    1,
)
integration_extra = """

#[tokio::test]
async fn post_story_attach_id_matches_multipart_file_part() {
    let payload = PostStory::new(
        BusinessConnectionId("business".to_owned()),
        InputStoryContent::Photo(InputStoryContentPhoto {
            photo: file(b"story-photo", "story-photo.bin"),
        }),
        Seconds::from_seconds(6 * 3600),
    );
    let form = to_form_ref(&payload).unwrap().await;
    let mut request =
        Client::new().post("http://localhost.invalid").multipart(form).build().unwrap();
    let boundary = request.headers()[CONTENT_TYPE]
        .to_str()
        .unwrap()
        .split("boundary=")
        .nth(1)
        .expect("multipart boundary")
        .to_owned();
    let body = request.body_mut().take().unwrap().collect().await.unwrap().to_bytes();
    let parts = multipart_parts(&body, &boundary);

    let mut attach_ids = Vec::new();
    collect_attach_ids(
        &serde_json::from_slice(parts.get("content").expect("missing content part")).unwrap(),
        &mut attach_ids,
    );
    let file_part_ids: BTreeSet<_> = parts
        .keys()
        .filter(|name| !matches!(name.as_str(), "business_connection_id" | "content" | "active_period"))
        .cloned()
        .collect();

    assert_eq!(attach_ids.len(), 1);
    assert_eq!(attach_ids.into_iter().collect::<BTreeSet<_>>(), file_part_ids);
}

#[tokio::test]
async fn edit_story_attach_id_matches_multipart_file_part() {
    let payload = EditStory::new(
        BusinessConnectionId("business".to_owned()),
        StoryId(1),
        InputStoryContent::Video(InputStoryContentVideo {
            video: file(b"story-video", "story-video.bin"),
            duration: None,
            cover_frame_timestamp: None,
            is_animation: None,
        }),
    );
    let form = to_form_ref(&payload).unwrap().await;
    let mut request =
        Client::new().post("http://localhost.invalid").multipart(form).build().unwrap();
    let boundary = request.headers()[CONTENT_TYPE]
        .to_str()
        .unwrap()
        .split("boundary=")
        .nth(1)
        .expect("multipart boundary")
        .to_owned();
    let body = request.body_mut().take().unwrap().collect().await.unwrap().to_bytes();
    let parts = multipart_parts(&body, &boundary);

    let mut attach_ids = Vec::new();
    collect_attach_ids(
        &serde_json::from_slice(parts.get("content").expect("missing content part")).unwrap(),
        &mut attach_ids,
    );
    let file_part_ids: BTreeSet<_> = parts
        .keys()
        .filter(|name| !matches!(name.as_str(), "business_connection_id" | "story_id" | "content"))
        .cloned()
        .collect();

    assert_eq!(attach_ids.len(), 1);
    assert_eq!(attach_ids.into_iter().collect::<BTreeSet<_>>(), file_part_ids);
}
"""
integration_path.write_text(integration + integration_extra, encoding="utf-8")

# Remove the one-shot machinery from the resulting commit.
subprocess.run(["git", "fetch", "origin", "next"], cwd=ROOT, check=True)
workflow = subprocess.run(
    ["git", "show", "origin/next:.github/workflows/ci.yml"],
    cwd=ROOT,
    check=True,
    capture_output=True,
    text=True,
).stdout
(ROOT / ".github/workflows/ci.yml").write_text(workflow, encoding="utf-8")
Path(__file__).unlink()
