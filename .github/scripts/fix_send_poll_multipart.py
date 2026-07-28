from pathlib import Path


def replace_once(path: Path, old: str, new: str, label: str) -> None:
    text = path.read_text()
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one match, got {count}")
    path.write_text(text.replace(old, new, 1))


poll_media_path = Path("crates/teloxide-core/src/types/poll_media.rs")
poll_media = poll_media_path.read_text()
poll_media = poll_media.replace(
    "use serde::{Deserialize, Serialize};\n\nuse crate::types::{",
    "use serde::{Deserialize, Serialize};\n\nuse crate::types::{\n    InputFile,",
    1,
)
poll_media += r'''

impl InputPollMedia {
    pub(crate) fn files(&self) -> impl Iterator<Item = &InputFile> {
        let mut files = Vec::new();

        match self {
            Self::Animation(media) => {
                files.push(&media.media);
                files.extend(media.thumbnail.iter());
            }
            Self::Audio(media) => {
                files.push(&media.media);
                files.extend(media.thumbnail.iter());
            }
            Self::Document(media) => {
                files.push(&media.media);
                files.extend(media.thumbnail.iter());
            }
            Self::LivePhoto(media) => {
                files.push(&media.media);
                files.push(&media.photo);
            }
            Self::Photo(media) => files.push(&media.media),
            Self::Video(media) => {
                files.push(&media.media);
                files.extend(media.thumbnail.iter());
                files.extend(media.cover.iter());
            }
            Self::Location(_) | Self::Venue(_) => {}
        }

        files.into_iter()
    }

    pub(crate) fn files_mut(&mut self) -> impl Iterator<Item = &mut InputFile> {
        let mut files = Vec::new();

        match self {
            Self::Animation(media) => {
                files.push(&mut media.media);
                files.extend(media.thumbnail.iter_mut());
            }
            Self::Audio(media) => {
                files.push(&mut media.media);
                files.extend(media.thumbnail.iter_mut());
            }
            Self::Document(media) => {
                files.push(&mut media.media);
                files.extend(media.thumbnail.iter_mut());
            }
            Self::LivePhoto(media) => {
                files.push(&mut media.media);
                files.push(&mut media.photo);
            }
            Self::Photo(media) => files.push(&mut media.media),
            Self::Video(media) => {
                files.push(&mut media.media);
                files.extend(media.thumbnail.iter_mut());
                files.extend(media.cover.iter_mut());
            }
            Self::Location(_) | Self::Venue(_) => {}
        }

        files.into_iter()
    }
}

impl InputPollOptionMedia {
    pub(crate) fn files(&self) -> impl Iterator<Item = &InputFile> {
        let mut files = Vec::new();

        match self {
            Self::Animation(media) => {
                files.push(&media.media);
                files.extend(media.thumbnail.iter());
            }
            Self::LivePhoto(media) => {
                files.push(&media.media);
                files.push(&media.photo);
            }
            Self::Photo(media) => files.push(&media.media),
            Self::Sticker(media) => files.push(&media.media),
            Self::Video(media) => {
                files.push(&media.media);
                files.extend(media.thumbnail.iter());
                files.extend(media.cover.iter());
            }
            Self::Location(_) | Self::Venue(_) => {}
        }

        files.into_iter()
    }

    pub(crate) fn files_mut(&mut self) -> impl Iterator<Item = &mut InputFile> {
        let mut files = Vec::new();

        match self {
            Self::Animation(media) => {
                files.push(&mut media.media);
                files.extend(media.thumbnail.iter_mut());
            }
            Self::LivePhoto(media) => {
                files.push(&mut media.media);
                files.push(&mut media.photo);
            }
            Self::Photo(media) => files.push(&mut media.media),
            Self::Sticker(media) => files.push(&mut media.media),
            Self::Video(media) => {
                files.push(&mut media.media);
                files.extend(media.thumbnail.iter_mut());
                files.extend(media.cover.iter_mut());
            }
            Self::Location(_) | Self::Venue(_) => {}
        }

        files.into_iter()
    }
}
'''
poll_media_path.write_text(poll_media)

input_poll_option_path = Path("crates/teloxide-core/src/types/input_poll_option.rs")
input_poll_option = input_poll_option_path.read_text()
input_poll_option = input_poll_option.replace("use std::hash::{Hash, Hasher};\n\n", "", 1)
start = input_poll_option.index("impl PartialEq for InputPollOption")
end = input_poll_option.index("#[derive(Clone, Debug)]", start)
input_poll_option = input_poll_option[:start] + input_poll_option[end:]
input_poll_option_path.write_text(input_poll_option)

multipart_path = Path("crates/teloxide-core/src/requests/multipart_payload.rs")
multipart = multipart_path.read_text()
multipart = multipart.replace(
    "types::{InputFile, InputFileLike, InputMedia, InputPaidMedia, InputSticker}",
    "types::{\n        InputFile, InputFileLike, InputMedia, InputPaidMedia, InputPollMedia,\n        InputPollOptionMedia, InputSticker,\n    }",
    1,
)
multipart += r'''

impl MultipartPayload for payloads::SendPoll {
    fn copy_files(&self, into: &mut dyn FnMut(InputFile)) {
        if let Some(media) = &self.media {
            media.files().for_each(|file| file.copy_into(into));
        }
        if let Some(media) = &self.explanation_media {
            media.files().for_each(|file| file.copy_into(into));
        }
        for option in &self.options {
            if let Some(media) = &option.media {
                media.files().for_each(|file| file.copy_into(into));
            }
        }
    }

    fn move_files(&mut self, into: &mut dyn FnMut(InputFile)) {
        if let Some(media) = &mut self.media {
            media.files_mut().for_each(|file| file.move_into(into));
        }
        if let Some(media) = &mut self.explanation_media {
            media.files_mut().for_each(|file| file.move_into(into));
        }
        for option in &mut self.options {
            if let Some(media) = &mut option.media {
                media.files_mut().for_each(|file| file.move_into(into));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        payloads::SendPoll,
        requests::{MultipartPayload, MultipartRequest, Requester},
        types::{
            ChatId, InputFile, InputMediaAnimation, InputMediaLivePhoto, InputMediaSticker,
            InputMediaVideo, InputPollMedia, InputPollOption, InputPollOptionMedia,
        },
        Bot,
    };

    fn file() -> InputFile {
        InputFile::memory([1_u8])
    }

    fn populated_poll() -> SendPoll {
        let options = vec![
            InputPollOption::new("sticker").media(InputPollOptionMedia::Sticker(
                InputMediaSticker { media: file(), emoji: None },
            )),
            InputPollOption::new("live").media(InputPollOptionMedia::LivePhoto(
                InputMediaLivePhoto::new(file(), file()),
            )),
        ];

        let mut payload = SendPoll::new(ChatId(1), "question", options);
        payload.media = Some(InputPollMedia::Video(
            InputMediaVideo::new(file()).thumbnail(file()).cover(file()),
        ));
        payload.explanation_media = Some(InputPollMedia::Animation(
            InputMediaAnimation::new(file()).thumbnail(file()),
        ));
        payload
    }

    #[test]
    fn send_poll_collects_every_nested_attachment() {
        let mut payload = populated_poll();

        let mut copied = 0;
        payload.copy_files(&mut |_| copied += 1);
        assert_eq!(copied, 8);

        let mut moved = 0;
        payload.move_files(&mut |_| moved += 1);
        assert_eq!(moved, 8);
    }

    #[test]
    fn send_poll_uses_multipart_request() {
        fn assert_multipart(_: MultipartRequest<SendPoll>) {}

        let request = Bot::new("token").send_poll(
            ChatId(1),
            "question",
            vec![InputPollOption::new("one"), InputPollOption::new("two")],
        );

        assert_multipart(request);
    }
}
'''
multipart_path.write_text(multipart)

api_path = Path("crates/teloxide-core/src/bot/api.rs")
replace_once(
    api_path,
    "    type SendPoll = JsonRequest<payloads::SendPoll>;",
    "    type SendPoll = MultipartRequest<payloads::SendPoll>;",
    "Bot::SendPoll request transport",
)
