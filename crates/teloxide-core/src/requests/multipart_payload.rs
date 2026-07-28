use crate::{
    payloads,
    requests::Payload,
    types::{InputFile, InputFileLike, InputMedia, InputPaidMedia, InputSticker},
};

/// Payloads that need to be sent as `multipart/form-data` because they contain
/// files inside.
pub trait MultipartPayload: Payload {
    fn copy_files(&self, into: &mut dyn FnMut(InputFile));

    fn move_files(&mut self, into: &mut dyn FnMut(InputFile));
}

impl MultipartPayload for payloads::SendPaidMedia {
    fn copy_files(&self, into: &mut dyn FnMut(InputFile)) {
        self.media.iter().flat_map(InputPaidMedia::files).for_each(|f| f.copy_into(into))
    }

    fn move_files(&mut self, into: &mut dyn FnMut(InputFile)) {
        self.media.iter_mut().flat_map(InputPaidMedia::files_mut).for_each(|f| f.move_into(into))
    }
}

impl MultipartPayload for payloads::SendMediaGroup {
    fn copy_files(&self, into: &mut dyn FnMut(InputFile)) {
        self.media.iter().flat_map(InputMedia::files).for_each(|f| f.copy_into(into))
    }

    fn move_files(&mut self, into: &mut dyn FnMut(InputFile)) {
        self.media.iter_mut().flat_map(InputMedia::files_mut).for_each(|f| f.move_into(into))
    }
}

impl MultipartPayload for payloads::EditMessageMedia {
    fn copy_files(&self, into: &mut dyn FnMut(InputFile)) {
        self.media.files().for_each(|f| f.copy_into(into))
    }

    fn move_files(&mut self, into: &mut dyn FnMut(InputFile)) {
        self.media.files_mut().for_each(|f| f.move_into(into))
    }
}

impl MultipartPayload for payloads::EditMessageMediaInline {
    fn copy_files(&self, into: &mut dyn FnMut(InputFile)) {
        self.media.files().for_each(|f| f.copy_into(into))
    }

    fn move_files(&mut self, into: &mut dyn FnMut(InputFile)) {
        self.media.files_mut().for_each(|f| f.move_into(into))
    }
}

impl MultipartPayload for payloads::CreateNewStickerSet {
    fn copy_files(&self, into: &mut dyn FnMut(InputFile)) {
        self.stickers
            .iter()
            .for_each(|InputSticker { sticker: f, .. }: &InputSticker| f.copy_into(into))
    }

    fn move_files(&mut self, into: &mut dyn FnMut(InputFile)) {
        self.stickers
            .iter_mut()
            .for_each(|InputSticker { sticker: f, .. }: &mut InputSticker| f.move_into(into))
    }
}

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
        InputFile::memory(vec![1_u8])
    }

    fn populated_poll() -> SendPoll {
        let options = vec![
            InputPollOption::new("sticker").media(InputPollOptionMedia::Sticker(
                InputMediaSticker { media: file(), emoji: None },
            )),
            InputPollOption::new("live")
                .media(InputPollOptionMedia::LivePhoto(InputMediaLivePhoto::new(file(), file()))),
        ];

        let mut payload = SendPoll::new(ChatId(1), "question", options);
        payload.media = Some(InputPollMedia::Video(
            InputMediaVideo::new(file()).thumbnail(file()).cover(file()),
        ));
        payload.explanation_media =
            Some(InputPollMedia::Animation(InputMediaAnimation::new(file()).thumbnail(file())));
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
