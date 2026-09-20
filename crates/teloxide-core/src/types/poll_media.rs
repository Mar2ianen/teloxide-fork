use serde::{Deserialize, Serialize};

use crate::types::{
    Animation, Audio, Document, InputFile, InputFileLike, LivePhoto, Location, PhotoSize, Sticker,
    Venue, Video,
};

/// This object describes media attached to a poll description, quiz
/// explanation, or poll option.
///
/// [The official docs](https://core.telegram.org/bots/api#pollmedia).
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct PollMedia {
    pub animation: Option<Animation>,
    pub audio: Option<Audio>,
    pub document: Option<Document>,
    pub live_photo: Option<LivePhoto>,
    pub location: Option<Location>,
    pub photo: Option<Vec<PhotoSize>>,
    pub sticker: Option<Sticker>,
    pub venue: Option<Venue>,
    pub video: Option<Video>,
}

/// Content of a poll description or quiz explanation to be sent.
///
/// [The official docs](https://core.telegram.org/bots/api#inputpollmedia).
#[derive(Clone, Debug)]
#[derive(Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum InputPollMedia {
    Animation(crate::types::InputMediaAnimation),
    Audio(crate::types::InputMediaAudio),
    Document(crate::types::InputMediaDocument),
    LivePhoto(crate::types::InputMediaLivePhoto),
    Location(crate::types::InputMediaLocation),
    Photo(crate::types::InputMediaPhoto),
    Venue(crate::types::InputMediaVenue),
    Video(crate::types::InputMediaVideo),
}

/// Content of a poll option to be sent.
///
/// [The official docs](https://core.telegram.org/bots/api#inputpolloptionmedia).
#[derive(Clone, Debug)]
#[derive(Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum InputPollOptionMedia {
    Animation(crate::types::InputMediaAnimation),
    LivePhoto(crate::types::InputMediaLivePhoto),
    Location(crate::types::InputMediaLocation),
    Photo(crate::types::InputMediaPhoto),
    Sticker(crate::types::InputMediaSticker),
    Venue(crate::types::InputMediaVenue),
    Video(crate::types::InputMediaVideo),
}

impl InputFileLike for InputPollMedia {
    fn copy_into(&self, into: &mut dyn FnMut(InputFile)) {
        for file in self.files() {
            file.copy_into(into);
        }
    }

    fn move_into(&mut self, into: &mut dyn FnMut(InputFile)) {
        for file in self.files_mut() {
            file.move_into(into);
        }
    }
}

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

impl InputFileLike for InputPollOptionMedia {
    fn copy_into(&self, into: &mut dyn FnMut(InputFile)) {
        for file in self.files() {
            file.copy_into(into);
        }
    }

    fn move_into(&mut self, into: &mut dyn FnMut(InputFile)) {
        for file in self.files_mut() {
            file.move_into(into);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{InputFile, InputFileLike, InputMediaLocation, InputMediaVideo};

    fn video_media() -> InputPollMedia {
        let mut video = InputMediaVideo::new(InputFile::file_id("1".into()));
        video.thumbnail = Some(InputFile::file_id("2".into()));
        video.cover = Some(InputFile::file_id("3".into()));
        InputPollMedia::Video(video)
    }

    #[test]
    fn poll_media_files_include_thumbnail_and_cover() {
        assert_eq!(video_media().files().count(), 3);
    }

    #[test]
    fn poll_media_input_file_like_traverses_all_attachments() {
        let media = video_media();

        let mut copied = Vec::new();
        media.copy_into(&mut |file| copied.push(file));
        assert_eq!(copied.len(), 3);

        let mut media = media;
        let mut moved_count = 0;
        media.move_into(&mut |_| moved_count += 1);
        assert_eq!(moved_count, 3);
    }

    #[test]
    fn poll_option_media_location_has_no_files() {
        let media = InputPollOptionMedia::Location(InputMediaLocation {
            latitude: 0.0,
            longitude: 0.0,
            horizontal_accuracy: None,
        });

        assert_eq!(media.files().count(), 0);
    }
}
