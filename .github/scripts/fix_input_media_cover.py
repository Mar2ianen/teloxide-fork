from pathlib import Path

path = Path("crates/teloxide-core/src/types/input_media.rs")
text = path.read_text()

old = '''impl InputMedia {
    /// Returns an iterator of all files in this input media
    pub(crate) fn files(&self) -> impl Iterator<Item = &InputFile> {
        use InputMedia::*;

        let mut files = Vec::new();
        let (media, thumbnail) = match self {
            Photo(InputMediaPhoto { media, .. }) => (media, None),
            LivePhoto(InputMediaLivePhoto { media, photo, .. }) => (media, Some(photo)),
            Document(InputMediaDocument { media, thumbnail, .. })
            | Audio(InputMediaAudio { media, thumbnail, .. })
            | Animation(InputMediaAnimation { media, thumbnail, .. })
            | Video(InputMediaVideo { media, thumbnail, .. }) => (media, thumbnail.as_ref()),
        };

        files.push(media);
        files.extend(thumbnail);
        files.into_iter()
    }

    /// Returns an iterator of all files in this input media
    pub(crate) fn files_mut(&mut self) -> impl Iterator<Item = &mut InputFile> {
        use InputMedia::*;

        let mut files = Vec::new();
        let (media, thumbnail) = match self {
            Photo(InputMediaPhoto { media, .. }) => (media, None),
            LivePhoto(InputMediaLivePhoto { media, photo, .. }) => (media, Some(photo)),
            Document(InputMediaDocument { media, thumbnail, .. })
            | Audio(InputMediaAudio { media, thumbnail, .. })
            | Animation(InputMediaAnimation { media, thumbnail, .. })
            | Video(InputMediaVideo { media, thumbnail, .. }) => (media, thumbnail.as_mut()),
        };

        files.push(media);
        files.extend(thumbnail);
        files.into_iter()
    }
}'''

new = '''impl InputMedia {
    /// Returns an iterator of all files in this input media.
    pub(crate) fn files(&self) -> impl Iterator<Item = &InputFile> {
        let mut files = Vec::new();

        match self {
            Self::Photo(media) => files.push(&media.media),
            Self::LivePhoto(media) => {
                files.push(&media.media);
                files.push(&media.photo);
            }
            Self::Document(media) => {
                files.push(&media.media);
                files.extend(media.thumbnail.iter());
            }
            Self::Audio(media) => {
                files.push(&media.media);
                files.extend(media.thumbnail.iter());
            }
            Self::Animation(media) => {
                files.push(&media.media);
                files.extend(media.thumbnail.iter());
            }
            Self::Video(media) => {
                files.push(&media.media);
                files.extend(media.thumbnail.iter());
                files.extend(media.cover.iter());
            }
        }

        files.into_iter()
    }

    /// Returns an iterator of all mutable files in this input media.
    pub(crate) fn files_mut(&mut self) -> impl Iterator<Item = &mut InputFile> {
        let mut files = Vec::new();

        match self {
            Self::Photo(media) => files.push(&mut media.media),
            Self::LivePhoto(media) => {
                files.push(&mut media.media);
                files.push(&mut media.photo);
            }
            Self::Document(media) => {
                files.push(&mut media.media);
                files.extend(media.thumbnail.iter_mut());
            }
            Self::Audio(media) => {
                files.push(&mut media.media);
                files.extend(media.thumbnail.iter_mut());
            }
            Self::Animation(media) => {
                files.push(&mut media.media);
                files.extend(media.thumbnail.iter_mut());
            }
            Self::Video(media) => {
                files.push(&mut media.media);
                files.extend(media.thumbnail.iter_mut());
                files.extend(media.cover.iter_mut());
            }
        }

        files.into_iter()
    }
}'''

if text.count(old) != 1:
    raise RuntimeError("InputMedia file traversal anchor changed")
text = text.replace(old, new, 1)

anchor = '''    #[test]
    fn photo_serialize() {'''
test = '''    fn local_file() -> InputFile {
        InputFile::memory(vec![1_u8])
    }

    #[test]
    fn video_files_include_media_thumbnail_and_cover() {
        let mut video = InputMedia::Video(
            InputMediaVideo::new(local_file()).thumbnail(local_file()).cover(local_file()),
        );

        assert_eq!(video.files().count(), 3);
        assert_eq!(video.files_mut().count(), 3);
    }

    #[test]
    fn live_photo_files_include_video_and_photo() {
        let mut live_photo =
            InputMedia::LivePhoto(InputMediaLivePhoto::new(local_file(), local_file()));

        assert_eq!(live_photo.files().count(), 2);
        assert_eq!(live_photo.files_mut().count(), 2);
    }

'''
if text.count(anchor) != 1:
    raise RuntimeError("InputMedia test anchor changed")
text = text.replace(anchor, test + anchor, 1)

path.write_text(text)
