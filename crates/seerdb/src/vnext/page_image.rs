//! Checksummed persistent page-image envelope for the vNext kernel.
//!
//! The envelope owns generic materialization authority: store binding, logical
//! page identity, physical placement and the conservative WAL/undo requirements
//! of the exact captured bytes. It deliberately does not own access-method page
//! semantics, so a page's maximum WAL LSN can never be mistaken for logical
//! replay completeness.
//!
//! Framing follows the established vNext local policy: validate magic, version,
//! flags, sizes and reserved fields, then verify the header checksum, and only
//! then trust length fields. Complete corruption fails closed; only an
//! incomplete final append is repairable, and that decision belongs to the arena
//! scanner in [`super::page_store`], not to this codec.

use super::{
    Lsn, PageDependencies, PageId, PageKey, PageStoreError, StorageObjectId, StoreIncarnation,
    VersionId,
};

pub(crate) const IMAGE_FILE_MAGIC: [u8; 4] = *b"OMPA";
pub(crate) const IMAGE_FILE_VERSION: u8 = 1;
pub(crate) const IMAGE_FILE_HEADER_SIZE: usize = 40;
pub(crate) const PAGE_IMAGE_MAGIC: [u8; 4] = *b"OMPI";
pub(crate) const PAGE_IMAGE_VERSION: u8 = 1;
pub(crate) const PAGE_IMAGE_HEADER_SIZE: usize = 80;
pub(crate) const PAGE_IMAGE_TRAILER_SIZE: usize = 4;

/// Exact on-disk size of one page image for a configured logical page size.
pub(crate) const fn page_image_bytes(page_bytes: u32) -> Option<u64> {
    let header = PAGE_IMAGE_HEADER_SIZE as u64;
    let trailer = PAGE_IMAGE_TRAILER_SIZE as u64;
    match header.checked_add(page_bytes as u64) {
        Some(body) => body.checked_add(trailer),
        None => None,
    }
}

/// Whether `offset` is the start of an image slot for this page size.
pub(crate) const fn is_image_offset(offset: u64, page_bytes: u32) -> bool {
    match page_image_bytes(page_bytes) {
        Some(slot) => {
            let header = IMAGE_FILE_HEADER_SIZE as u64;
            offset >= header && (offset - header).is_multiple_of(slot)
        }
        None => false,
    }
}

/// Physical placement of one persisted image inside its store.
///
/// The location is scoped to the containing store incarnation. It is not a
/// standalone identity: two stores may reuse the same offset.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct ImageLocation {
    offset: u64,
    checksum: u32,
}

impl ImageLocation {
    pub(crate) const fn new(offset: u64, checksum: u32) -> Self {
        Self { offset, checksum }
    }

    /// Return the absolute byte offset of the image envelope.
    #[must_use]
    pub const fn offset(self) -> u64 {
        self.offset
    }

    /// Return the expected checksum of the complete image frame.
    #[must_use]
    pub const fn checksum(self) -> u32 {
        self.checksum
    }
}

/// Validated metadata of one persisted page image.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PageImageMetadata {
    store: StoreIncarnation,
    key: PageKey,
    location: ImageLocation,
    page_bytes: u32,
    dependencies: PageDependencies,
}

impl PageImageMetadata {
    /// Return the store incarnation that owns this image.
    #[must_use]
    pub const fn store(self) -> StoreIncarnation {
        self.store
    }

    /// Return the logical page identity of this image.
    #[must_use]
    pub const fn key(self) -> PageKey {
        self.key
    }

    /// Return the physical placement of this image.
    #[must_use]
    pub const fn location(self) -> ImageLocation {
        self.location
    }

    /// Return the configured logical page size.
    #[must_use]
    pub const fn page_bytes(self) -> u32 {
        self.page_bytes
    }

    /// Return the conservative durability requirements of these exact bytes.
    #[must_use]
    pub const fn dependencies(self) -> PageDependencies {
        self.dependencies
    }
}

/// Validated image-arena file header.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct ImageFileHeader {
    store: StoreIncarnation,
    page_bytes: u32,
}

impl ImageFileHeader {
    pub(crate) const fn store(self) -> StoreIncarnation {
        self.store
    }

    pub(crate) const fn page_bytes(self) -> u32 {
        self.page_bytes
    }
}

pub(crate) fn read_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    let slice = bytes.get(offset..end)?;
    Some(u32::from_le_bytes(slice.try_into().ok()?))
}

pub(crate) fn read_u64(bytes: &[u8], offset: usize) -> Option<u64> {
    let end = offset.checked_add(8)?;
    let slice = bytes.get(offset..end)?;
    Some(u64::from_le_bytes(slice.try_into().ok()?))
}

/// Encode the mandatory image-arena file header.
pub(crate) fn encode_file_header(
    store: StoreIncarnation,
    page_bytes: u32,
) -> [u8; IMAGE_FILE_HEADER_SIZE] {
    let mut header = [0u8; IMAGE_FILE_HEADER_SIZE];
    header[..4].copy_from_slice(&IMAGE_FILE_MAGIC);
    header[4] = IMAGE_FILE_VERSION;
    header[6..8].copy_from_slice(&(IMAGE_FILE_HEADER_SIZE as u16).to_le_bytes());
    header[8..24].copy_from_slice(store.as_bytes());
    header[24..28].copy_from_slice(&page_bytes.to_le_bytes());
    let checksum = crc32c::crc32c(&header[..36]);
    header[36..40].copy_from_slice(&checksum.to_le_bytes());
    header
}

/// Decode and validate the image-arena file header.
pub(crate) fn decode_file_header(bytes: &[u8]) -> Result<ImageFileHeader, PageStoreError> {
    let header = bytes
        .get(..IMAGE_FILE_HEADER_SIZE)
        .ok_or(PageStoreError::Corruption(
            "truncated page-image file header",
        ))?;
    if header[..4] != IMAGE_FILE_MAGIC {
        return Err(PageStoreError::Corruption("invalid page-image file magic"));
    }
    if header[4] != IMAGE_FILE_VERSION {
        return Err(PageStoreError::Corruption(
            "unsupported page-image file version",
        ));
    }
    if header[5] != 0 {
        return Err(PageStoreError::Corruption("unsupported page-image flags"));
    }
    if read_u16(header, 6) != Some(IMAGE_FILE_HEADER_SIZE as u16) {
        return Err(PageStoreError::Corruption(
            "invalid page-image file header size",
        ));
    }
    if header[28..36].iter().any(|byte| *byte != 0) {
        return Err(PageStoreError::Corruption(
            "nonzero page-image file reserved field",
        ));
    }
    let expected = read_u32(header, 36).ok_or(PageStoreError::Corruption(
        "invalid page-image file header checksum field",
    ))?;
    if crc32c::crc32c(&header[..36]) != expected {
        return Err(PageStoreError::Corruption(
            "page-image file header checksum mismatch",
        ));
    }
    let store = read_incarnation(header, 8, "page-image file store incarnation")?;
    let page_bytes =
        read_u32(header, 24)
            .filter(|size| *size != 0)
            .ok_or(PageStoreError::Corruption(
                "invalid page-image file page size",
            ))?;
    Ok(ImageFileHeader { store, page_bytes })
}

/// Validated page-image frame header.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct ImageHeader {
    store: StoreIncarnation,
    offset: u64,
    key: PageKey,
    page_bytes: u32,
    dependencies: PageDependencies,
}

impl ImageHeader {
    pub(crate) const fn store(self) -> StoreIncarnation {
        self.store
    }

    pub(crate) const fn offset(self) -> u64 {
        self.offset
    }

    pub(crate) const fn key(self) -> PageKey {
        self.key
    }

    pub(crate) const fn page_bytes(self) -> u32 {
        self.page_bytes
    }

    pub(crate) const fn dependencies(self) -> PageDependencies {
        self.dependencies
    }
}

/// Decode and validate one page-image frame header.
///
/// This performs the framing checks that must hold before any length field is
/// trusted, including the header checksum, store binding and configured page
/// size. It is what distinguishes a well-framed but incomplete final append
/// from a malformed header, so it never depends on the frame being complete.
pub(crate) fn decode_image_header(
    bytes: &[u8],
    expected_store: StoreIncarnation,
    expected_page_bytes: u32,
) -> Result<ImageHeader, PageStoreError> {
    let header = bytes
        .get(..PAGE_IMAGE_HEADER_SIZE)
        .ok_or(PageStoreError::Corruption("truncated page-image header"))?;
    if header[..4] != PAGE_IMAGE_MAGIC {
        return Err(PageStoreError::Corruption("invalid page-image magic"));
    }
    if header[4] != PAGE_IMAGE_VERSION {
        return Err(PageStoreError::Corruption("unsupported page-image version"));
    }
    if header[5] != 0 {
        return Err(PageStoreError::Corruption("unsupported page-image flags"));
    }
    if read_u16(header, 6) != Some(PAGE_IMAGE_HEADER_SIZE as u16) {
        return Err(PageStoreError::Corruption("invalid page-image header size"));
    }
    if header[68..76].iter().any(|byte| *byte != 0) {
        return Err(PageStoreError::Corruption(
            "nonzero page-image reserved field",
        ));
    }
    let header_checksum = read_u32(header, 76).ok_or(PageStoreError::Corruption(
        "invalid page-image header checksum field",
    ))?;
    if crc32c::crc32c(&header[..76]) != header_checksum {
        return Err(PageStoreError::Corruption(
            "page-image header checksum mismatch",
        ));
    }

    let store = read_incarnation(header, 8, "page-image store incarnation")?;
    if store != expected_store {
        return Err(PageStoreError::Corruption(
            "page-image store incarnation mismatch",
        ));
    }
    let page_bytes = read_u32(header, 64)
        .filter(|size| *size != 0)
        .ok_or(PageStoreError::Corruption("invalid page-image page size"))?;
    if page_bytes != expected_page_bytes {
        return Err(PageStoreError::Corruption(
            "page-image payload size mismatch",
        ));
    }

    let offset = read_u64(header, 24).ok_or(PageStoreError::Corruption(
        "invalid page-image offset field",
    ))?;
    let object = read_u64(header, 32).ok_or(PageStoreError::Corruption(
        "invalid page-image object field",
    ))?;
    let page =
        read_u64(header, 40).ok_or(PageStoreError::Corruption("invalid page-image page field"))?;
    let required_wal = read_u64(header, 48).ok_or(PageStoreError::Corruption(
        "invalid page-image WAL requirement field",
    ))?;
    let required_undo = read_u64(header, 56).ok_or(PageStoreError::Corruption(
        "invalid page-image undo requirement field",
    ))?;
    Ok(ImageHeader {
        store,
        offset,
        key: PageKey::new(StorageObjectId::new(object), PageId::new(page)),
        page_bytes,
        dependencies: PageDependencies::new(
            Lsn::new(required_wal),
            (required_undo != 0).then(|| VersionId::new(required_undo)),
        ),
    })
}

/// Decode and validate one complete page-image frame.
///
/// The caller supplies exactly one frame. Incomplete or oversized input is
/// corruption here; incomplete-final-append classification belongs to the arena
/// scanner, which knows the file's complete prefix.
pub(crate) fn decode_image(
    bytes: &[u8],
    expected_store: StoreIncarnation,
    expected_page_bytes: u32,
) -> Result<(PageImageMetadata, &[u8]), PageStoreError> {
    let header = decode_image_header(bytes, expected_store, expected_page_bytes)?;
    let total = page_image_bytes(header.page_bytes())
        .and_then(|total| usize::try_from(total).ok())
        .ok_or(PageStoreError::Exhausted)?;
    if bytes.len() != total {
        return Err(PageStoreError::Corruption(
            "page-image frame length mismatch",
        ));
    }
    let trailer = total - PAGE_IMAGE_TRAILER_SIZE;
    let frame_checksum = read_u32(bytes, trailer).ok_or(PageStoreError::Corruption(
        "invalid page-image frame checksum field",
    ))?;
    if crc32c::crc32c(&bytes[..trailer]) != frame_checksum {
        return Err(PageStoreError::Corruption(
            "page-image frame checksum mismatch",
        ));
    }
    let metadata = PageImageMetadata {
        store: header.store(),
        key: header.key(),
        location: ImageLocation::new(header.offset(), frame_checksum),
        page_bytes: header.page_bytes(),
        dependencies: header.dependencies(),
    };
    Ok((metadata, &bytes[PAGE_IMAGE_HEADER_SIZE..trailer]))
}

/// Encode one complete page-image frame.
pub(crate) fn encode_image(
    store: StoreIncarnation,
    key: PageKey,
    offset: u64,
    page_bytes: u32,
    dependencies: PageDependencies,
    payload: &[u8],
) -> Result<Vec<u8>, PageStoreError> {
    if payload.len() != page_bytes as usize {
        return Err(PageStoreError::InvalidInput(
            "page-image payload size does not match the configured page size",
        ));
    }
    let total = page_image_bytes(page_bytes)
        .and_then(|total| usize::try_from(total).ok())
        .ok_or(PageStoreError::Exhausted)?;
    let mut frame = vec![0u8; total];
    frame[..4].copy_from_slice(&PAGE_IMAGE_MAGIC);
    frame[4] = PAGE_IMAGE_VERSION;
    frame[6..8].copy_from_slice(&(PAGE_IMAGE_HEADER_SIZE as u16).to_le_bytes());
    frame[8..24].copy_from_slice(store.as_bytes());
    frame[24..32].copy_from_slice(&offset.to_le_bytes());
    frame[32..40].copy_from_slice(&key.object().get().to_le_bytes());
    frame[40..48].copy_from_slice(&key.page().get().to_le_bytes());
    frame[48..56].copy_from_slice(&dependencies.required_wal().get().to_le_bytes());
    let required_undo = match dependencies.required_undo() {
        Some(version) if version.get() != 0 => version.get(),
        Some(_) => {
            return Err(PageStoreError::InvalidInput(
                "page-image undo requirement cannot be the reserved identity zero",
            ));
        }
        None => 0,
    };
    frame[56..64].copy_from_slice(&required_undo.to_le_bytes());
    frame[64..68].copy_from_slice(&page_bytes.to_le_bytes());
    let header_checksum = crc32c::crc32c(&frame[..76]);
    frame[76..80].copy_from_slice(&header_checksum.to_le_bytes());
    frame[PAGE_IMAGE_HEADER_SIZE..PAGE_IMAGE_HEADER_SIZE + page_bytes as usize]
        .copy_from_slice(payload);
    let trailer = total - PAGE_IMAGE_TRAILER_SIZE;
    let frame_checksum = crc32c::crc32c(&frame[..trailer]);
    frame[trailer..].copy_from_slice(&frame_checksum.to_le_bytes());
    Ok(frame)
}

fn read_u16(bytes: &[u8], offset: usize) -> Option<u16> {
    let end = offset.checked_add(2)?;
    let slice = bytes.get(offset..end)?;
    Some(u16::from_le_bytes(slice.try_into().ok()?))
}

fn read_incarnation(
    bytes: &[u8],
    offset: usize,
    message: &'static str,
) -> Result<StoreIncarnation, PageStoreError> {
    let end = offset
        .checked_add(16)
        .ok_or(PageStoreError::Corruption(message))?;
    let slice = bytes
        .get(offset..end)
        .ok_or(PageStoreError::Corruption(message))?;
    let raw: [u8; 16] = slice
        .try_into()
        .map_err(|_| PageStoreError::Corruption(message))?;
    StoreIncarnation::from_bytes(raw).ok_or(PageStoreError::Corruption(message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::ids::test_incarnation;

    const N: u32 = 64;

    fn key(object: u64, page: u64) -> PageKey {
        PageKey::new(StorageObjectId::new(object), PageId::new(page))
    }

    fn frame(offset: u64) -> Vec<u8> {
        encode_image(
            test_incarnation(1),
            key(3, 4),
            offset,
            N,
            PageDependencies::new(Lsn::new(21), Some(VersionId::new(7))),
            &[0xab; N as usize],
        )
        .expect("encodes")
    }

    #[test]
    fn file_header_layout_is_fixed_and_checksum_covers_the_header() {
        let header = encode_file_header(test_incarnation(9), N);
        assert_eq!(header.len(), IMAGE_FILE_HEADER_SIZE);
        assert_eq!(&header[..4], &IMAGE_FILE_MAGIC);
        assert_eq!(header[4], IMAGE_FILE_VERSION);
        assert_eq!(header[5], 0);
        assert_eq!(u16::from_le_bytes(header[6..8].try_into().unwrap()), 40);
        assert_eq!(&header[8..24], test_incarnation(9).as_bytes());
        assert_eq!(u32::from_le_bytes(header[24..28].try_into().unwrap()), N);
        assert!(header[28..36].iter().all(|byte| *byte == 0));
        assert_eq!(
            u32::from_le_bytes(header[36..40].try_into().unwrap()),
            crc32c::crc32c(&header[..36])
        );
        assert_eq!(
            decode_file_header(&header).expect("decodes").page_bytes(),
            N
        );
    }

    #[test]
    fn image_layout_carries_page_identity_dependencies_and_placement() {
        let bytes = frame(40);
        assert_eq!(bytes.len(), (84 + N) as usize);
        assert_eq!(&bytes[..4], &PAGE_IMAGE_MAGIC);
        assert_eq!(bytes[4], PAGE_IMAGE_VERSION);
        assert_eq!(u64::from_le_bytes(bytes[24..32].try_into().unwrap()), 40);
        assert_eq!(u64::from_le_bytes(bytes[32..40].try_into().unwrap()), 3);
        assert_eq!(u64::from_le_bytes(bytes[40..48].try_into().unwrap()), 4);
        assert_eq!(u64::from_le_bytes(bytes[48..56].try_into().unwrap()), 21);
        assert_eq!(u64::from_le_bytes(bytes[56..64].try_into().unwrap()), 7);
        assert_eq!(u32::from_le_bytes(bytes[64..68].try_into().unwrap()), N);
        assert!(bytes[68..76].iter().all(|byte| *byte == 0));
        assert_eq!(
            u32::from_le_bytes(bytes[76..80].try_into().unwrap()),
            crc32c::crc32c(&bytes[..76])
        );
        let trailer = bytes.len() - 4;
        assert_eq!(
            u32::from_le_bytes(bytes[trailer..].try_into().unwrap()),
            crc32c::crc32c(&bytes[..trailer])
        );

        let (metadata, payload) = decode_image(&bytes, test_incarnation(1), N).expect("decodes");
        assert_eq!(metadata.key(), key(3, 4));
        assert_eq!(metadata.location().offset(), 40);
        assert_eq!(
            metadata.location().checksum(),
            crc32c::crc32c(&bytes[..trailer])
        );
        assert_eq!(metadata.dependencies().required_wal(), Lsn::new(21));
        assert_eq!(
            metadata.dependencies().required_undo(),
            Some(VersionId::new(7))
        );
        assert_eq!(payload, &[0xab; N as usize]);
    }

    #[test]
    fn image_field_corruption_fails_closed() {
        let valid = frame(40);
        // Header framing fields and the header checksum are validated first.
        for (offset, value) in [(0usize, 0x00u8), (4, 2), (5, 1), (6, 41), (68, 1), (76, 0)] {
            let mut bytes = valid.clone();
            bytes[offset] = value;
            assert!(
                matches!(
                    decode_image(&bytes, test_incarnation(1), N),
                    Err(PageStoreError::Corruption(_))
                ),
                "header byte {offset} must fail closed"
            );
        }
        // Identity, placement and dependency fields are covered by the header CRC.
        for offset in [8usize, 24, 32, 40, 48, 56, 64] {
            let mut bytes = valid.clone();
            bytes[offset] ^= 0xff;
            assert!(matches!(
                decode_image(&bytes, test_incarnation(1), N),
                Err(PageStoreError::Corruption(_))
            ));
        }
        // Payload and trailer are covered by the frame checksum.
        let mut payload = valid.clone();
        payload[PAGE_IMAGE_HEADER_SIZE] ^= 0xff;
        assert!(matches!(
            decode_image(&payload, test_incarnation(1), N),
            Err(PageStoreError::Corruption(_))
        ));
        let mut trailer = valid.clone();
        let last = trailer.len() - 1;
        trailer[last] ^= 0xff;
        assert!(matches!(
            decode_image(&trailer, test_incarnation(1), N),
            Err(PageStoreError::Corruption(_))
        ));
    }

    #[test]
    fn foreign_store_and_wrong_page_size_fail_closed() {
        let bytes = frame(40);
        assert!(matches!(
            decode_image(&bytes, test_incarnation(2), N),
            Err(PageStoreError::Corruption(_))
        ));
        assert!(matches!(
            decode_image(&bytes, test_incarnation(1), N + 1),
            Err(PageStoreError::Corruption(_))
        ));
        assert!(matches!(
            decode_image(&bytes[..bytes.len() - 1], test_incarnation(1), N),
            Err(PageStoreError::Corruption(_))
        ));
        assert!(matches!(
            decode_image_header(&bytes[..79], test_incarnation(1), N),
            Err(PageStoreError::Corruption(_))
        ));
    }

    #[test]
    fn encoding_rejects_mismatched_payload_and_reserved_undo_identity() {
        assert!(matches!(
            encode_image(
                test_incarnation(1),
                key(1, 1),
                40,
                N,
                PageDependencies::none(),
                &[0; 4],
            ),
            Err(PageStoreError::InvalidInput(_))
        ));
        assert!(matches!(
            encode_image(
                test_incarnation(1),
                key(1, 1),
                40,
                N,
                PageDependencies::new(Lsn::new(0), Some(VersionId::new(0))),
                &[0; N as usize],
            ),
            Err(PageStoreError::InvalidInput(_))
        ));
        assert_eq!(page_image_bytes(N), Some(84 + N as u64));
        assert!(is_image_offset(40, N));
        assert!(is_image_offset(40 + (84 + N as u64), N));
        assert!(!is_image_offset(41, N));
        assert!(!is_image_offset(39, N));
    }
}
