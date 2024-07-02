use std::borrow::Cow;

use heed::BoxedError;
use obkv::KvReaderU16;

pub struct CompressedObkvCodec;

impl<'a> heed::BytesDecode<'a> for CompressedObkvCodec {
    type DItem = CompressedKvReaderU16<'a>;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        Ok(CompressedKvReaderU16(bytes))
    }
}

impl heed::BytesEncode<'_> for CompressedObkvCodec {
    type EItem = CompressedKvWriterU16;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
        Ok(Cow::Borrowed(&item.0))
    }
}

pub struct CompressedKvReaderU16<'a>(&'a [u8]);

impl<'a> CompressedKvReaderU16<'a> {
    /// Decompresses the KvReader into the buffer using the provided dictionnary.
    pub fn decompress_with<'b>(
        &self,
        buffer: &'b mut Vec<u8>,
        dictionnary: &[u8],
    ) -> Result<KvReaderU16<'b>, lz4_flex::block::DecompressError> {
        let (size, input) = lz4_flex::block::uncompressed_size(self.0)?;
        buffer.resize(size, 0);
        // TODO loop to increase the buffer size of need be
        let size =
            lz4_flex::block::decompress_into_with_dict(input, &mut buffer[..size], dictionnary)?;
        Ok(KvReaderU16::new(&buffer[..size]))
    }

    /// Returns the KvReader like it is not compressed. Happends when there is no dictionnary yet.
    pub fn as_non_compressed(&self) -> KvReaderU16<'a> {
        KvReaderU16::new(self.0)
    }
}

pub struct CompressedKvWriterU16(Vec<u8>);

impl CompressedKvWriterU16 {
    // TODO ask for a KvReaderU16 here
    pub fn new_with_dictionary(writer: &[u8], dictionary: &[u8]) -> Self {
        CompressedKvWriterU16(lz4_flex::block::compress_prepend_size_with_dict(writer, dictionary))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
