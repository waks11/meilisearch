use std::collections::BTreeSet;

use heed::RoTxn;
use serde_json::value::RawValue;

use super::document_change::Versions;
use super::{KvReaderFieldId, KvWriterFieldId, TopLevelMap};
use crate::documents::FieldIdMapper;
use crate::{DocumentId, FieldId, Index, Result};

pub trait Document<'s> {
    fn iter_top_level_fields(&'s self) -> impl Iterator<Item = (&'s str, &'s RawValue)>;
    fn field(&'s self, name: &str) -> Option<&'s RawValue>;

    fn write_to_obkv<'a, 'b>(
        &'s self,
        fields_ids_map: &'a impl FieldIdMapper,
        mut document_buffer: &'a mut Vec<u8>,
    ) -> &'a KvReaderFieldId
    where
        's: 'a,
        's: 'b,
    {
        document_buffer.clear();
        let mut unordered_field_buffer = Vec::new();
        unordered_field_buffer.clear();

        let mut writer = KvWriterFieldId::new(&mut document_buffer);

        unordered_field_buffer.extend(self.iter_top_level_fields().map(|(field_name, value)| {
            /// TODO: handle error here, all top level fields should be present
            let field_id = fields_ids_map.id(field_name).unwrap();
            (field_id, value)
        }));

        unordered_field_buffer.sort_by_key(|(fid, _)| *fid);
        for (fid, value) in unordered_field_buffer.iter() {
            writer.insert(*fid, value.get().as_bytes()).unwrap();
        }

        writer.finish().unwrap();
        KvReaderFieldId::from_slice(document_buffer)
    }
}

#[derive(Clone, Copy)]
pub struct DocumentFromDb<'t, Mapper: FieldIdMapper>
where
    Mapper: FieldIdMapper,
{
    fields_ids_map: &'t Mapper,
    content: &'t KvReaderFieldId,
}

impl<'d, 't: 'd, Mapper: FieldIdMapper> Document<'d> for DocumentFromDb<'t, Mapper> {
    fn iter_top_level_fields(&'d self) -> impl Iterator<Item = (&'d str, &'d RawValue)> {
        let mut it = self.content.iter();
        std::iter::from_fn(move || {
            let (fid, value) = it.next()?;
            /// FIXME: handle unwrap
            let value = serde_json::from_slice(value).unwrap();
            /// FIXME: handle unwrap
            let name = self.fields_ids_map.name(fid).unwrap();

            Some((name, value))
        })
    }

    fn field(&'d self, name: &str) -> Option<&'d RawValue> {
        /// TODO: handle nested fields
        self.fields_ids_map.id(name).and_then(|fid| self.field_from_fid(fid))
    }
}

impl<'t, Mapper: FieldIdMapper> DocumentFromDb<'t, Mapper> {
    pub fn new(
        docid: DocumentId,
        rtxn: &'t RoTxn,
        index: &'t Index,
        db_fields_ids_map: &'t Mapper,
    ) -> Result<Option<Self>> {
        index.documents.get(rtxn, &docid).map_err(crate::Error::from).map(|reader| {
            reader.map(|reader| Self { fields_ids_map: db_fields_ids_map, content: reader })
        })
    }

    fn field_from_fid(&self, fid: FieldId) -> Option<&'t RawValue> {
        /// FIXME: handle unwrap
        self.content.get(fid).map(|v| serde_json::from_slice(v).unwrap())
    }
}

pub struct DocumentFromPayload<'pl> {
    versions: &'pl Versions<'pl>,
}

impl<'pl> DocumentFromPayload<'pl> {
    pub fn new(versions: &'pl Versions<'pl>) -> Self {
        Self { versions }
    }
}

pub struct DocumentFromPayloadOrDb<'pl, 't, Mapper: FieldIdMapper> {
    payload: DocumentFromPayload<'pl>,
    db: Option<DocumentFromDb<'t, Mapper>>,
}

impl<'pl, 't, Mapper: FieldIdMapper> DocumentFromPayloadOrDb<'pl, 't, Mapper> {
    pub fn new(payload: DocumentFromPayload<'pl>, db: Option<DocumentFromDb<'t, Mapper>>) -> Self {
        Self { payload, db }
    }

    pub fn with_db(
        docid: DocumentId,
        rtxn: &'t RoTxn,
        index: &'t Index,
        db_fields_ids_map: &'t Mapper,
        versions: &'pl Versions<'pl>,
    ) -> Result<Self> {
        let payload = DocumentFromPayload::new(versions);
        let db = DocumentFromDb::new(docid, rtxn, index, db_fields_ids_map)?;
        Ok(Self { payload, db })
    }

    pub fn without_db(versions: &'pl Versions<'pl>) -> Self {
        let payload = DocumentFromPayload::new(versions);
        Self { payload, db: None }
    }
}

impl<'d, 'pl: 'd> Document<'d> for TopLevelMap<'pl> {
    fn iter_top_level_fields(&'d self) -> impl Iterator<Item = (&'d str, &'d RawValue)> {
        self.iter().map(|(name, value)| {
            let value = *value;
            (name.as_ref(), value)
        })
    }

    fn field(&'d self, name: &str) -> Option<&'d RawValue> {
        self.get(name).copied()
    }
}

impl<'d, 'pl: 'd, 't: 'd, Mapper: FieldIdMapper> Document<'d>
    for DocumentFromPayloadOrDb<'pl, 't, Mapper>
{
    fn iter_top_level_fields(&'d self) -> impl Iterator<Item = (&'d str, &'d RawValue)> {
        match self.payload.versions {
            Versions::Single(version) => either::Left(
                version
                    .iter_top_level_fields()
                    .chain(self.db.iter().flat_map(|db| db.iter_top_level_fields())),
            ),
            Versions::Multiple(versions) => {
                let mut seen_fields: BTreeSet<&str> = BTreeSet::new();
                let version_it = versions
                    .iter()
                    .rev()
                    .flat_map(|version| version.iter())
                    .map(|(name, value)| (name.as_ref(), *value));
                let db_it = self.db.iter().flat_map(|db| db.iter_top_level_fields());

                let mut it = version_it.chain(db_it);

                either::Either::Right(std::iter::from_fn(move || loop {
                    let (name, value) = it.next()?;

                    if seen_fields.contains(name) {
                        continue;
                    }
                    seen_fields.insert(name);
                    return Some((name, value));
                }))
            }
        }
    }

    fn field(&'d self, name: &str) -> Option<&'d RawValue> {
        let payload_field = self.payload.field(name);
        match payload_field {
            Some(payload_field) => Some(payload_field),
            None => {
                if let Some(db) = &self.db {
                    db.field(name)
                } else {
                    None
                }
            }
        }
    }
}

impl<'d, 'pl: 'd> Document<'d> for DocumentFromPayload<'pl> {
    fn iter_top_level_fields(&'d self) -> impl Iterator<Item = (&'d str, &'d RawValue)> {
        match self.versions {
            Versions::Single(version) => either::Either::Left(version.iter_top_level_fields()),
            Versions::Multiple(versions) => {
                let mut seen_fields = BTreeSet::new();
                let mut it = versions.iter().rev().flat_map(|version| version.iter());
                either::Either::Right(std::iter::from_fn(move || loop {
                    let (name, value) = it.next()?;
                    let value = *value;
                    if seen_fields.contains(&name) {
                        continue;
                    }
                    seen_fields.insert(name);
                    return Some((name.as_ref(), value));
                }))
            }
        }
    }

    fn field(&self, name: &str) -> Option<&'pl RawValue> {
        /// TODO: handle nested fields
        match self.versions {
            Versions::Single(version) => version.field(name),
            Versions::Multiple(versions) => {
                for version in versions.iter().rev() {
                    match version.get(name) {
                        Some(field) => return Some(field),
                        None => {
                            continue;
                        }
                    }
                }
                None
            }
        }
    }
}
