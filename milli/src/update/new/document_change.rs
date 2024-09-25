use heed::RoTxn;

use super::document::{DocumentFromDb, DocumentFromPayload, DocumentFromPayloadOrDb};
use super::TopLevelMap;
use crate::documents::FieldIdMapper;
use crate::update::IndexDocumentsMethod;
use crate::{DocumentId, Index, Result};

pub enum DocumentChange<'pl> {
    Deletion(Deletion),
    Update(Update<'pl>),
    Insertion(Insertion<'pl>),
}
pub struct Deletion {
    docid: DocumentId,
}
pub enum Versions<'pl> {
    Single(TopLevelMap<'pl>),
    Multiple(Vec<TopLevelMap<'pl>>),
}

impl<'pl> Versions<'pl> {
    pub fn into_replace(self) -> Self {
        match self {
            Versions::Single(v) => Versions::Single(v),
            // unwrap: cannot be empty
            Versions::Multiple(mut v) => Versions::Single(v.pop().unwrap()),
        }
    }
}

pub struct Update<'pl> {
    docid: DocumentId,
    versions: Versions<'pl>,
    has_deletion: bool,
}

pub struct Insertion<'pl> {
    docid: DocumentId,
    versions: Versions<'pl>,
}

impl<'pl> DocumentChange<'pl> {
    pub fn docid(&self) -> DocumentId {
        match &self {
            Self::Deletion(inner) => inner.docid(),
            Self::Update(inner) => inner.docid(),
            Self::Insertion(inner) => inner.docid(),
        }
    }
}

impl Deletion {
    pub fn create(docid: DocumentId) -> Self {
        Self { docid }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
    }
    pub fn current<'a, Mapper: FieldIdMapper>(
        &self,
        rtxn: &'a RoTxn,
        index: &'a Index,
        db_fields_ids_map: &'a Mapper,
    ) -> Result<Option<DocumentFromDb<'a, Mapper>>> {
        DocumentFromDb::new(self.docid, rtxn, index, &db_fields_ids_map)
    }
}

impl<'pl> Insertion<'pl> {
    pub fn create(
        docid: DocumentId,
        method: IndexDocumentsMethod,
        versions: Versions<'pl>,
    ) -> Self {
        let versions = match method {
            IndexDocumentsMethod::ReplaceDocuments => versions.into_replace(),
            IndexDocumentsMethod::UpdateDocuments => versions,
        };
        Insertion { docid, versions }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
    }

    pub fn new(&self) -> DocumentFromPayload<'_> {
        DocumentFromPayload::new(&self.versions)
    }
}

impl<'pl> Update<'pl> {
    pub fn create(
        docid: DocumentId,
        method: IndexDocumentsMethod,
        versions: Versions<'pl>,
        has_deletion: bool,
    ) -> Self {
        match method {
            IndexDocumentsMethod::ReplaceDocuments => {
                Update { docid, versions: versions.into_replace(), has_deletion: true }
            }
            IndexDocumentsMethod::UpdateDocuments => Update { docid, versions, has_deletion },
        }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
    }

    pub fn current<'a, Mapper: FieldIdMapper>(
        &self,
        rtxn: &'a RoTxn,
        index: &'a Index,
        db_fields_ids_map: &'a Mapper,
    ) -> Result<Option<DocumentFromDb<'a, Mapper>>> {
        DocumentFromDb::new(self.docid, rtxn, index, &db_fields_ids_map)
    }

    pub fn new<'t, Mapper: FieldIdMapper>(
        &self,
        rtxn: &'t RoTxn,
        index: &'t Index,
        db_fields_ids_map: &'t Mapper,
    ) -> Result<DocumentFromPayloadOrDb<'_, 't, Mapper>> {
        if self.has_deletion {
            Ok(DocumentFromPayloadOrDb::without_db(&self.versions))
        } else {
            DocumentFromPayloadOrDb::with_db(
                self.docid,
                rtxn,
                index,
                db_fields_ids_map,
                &self.versions,
            )
        }
    }
}
