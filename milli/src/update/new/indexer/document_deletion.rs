use std::sync::Arc;

use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use roaring::RoaringBitmap;

use super::DocumentChanges;
use crate::update::new::{Deletion, DocumentChange, ItemsPool};
use crate::{FieldsIdsMap, Index, Result};

pub struct DocumentDeletion {
    pub to_delete: RoaringBitmap,
}

impl DocumentDeletion {
    pub fn new() -> Self {
        Self { to_delete: Default::default() }
    }

    pub fn delete_documents_by_docids(&mut self, docids: RoaringBitmap) {
        self.to_delete |= docids;
    }
}

impl<'p> DocumentChanges<'p> for DocumentDeletion {
    type Parameter = ();

    fn document_changes<'a>(
        self,
        _fields_ids_map: &'a mut FieldsIdsMap,
        _param: Self::Parameter,
    ) -> Result<impl IndexedParallelIterator<Item = Result<DocumentChange<'p>>> + Clone + 'p> {
        let to_delete: Vec<_> = self.to_delete.into_iter().collect();
        Ok(to_delete
            .into_par_iter()
            .map(|docid| Ok(DocumentChange::Deletion(Deletion::create(docid)))))
    }
}
