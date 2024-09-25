use rayon::iter::{IndexedParallelIterator, ParallelBridge, ParallelIterator};

use super::{DocumentChanges, TopLevelMap};
use crate::documents::{DocumentIdExtractionError, PrimaryKey};
use crate::update::concurrent_available_ids::ConcurrentAvailableIds;
use crate::update::new::document_change::Versions;
use crate::update::new::{DocumentChange, Insertion};
use crate::update::IndexDocumentsMethod;
use crate::{Error, FieldsIdsMap, InternalError, Result, UserError};

pub struct PartialDump<I> {
    iter: I,
}

impl<I> PartialDump<I> {
    pub fn new_from_jsonlines(iter: I) -> Self {
        PartialDump { iter }
    }
}

impl<'p, I> DocumentChanges<'p> for PartialDump<I>
where
    I: IndexedParallelIterator<Item = TopLevelMap<'p>> + Clone + 'p,
{
    type Parameter = (&'p ConcurrentAvailableIds, &'p PrimaryKey<'p>);

    /// Note for future self:
    ///   - the field ids map must already be valid so you must have to generate it beforehand.
    ///   - We should probably expose another method that generates the fields ids map from an iterator of JSON objects.
    ///   - We recommend sending chunks of documents in this `PartialDumpIndexer` we therefore need to create a custom take_while_size method (that doesn't drop items).
    fn document_changes<'a>(
        self,
        _fields_ids_map: &'a mut FieldsIdsMap,
        param: Self::Parameter,
    ) -> Result<impl IndexedParallelIterator<Item = Result<DocumentChange<'p>>> + Clone + 'p> {
        let (concurrent_available_ids, primary_key) = param;

        Ok(self.iter.map(|document| {
            let docid = match concurrent_available_ids.next() {
                Some(id) => id,
                None => return Err(Error::UserError(UserError::DocumentLimitReached)),
            };

            let _external_docid = match primary_key.document_id_from_top_level_map(&document)? {
                Ok(document_id) => Ok(document_id),
                Err(DocumentIdExtractionError::InvalidDocumentId(user_error)) => Err(user_error),
                Err(DocumentIdExtractionError::MissingDocumentId) => {
                    Err(UserError::MissingDocumentId {
                        primary_key: primary_key.name().to_string(),
                        document: (&document).try_into().map_err(InternalError::SerdeJson)?,
                    })
                }
                Err(DocumentIdExtractionError::TooManyDocumentIds(_)) => {
                    Err(UserError::TooManyDocumentIds {
                        primary_key: primary_key.name().to_string(),
                        document: (&document).try_into().map_err(InternalError::SerdeJson)?,
                    })
                }
            }?;

            let insertion = Insertion::create(
                docid,
                IndexDocumentsMethod::ReplaceDocuments,
                Versions::Single(document),
            );
            Ok(DocumentChange::Insertion(insertion))
        }))
    }
}
