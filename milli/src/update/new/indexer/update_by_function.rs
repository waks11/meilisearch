use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

use super::DocumentChanges;
use crate::update::new::DocumentChange;
use crate::{FieldsIdsMap, Result};

pub struct UpdateByFunction;

impl<'p> DocumentChanges<'p> for UpdateByFunction {
    type Parameter = ();

    fn document_changes<'a>(
        self,
        _fields_ids_map: &'a mut FieldsIdsMap,
        _param: Self::Parameter,
    ) -> Result<impl IndexedParallelIterator<Item = Result<DocumentChange<'p>>> + Clone + 'p> {
        Ok((0..100).into_par_iter().map(|_| todo!()))
    }
}
