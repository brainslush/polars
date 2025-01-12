use std::sync::Arc;

use polars_core::{prelude::*, POOL};
use polars_plan::plans::{AnonymousScan, AnonymousScanArgs, AnonymousSourceResult};

use crate::operators::{Source, PExecutionContext, SourceResult};



pub struct AnonymousSource {
    pub function: Arc<dyn AnonymousScan>,
    n_threads: usize,
    scan_arguments: AnonymousScanArgs
}

impl AnonymousSource {
    pub fn new(
        function: Arc<dyn AnonymousScan>,
    ) -> Self {
        Self { function, n_threads: POOL.current_num_threads(), }
    }
}

impl Into<SourceResult> for AnonymousSourceResult {
    fn into(self) -> SourceResult {
        match self {
            AnonymousSourceResult::Finished => SourceResult::Finished,
            AnonymousSourceResult::GotMoreData(chunks) => SourceResult::GotMoreData(chunks),
        }
    }
}

impl Source for AnonymousSource {
    fn get_batches(&mut self, _context: &PExecutionContext) -> PolarsResult<SourceResult> {
        self.function.get_batches(scan_opts)
    }

    fn fmt(&self) -> &str {
        self.function.as_ref().fmt()
    }
}
