use std::sync::Arc;

use polars_core::prelude::*;
use polars_plan::plans::AnonymousScan;

use crate::operators::{Source, PExecutionContext, SourceResult};



pub struct AnonymousSource {
    pub function: Arc<dyn AnonymousScan>,
}

impl AnonymousSource {
    pub fn new(function: Arc<dyn AnonymousScan>) -> Self {
        Self { function }
    }
}

impl Source for AnonymousSource {
    fn get_batches(&mut self, _context: &PExecutionContext) -> PolarsResult<SourceResult> {
        match self.function.get_batches(scan_opts)? {
        
        }
    }
    fn fmt(&self) -> &str {
        self.function.fmt()
    }
}
