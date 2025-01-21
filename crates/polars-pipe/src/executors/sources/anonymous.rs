use std::sync::Arc;

use polars_core::prelude::*;
use polars_core::POOL;
use polars_io::predicates::PhysicalIoExpr;
use polars_plan::plans::expr_ir::ExprIR;
use polars_plan::plans::{AnonymousScan, AnonymousScanArgs, AnonymousSourceResult, AnonymousDataChunk};

use crate::operators::{PExecutionContext, Source, SourceResult};

use super::DataChunk;

pub struct AnonymousSource {
    function: Arc<dyn AnonymousScan>,
    predicate: Option<Arc<dyn PhysicalIoExpr>>,
    n_threads: usize,
    scan_arguments: AnonymousScanArgs,
}

impl AnonymousSource {
    pub fn new(
        function: Arc<dyn AnonymousScan>,
        predicate: Option<ExprIR>,
        slice: Option<(usize, usize)>,
    ) -> Self {
        function.init_batched_scan(scan_opts)

        Self {
            function,
            predicate,
            n_threads: POOL.current_num_threads(),
        }
    }
}

impl Into<DataChunk> for AnonymousDataChunk {
    fn into(self) -> DataChunk {
        DataChunk{chunk_index: self.chunk_index, data: self.data}
    }
}

impl Into<SourceResult> for AnonymousSourceResult {
    fn into(self) -> SourceResult {
        match self {
            AnonymousSourceResult::Finished => SourceResult::Finished,
            AnonymousSourceResult::GotMoreData(chunks) => SourceResult::GotMoreData(chunks.into()),
        }
    }
}

impl Source for AnonymousSource {
    fn get_batches(&mut self, _context: &PExecutionContext) -> PolarsResult<SourceResult> {
        self.function.next_batches().map(|result| result.into())
    }

    fn fmt(&self) -> &str {
        self.function.as_ref().fmt()
    }
}
