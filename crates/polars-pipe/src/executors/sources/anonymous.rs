use std::sync::Arc;

use polars_core::prelude::*;
use polars_core::POOL;
use polars_io::predicates::PhysicalIoExpr;
use polars_plan::plans::expr_ir::ExprIR;
use polars_plan::plans::{
    AnonymousScanArgs, AnonymousDataChunk, AnonymousScan, AnonymousScanArgs,
    AnonymousSourceResult,
};

use super::DataChunk;
use crate::operators::{PExecutionContext, Source, SourceResult};

pub struct AnonymousSource {
    function: Arc<dyn AnonymousScan>,
    scan_args: AnonymousScanArgs,
}

impl AnonymousSource {
    pub fn new(
        function: Arc<dyn AnonymousScan>,
        scan_args: AnonymousScanArgs,
    ) -> PolarsResult<Self> {
        function.as_ref().init_batched_scan(scan_args)?;

        Ok(Self {
            function,
            scan_args,
        })
    }
}

impl Into<DataChunk> for AnonymousDataChunk {
    fn into(self) -> DataChunk {
        DataChunk {
            chunk_index: self.chunk_index,
            data: self.data,
        }
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
