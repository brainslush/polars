use std::any::Any;
use std::fmt::{Debug, Formatter};

use polars_core::prelude::*;

use crate::dsl::Expr;

pub struct AnonymousDataChunk {
    pub chunk_index: IdxSize,
    pub data: DataFrame,
}

pub enum AnonymousSourceResult {
    Finished,
    GotMoreData(Vec<AnonymousDataChunk>),
}

pub struct AnonymousScanArgs {
    pub schema: SchemaRef,
    pub output_schema: Option<SchemaRef>,
    pub skip_rows: Option<usize>,
    pub n_rows: Option<usize>,
    pub predicate: Option<Expr>,
}

impl AnonymousScanArgs {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            output_schema: None,
            skip_rows: None,
            n_rows: None,
            predicate: None,
        }
    }
    pub fn with_output_schema(mut self, output_schema: SchemaRef) -> Self {
        self.output_schema = Some(output_schema);
        self
    }
    pub fn with_skip_rows(mut self, skip_rows: usize) -> Self {
        self.skip_rows = Some(skip_rows);
        self
    }
    pub fn with_n_rows(mut self, n_rows: usize) -> Self {
        self.n_rows = Some(n_rows);
        self
    }
    pub fn with_predicate(mut self, predicate: Expr) -> Self {
        self.predicate = Some(predicate);
        self
    }
}

pub trait AnonymousScan: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    /// Creates a DataFrame from the supplied function & scan options.
    fn scan(&self, scan_opts: AnonymousScanArgs) -> PolarsResult<DataFrame>;

    /// function to supply the schema.
    /// Allows for an optional infer schema argument for data sources with dynamic schemas
    fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef>;

    /// function which gets called before the first batches are collected, Implement this
    /// function and next_batches to get proper streaming support.
    fn init_batched_scan(&self, _scan_opts: AnonymousScanArgs) -> PolarsResult<()> {
        Ok(())
    }

    /// Produce the next batch Polars can consume. Implement this method to get proper
    /// streaming support.
    fn next_batches(&self) -> PolarsResult<AnonymousSourceResult> {
        Ok(AnonymousSourceResult::Finished {})
    }

    /// Specify if the scan provider should allow predicate pushdowns.
    ///
    /// Defaults to `false`
    fn allows_predicate_pushdown(&self) -> bool {
        false
    }
    /// Specify if the scan provider should allow projection pushdowns.
    ///
    /// Defaults to `false`
    fn allows_projection_pushdown(&self) -> bool {
        false
    }
    /// Specify if the scan provider should allow slice pushdowns.
    ///
    /// Defaults to `false`
    fn allows_slice_pushdown(&self) -> bool {
        false
    }
    /// Specify if the scan can stream batches.
    /// Requires the implementation of `next_batch`.
    ///
    /// Defaults to `false`
    fn streamable(&self) -> bool {
        false
    }
    /// Specify a custom name for the anonymous reader.
    /// Defaults to `"anonymous_scan"`.
    fn fmt(&self) -> &str {
        "anonymous_scan"
    }
}

impl Debug for dyn AnonymousScan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "anonymous_scan")
    }
}
