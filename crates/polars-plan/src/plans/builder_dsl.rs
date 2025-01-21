use std::sync::Arc;

use polars_core::prelude::*;
#[cfg(any(feature = "parquet", feature = "ipc", feature = "csv"))]
use polars_io::cloud::CloudOptions;
#[cfg(feature = "csv")]
use polars_io::csv::read::CsvReadOptions;
#[cfg(feature = "ipc")]
use polars_io::ipc::IpcScanOptions;
#[cfg(feature = "parquet")]
use polars_io::parquet::read::ParquetOptions;
use polars_io::HiveOptions;
#[cfg(any(feature = "parquet", feature = "csv", feature = "ipc"))]
use polars_io::RowIndex;

#[cfg(feature = "python")]
use crate::prelude::python_udf::PythonFunction;
use crate::prelude::*;

pub struct DslBuilder(pub DslPlan);

impl From<DslPlan> for DslBuilder {
    fn from(lp: DslPlan) -> Self {
        DslBuilder(lp)
    }
}

impl DslBuilder {
    pub fn anonymous_scan(
        function: Arc<dyn AnonymousScan>,
        schema: Option<SchemaRef>,
        infer_schema_length: Option<usize>,
        skip_rows: Option<usize>,
        n_rows: Option<usize>,
        name: &'static str,
    ) -> PolarsResult<Self> {
        let schema = match schema {
            Some(s) => s,
            None => function.schema(infer_schema_length)?,
        };

        let file_info = FileInfo::new(schema.clone(), None, (n_rows, n_rows.unwrap_or(usize::MAX)));
        let slice = match (skip_rows, n_rows) {
            (Some(a), Some(b)) => Some((a as i64, a+b)),
            (Some(a), None) => Some((a as i64, usize::MAX)),
            (None, Some(b)) => Some((0, b)),
            _ => None
        };

        let file_options = FileScanOptions {
            slice,
            with_columns: None,
            cache: false,
            row_index: None,
            rechunk: false,
            file_counter: Default::default(),
            // TODO: Support Hive partitioning.
            hive_options: HiveOptions {
                enabled: Some(false),
                ..Default::default()
            },
            glob: false,
            include_file_paths: None,
            allow_missing_columns: false,
        };

        Ok(DslPlan::Scan {
            sources: ScanSources::Buffers(Arc::default()),
            file_info: Some(file_info),
            file_options,
            scan_type: FileScan::Anonymous {
                function,
                options: Arc::new(AnonymousScanOptions {
                    fmt_str: name,
                }),
            },
            cached_ir: Default::default(),
        }
        .into())
    }

    #[cfg(feature = "parquet")]
    #[allow(clippy::too_many_arguments)]
    pub fn scan_parquet(
        sources: ScanSources,
        n_rows: Option<usize>,
        cache: bool,
        parallel: polars_io::parquet::read::ParallelStrategy,
        row_index: Option<RowIndex>,
        rechunk: bool,
        low_memory: bool,
        cloud_options: Option<CloudOptions>,
        use_statistics: bool,
        schema: Option<SchemaRef>,
        hive_options: HiveOptions,
        glob: bool,
        include_file_paths: Option<PlSmallStr>,
        allow_missing_columns: bool,
    ) -> PolarsResult<Self> {
        let options = FileScanOptions {
            with_columns: None,
            cache,
            slice: n_rows.map(|x| (0, x)),
            rechunk,
            row_index,
            file_counter: Default::default(),
            hive_options,
            glob,
            include_file_paths,
            allow_missing_columns,
        };
        Ok(DslPlan::Scan {
            sources,
            file_info: None,
            file_options: options,
            scan_type: FileScan::Parquet {
                options: ParquetOptions {
                    schema,
                    parallel,
                    low_memory,
                    use_statistics,
                },
                cloud_options,
                metadata: None,
            },
            cached_ir: Default::default(),
        }
        .into())
    }

    #[cfg(feature = "ipc")]
    #[allow(clippy::too_many_arguments)]
    pub fn scan_ipc(
        sources: ScanSources,
        options: IpcScanOptions,
        n_rows: Option<usize>,
        cache: bool,
        row_index: Option<RowIndex>,
        rechunk: bool,
        cloud_options: Option<CloudOptions>,
        hive_options: HiveOptions,
        include_file_paths: Option<PlSmallStr>,
    ) -> PolarsResult<Self> {
        Ok(DslPlan::Scan {
            sources,
            file_info: None,
            file_options: FileScanOptions {
                with_columns: None,
                cache,
                slice: n_rows.map(|x| (0, x)),
                rechunk,
                row_index,
                file_counter: Default::default(),
                hive_options,
                glob: true,
                include_file_paths,
                allow_missing_columns: false,
            },
            scan_type: FileScan::Ipc {
                options,
                cloud_options,
                metadata: None,
            },
            cached_ir: Default::default(),
        }
        .into())
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg(feature = "csv")]
    pub fn scan_csv(
        sources: ScanSources,
        read_options: CsvReadOptions,
        cache: bool,
        cloud_options: Option<CloudOptions>,
        glob: bool,
        include_file_paths: Option<PlSmallStr>,
    ) -> PolarsResult<Self> {
        // This gets partially moved by FileScanOptions
        let read_options_clone = read_options.clone();

        let options = FileScanOptions {
            with_columns: None,
            cache,
            slice: read_options_clone.n_rows.map(|x| (0, x)),
            rechunk: read_options_clone.rechunk,
            row_index: read_options_clone.row_index,
            file_counter: Default::default(),
            // TODO: Support Hive partitioning.
            hive_options: HiveOptions {
                enabled: Some(false),
                ..Default::default()
            },
            glob,
            include_file_paths,
            allow_missing_columns: false,
        };
        Ok(DslPlan::Scan {
            sources,
            file_info: None,
            file_options: options,
            scan_type: FileScan::Csv {
                options: read_options,
                cloud_options,
            },
            cached_ir: Default::default(),
        }
        .into())
    }

    pub fn cache(self) -> Self {
        let input = Arc::new(self.0);
        let id = input.as_ref() as *const DslPlan as usize;
        DslPlan::Cache { input, id }.into()
    }

    pub fn drop(self, to_drop: Vec<Selector>, strict: bool) -> Self {
        self.map_private(DslFunction::Drop(DropFunction { to_drop, strict }))
    }

    pub fn project(self, exprs: Vec<Expr>, options: ProjectionOptions) -> Self {
        DslPlan::Select {
            expr: exprs,
            input: Arc::new(self.0),
            options,
        }
        .into()
    }

    pub fn fill_null(self, fill_value: Expr) -> Self {
        self.project(
            vec![all().fill_null(fill_value)],
            ProjectionOptions {
                duplicate_check: false,
                ..Default::default()
            },
        )
    }

    pub fn drop_nans(self, subset: Option<Vec<Expr>>) -> Self {
        if let Some(subset) = subset {
            self.filter(
                all_horizontal(
                    subset
                        .into_iter()
                        .map(|v| v.is_not_nan())
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            )
        } else {
            self.filter(
                // TODO: when Decimal supports NaN, include here
                all_horizontal([dtype_cols([DataType::Float32, DataType::Float64]).is_not_nan()])
                    .unwrap(),
            )
        }
    }

    pub fn drop_nulls(self, subset: Option<Vec<Expr>>) -> Self {
        if let Some(subset) = subset {
            self.filter(
                all_horizontal(
                    subset
                        .into_iter()
                        .map(|v| v.is_not_null())
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            )
        } else {
            self.filter(all_horizontal([all().is_not_null()]).unwrap())
        }
    }

    pub fn fill_nan(self, fill_value: Expr) -> Self {
        self.map_private(DslFunction::FillNan(fill_value))
    }

    pub fn with_columns(self, exprs: Vec<Expr>, options: ProjectionOptions) -> Self {
        if exprs.is_empty() {
            return self;
        }

        DslPlan::HStack {
            input: Arc::new(self.0),
            exprs,
            options,
        }
        .into()
    }

    pub fn with_context(self, contexts: Vec<DslPlan>) -> Self {
        DslPlan::ExtContext {
            input: Arc::new(self.0),
            contexts,
        }
        .into()
    }

    /// Apply a filter
    pub fn filter(self, predicate: Expr) -> Self {
        DslPlan::Filter {
            predicate,
            input: Arc::new(self.0),
        }
        .into()
    }

    pub fn group_by<E: AsRef<[Expr]>>(
        self,
        keys: Vec<Expr>,
        aggs: E,
        apply: Option<(Arc<dyn DataFrameUdf>, SchemaRef)>,
        maintain_order: bool,
        #[cfg(feature = "dynamic_group_by")] dynamic_options: Option<DynamicGroupOptions>,
        #[cfg(feature = "dynamic_group_by")] rolling_options: Option<RollingGroupOptions>,
    ) -> Self {
        let aggs = aggs.as_ref().to_vec();
        let options = GroupbyOptions {
            #[cfg(feature = "dynamic_group_by")]
            dynamic: dynamic_options,
            #[cfg(feature = "dynamic_group_by")]
            rolling: rolling_options,
            slice: None,
        };

        DslPlan::GroupBy {
            input: Arc::new(self.0),
            keys,
            aggs,
            apply,
            maintain_order,
            options: Arc::new(options),
        }
        .into()
    }

    pub fn build(self) -> DslPlan {
        self.0
    }

    pub fn from_existing_df(df: DataFrame) -> Self {
        let schema = Arc::new(df.schema());
        DslPlan::DataFrameScan {
            df: Arc::new(df),
            schema,
        }
        .into()
    }

    pub fn sort(self, by_column: Vec<Expr>, sort_options: SortMultipleOptions) -> Self {
        DslPlan::Sort {
            input: Arc::new(self.0),
            by_column,
            slice: None,
            sort_options,
        }
        .into()
    }

    pub fn explode(self, columns: Vec<Selector>, allow_empty: bool) -> Self {
        DslPlan::MapFunction {
            input: Arc::new(self.0),
            function: DslFunction::Explode {
                columns,
                allow_empty,
            },
        }
        .into()
    }

    #[cfg(feature = "pivot")]
    pub fn unpivot(self, args: UnpivotArgsDSL) -> Self {
        DslPlan::MapFunction {
            input: Arc::new(self.0),
            function: DslFunction::Unpivot { args },
        }
        .into()
    }

    pub fn row_index(self, name: PlSmallStr, offset: Option<IdxSize>) -> Self {
        DslPlan::MapFunction {
            input: Arc::new(self.0),
            function: DslFunction::RowIndex { name, offset },
        }
        .into()
    }

    pub fn distinct(self, options: DistinctOptionsDSL) -> Self {
        DslPlan::Distinct {
            input: Arc::new(self.0),
            options,
        }
        .into()
    }

    pub fn slice(self, offset: i64, len: IdxSize) -> Self {
        DslPlan::Slice {
            input: Arc::new(self.0),
            offset,
            len,
        }
        .into()
    }

    pub fn join(
        self,
        other: DslPlan,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        options: Arc<JoinOptions>,
    ) -> Self {
        DslPlan::Join {
            input_left: Arc::new(self.0),
            input_right: Arc::new(other),
            left_on,
            right_on,
            predicates: Default::default(),
            options,
        }
        .into()
    }
    pub fn map_private(self, function: DslFunction) -> Self {
        DslPlan::MapFunction {
            input: Arc::new(self.0),
            function,
        }
        .into()
    }

    #[cfg(feature = "python")]
    pub fn map_python(
        self,
        function: PythonFunction,
        optimizations: AllowedOptimizations,
        schema: Option<SchemaRef>,
        validate_output: bool,
    ) -> Self {
        DslPlan::MapFunction {
            input: Arc::new(self.0),
            function: DslFunction::OpaquePython(OpaquePythonUdf {
                function,
                schema,
                predicate_pd: optimizations.contains(OptFlags::PREDICATE_PUSHDOWN),
                projection_pd: optimizations.contains(OptFlags::PROJECTION_PUSHDOWN),
                streamable: optimizations.contains(OptFlags::STREAMING),
                validate_output,
            }),
        }
        .into()
    }

    pub fn map<F>(
        self,
        function: F,
        optimizations: AllowedOptimizations,
        schema: Option<Arc<dyn UdfSchema>>,
        name: PlSmallStr,
    ) -> Self
    where
        F: DataFrameUdf + 'static,
    {
        let function = Arc::new(function);

        DslPlan::MapFunction {
            input: Arc::new(self.0),
            function: DslFunction::FunctionIR(FunctionIR::Opaque {
                function,
                schema,
                predicate_pd: optimizations.contains(OptFlags::PREDICATE_PUSHDOWN),
                projection_pd: optimizations.contains(OptFlags::PROJECTION_PUSHDOWN),
                streamable: optimizations.contains(OptFlags::STREAMING),
                fmt_str: name,
            }),
        }
        .into()
    }
}
