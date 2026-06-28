use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use datafusion::error::{DataFusionError, Result as DfResult};
use pgwire::api::portal::Format;
use pgwire::api::results::{FieldInfo, QueryResponse, Response};
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;
use postgres_types::{Kind, Type};

use crate::config::ColumnConfig;

use super::FederatedError;

/// Map a config column type name to Arrow (QuestDB dialect).
pub fn config_type_to_arrow(type_name: &str) -> anyhow::Result<DataType> {
    match type_name.to_ascii_lowercase().as_str() {
        "symbol" | "string" | "varchar" | "text" | "char" => Ok(DataType::Utf8),
        "double" | "float8" => Ok(DataType::Float64),
        "float" | "float4" | "real" => Ok(DataType::Float32),
        "long" | "int8" | "bigint" => Ok(DataType::Int64),
        "int" | "int4" | "integer" => Ok(DataType::Int32),
        "short" | "int2" | "smallint" => Ok(DataType::Int16),
        "boolean" | "bool" => Ok(DataType::Boolean),
        "timestamp" | "datetime" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "timestamptz" => Ok(DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))),
        "date" => Ok(DataType::Date32),
        "binary" | "bytea" => Ok(DataType::Binary),
        other => anyhow::bail!("unsupported column type: {other}"),
    }
}

pub fn schema_from_columns(columns: &[ColumnConfig]) -> anyhow::Result<Arc<Schema>> {
    if columns.is_empty() {
        anyhow::bail!("table must declare at least one column");
    }
    let arrow_fields: Vec<Field> = columns
        .iter()
        .map(|c| {
            Ok(Field::new(
                c.name.clone(),
                config_type_to_arrow(&c.type_name)?,
                true,
            ))
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(Arc::new(Schema::new(arrow_fields)))
}

/// Map a PostgreSQL column type to the Arrow type used in federated batches.
pub fn pg_type_to_arrow(pg_type: &Type) -> DataType {
    if let Kind::Array(elem) = pg_type.kind() {
        return DataType::List(Arc::new(Field::new(
            "item",
            pg_type_to_arrow(elem),
            true,
        )));
    }

    match *pg_type {
        Type::BOOL => DataType::Boolean,
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        Type::NUMERIC => DataType::Float64,
        Type::DATE => DataType::Date32,
        Type::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
        Type::TIMESTAMPTZ => DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        Type::BYTEA => DataType::Binary,
        _ => DataType::Utf8,
    }
}

pub fn field_infos_to_arrow_schema(fields: &[FieldInfo]) -> Arc<Schema> {
    let arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| Field::new(f.name(), pg_type_to_arrow(f.datatype()), true))
        .collect();
    Arc::new(Schema::new(arrow_fields))
}

pub fn decode_row_cells(row: &DataRow, col_count: usize) -> Vec<Option<String>> {
    use bytes::Buf;
    let mut buf = row.data.as_ref();
    let mut cells = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        if buf.remaining() < 4 {
            cells.push(None);
            continue;
        }
        let len = buf.get_i32();
        if len < 0 {
            cells.push(None);
            continue;
        }
        if buf.remaining() < len as usize {
            cells.push(None);
            continue;
        }
        let bytes = buf.copy_to_bytes(len as usize);
        cells.push(
            std::str::from_utf8(&bytes)
                .ok()
                .map(|s| s.to_string()),
        );
    }
    cells
}

pub fn rows_to_typed_batch(
    fields: &[FieldInfo],
    rows: &[Vec<Option<String>>],
) -> DfResult<RecordBatch> {
    rows_to_typed_batch_for_schema(&field_infos_to_arrow_schema(fields), fields, rows)
}

/// Build a batch whose columns match `target_schema` order, mapping PG columns by name.
pub fn rows_to_typed_batch_for_schema(
    target_schema: &Schema,
    pg_fields: &[FieldInfo],
    rows: &[Vec<Option<String>>],
) -> DfResult<RecordBatch> {
    use arrow::record_batch::RecordBatch;
    use std::collections::HashMap;

    let pg_index: HashMap<String, usize> = pg_fields
        .iter()
        .enumerate()
        .map(|(idx, field)| (field.name().to_ascii_lowercase(), idx))
        .collect();

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for target_field in target_schema.fields() {
        let pg_idx = pg_index.get(&target_field.name().to_ascii_lowercase());
        let pg_field = pg_idx.and_then(|&idx| pg_fields.get(idx));
        let data_type = target_field.data_type();
        let values: Vec<Option<&str>> = rows
            .iter()
            .map(|row| {
                pg_idx
                    .and_then(|&idx| row.get(idx))
                    .and_then(|v| v.as_deref())
            })
            .collect();
        arrays.push(build_array(
            pg_field
                .map(|f| pg_type_to_arrow(f.datatype()))
                .as_ref()
                .unwrap_or(data_type),
            &values,
        )?);
    }

    RecordBatch::try_new(Arc::new(target_schema.clone()), arrays)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn build_array(data_type: &DataType, values: &[Option<&str>]) -> DfResult<ArrayRef> {
    match data_type {
        DataType::Boolean => {
            let parsed: Vec<Option<bool>> = values.iter().map(|v| v.and_then(parse_bool)).collect();
            Ok(Arc::new(BooleanArray::from(parsed)) as ArrayRef)
        }
        DataType::Int16 => {
            let parsed: Vec<Option<i16>> = values.iter().map(|v| v.and_then(|s| s.parse().ok())).collect();
            Ok(Arc::new(Int16Array::from(parsed)) as ArrayRef)
        }
        DataType::Int32 => {
            let parsed: Vec<Option<i32>> = values.iter().map(|v| v.and_then(|s| s.parse().ok())).collect();
            Ok(Arc::new(Int32Array::from(parsed)) as ArrayRef)
        }
        DataType::Int64 => {
            let parsed: Vec<Option<i64>> = values.iter().map(|v| v.and_then(|s| s.parse().ok())).collect();
            Ok(Arc::new(Int64Array::from(parsed)) as ArrayRef)
        }
        DataType::Float32 => {
            let parsed: Vec<Option<f32>> = values.iter().map(|v| v.and_then(|s| s.parse().ok())).collect();
            Ok(Arc::new(Float32Array::from(parsed)) as ArrayRef)
        }
        DataType::Float64 => {
            let parsed: Vec<Option<f64>> = values.iter().map(|v| v.and_then(|s| s.parse().ok())).collect();
            Ok(Arc::new(Float64Array::from(parsed)) as ArrayRef)
        }
        DataType::Date32 => {
            let parsed: Vec<Option<i32>> = values.iter().map(|v| v.and_then(parse_date32)).collect();
            Ok(Arc::new(Date32Array::from(parsed)) as ArrayRef)
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let parsed: Vec<Option<i64>> = values
                .iter()
                .map(|v| v.and_then(|s| parse_timestamp_micros(s, tz.is_some())))
                .collect();
            Ok(Arc::new(TimestampMicrosecondArray::from(parsed)) as ArrayRef)
        }
        DataType::Binary => {
            let owned: Vec<Option<Vec<u8>>> =
                values.iter().map(|v| v.and_then(|s| parse_bytea(s))).collect();
            let refs: Vec<Option<&[u8]>> = owned.iter().map(|v| v.as_deref()).collect();
            Ok(Arc::new(arrow::array::BinaryArray::from(refs)) as ArrayRef)
        }
        DataType::List(_) | DataType::LargeList(_) => {
            // QuestDB array columns are uncommon; keep text for now.
            Ok(Arc::new(StringArray::from(values.to_vec())) as ArrayRef)
        }
        _ => Ok(Arc::new(StringArray::from(values.to_vec())) as ArrayRef),
    }
}

fn parse_bool(s: &str) -> Option<bool> {
    match s {
        "t" | "true" | "TRUE" | "1" => Some(true),
        "f" | "false" | "FALSE" | "0" => Some(false),
        _ => s.parse().ok(),
    }
}

fn parse_date32(s: &str) -> Option<i32> {
    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    Some(date.signed_duration_since(epoch).num_days() as i32)
}

fn parse_timestamp_micros(s: &str, has_tz: bool) -> Option<i64> {
    if has_tz {
        DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc).timestamp_micros())
    } else if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        Some(dt.naive_utc().and_utc().timestamp_micros())
    } else if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        Some(ndt.and_utc().timestamp_micros())
    } else if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        Some(ndt.and_utc().timestamp_micros())
    } else {
        s.parse::<i64>().ok()
    }
}

fn parse_bytea(s: &str) -> Option<Vec<u8>> {
    if let Some(hex) = s.strip_prefix("\\x") {
        return hex::decode(hex).ok();
    }
    Some(s.as_bytes().to_vec())
}

pub fn batches_to_pg_responses(batches: Vec<arrow::record_batch::RecordBatch>) -> PgWireResult<Vec<Response>> {
    use arrow_pg::datatypes::{arrow_schema_to_pg_fields, encode_recordbatch};

    if batches.is_empty() {
        return Ok(vec![Response::EmptyQuery]);
    }

    let schema = batches[0].schema();
    let fields = arrow_schema_to_pg_fields(schema.as_ref(), &Format::UnifiedText, None)
        .map_err(|e| {
            FederatedError::SchemaMismatch(e.to_string()).to_pgwire()
        })?;
    let fields_arc = Arc::new(fields);

    let mut rows = Vec::new();
    for batch in batches {
        for row_result in encode_recordbatch(fields_arc.clone(), batch) {
            rows.push(row_result.map_err(|e| {
                FederatedError::ShardQuery {
                    shard_id: 0,
                    message: e.to_string(),
                }
                .to_pgwire()
            })?);
        }
    }

    Ok(vec![Response::Query(QueryResponse::new(
        fields_arc,
        futures::stream::iter(rows.into_iter().map(Ok)),
    ))])
}

use arrow::record_batch::RecordBatch;

#[cfg(test)]
mod tests {
    use super::*;
    use pgwire::api::results::FieldFormat;

    #[test]
    fn pg_type_maps_double_to_float64() {
        assert_eq!(pg_type_to_arrow(&Type::FLOAT8), DataType::Float64);
    }

    #[test]
    fn pg_type_maps_timestamp() {
        assert_eq!(
            pg_type_to_arrow(&Type::TIMESTAMP),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn typed_batch_preserves_float_columns() {
        let fields = vec![
            FieldInfo::new(
                "open".into(),
                None,
                None,
                Type::FLOAT8,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "symbol".into(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            ),
        ];
        let rows = vec![
            vec![Some("100.5".into()), Some("BTC".into())],
            vec![Some("200.25".into()), Some("ETH".into())],
        ];
        let batch = rows_to_typed_batch(&fields, &rows).unwrap();
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 100.5).abs() < f64::EPSILON);
    }

    #[test]
    fn typed_batch_aligns_columns_by_name() {
        let pg_fields = vec![
            FieldInfo::new("symbol".into(), None, None, Type::VARCHAR, FieldFormat::Text),
            FieldInfo::new("interval".into(), None, None, Type::VARCHAR, FieldFormat::Text),
            FieldInfo::new("volume".into(), None, None, Type::FLOAT8, FieldFormat::Text),
            FieldInfo::new("ts".into(), None, None, Type::TIMESTAMP, FieldFormat::Text),
        ];
        let target = field_infos_to_arrow_schema(&[
            FieldInfo::new("symbol".into(), None, None, Type::VARCHAR, FieldFormat::Text),
            FieldInfo::new("volume".into(), None, None, Type::FLOAT8, FieldFormat::Text),
            FieldInfo::new("ts".into(), None, None, Type::TIMESTAMP, FieldFormat::Text),
        ]);
        let rows = vec![vec![
            Some("BTC".into()),
            Some("1m".into()),
            Some("42.5".into()),
            Some("2026-06-22T00:00:00".into()),
        ]];
        let batch = rows_to_typed_batch_for_schema(target.as_ref(), &pg_fields, &rows).unwrap();
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(1).name(), "volume");
        let volume = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((volume.value(0) - 42.5).abs() < f64::EPSILON);
    }
}
