use anyhow::{anyhow, Context};
use arrow::array::{FixedSizeBinaryArray, StringArray, UInt64Array, UInt8Array};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;

use crate::config::NautilusIngestConfig;

const KEY_INSTRUMENT_ID: &str = "instrument_id";
const KEY_PRICE_PRECISION: &str = "price_precision";
const KEY_SIZE_PRECISION: &str = "size_precision";
const PRECISION_BYTES: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NautilusDataKind {
    TradeTick,
    QuoteTick,
}

#[derive(Debug, Clone)]
struct InstrumentMeta {
    symbol: String,
    venue: String,
    price_precision: u8,
    size_precision: u8,
}

/// Detect data kind and instrument from Nautilus catalog object key.
pub fn classify_object_key(key: &str) -> Option<NautilusDataKind> {
    let lower = key.to_ascii_lowercase();
    if lower.contains("/data/trades/") {
        Some(NautilusDataKind::TradeTick)
    } else if lower.contains("/data/quotes/") {
        Some(NautilusDataKind::QuoteTick)
    } else {
        None
    }
}

fn instrument_from_path(key: &str, kind: NautilusDataKind) -> Option<String> {
    let marker = match kind {
        NautilusDataKind::TradeTick => "/data/trades/",
        NautilusDataKind::QuoteTick => "/data/quotes/",
    };
    let lower = key.to_ascii_lowercase();
    let idx = lower.find(marker)?;
    let rest = &key[idx + marker.len()..];
    let instrument = rest.split('/').next()?;
    if instrument.is_empty() {
        None
    } else {
        Some(instrument.to_string())
    }
}

fn parse_instrument_id(instrument_id: &str) -> (String, String) {
    match instrument_id.rsplit_once('.') {
        Some((symbol, venue)) => (symbol.to_string(), venue.to_string()),
        None => (instrument_id.to_string(), "UNKNOWN".into()),
    }
}

fn schema_meta(batch: &RecordBatch) -> InstrumentMeta {
    let schema = batch.schema();
    let metadata = schema.metadata();
    let instrument_id = metadata
        .get(KEY_INSTRUMENT_ID)
        .cloned()
        .or_else(|| metadata.get("instrument_id").cloned())
        .unwrap_or_default();

    let (symbol, venue) = if instrument_id.is_empty() {
        ("UNKNOWN".into(), "UNKNOWN".into())
    } else {
        parse_instrument_id(&instrument_id)
    };

    let price_precision = metadata
        .get(KEY_PRICE_PRECISION)
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let size_precision = metadata
        .get(KEY_SIZE_PRECISION)
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);

    InstrumentMeta {
        symbol,
        venue,
        price_precision,
        size_precision,
    }
}

fn fixed_to_f64(bytes: &[u8], precision: u8) -> anyhow::Result<f64> {
    if bytes.len() != PRECISION_BYTES {
        return Err(anyhow!("expected {PRECISION_BYTES}-byte fixed value, got {}", bytes.len()));
    }
    let mut arr = [0u8; PRECISION_BYTES];
    arr.copy_from_slice(bytes);
    let raw = i64::from_le_bytes(arr);
    let scale = 10_f64.powi(i32::from(precision));
    Ok(raw as f64 / scale)
}

fn aggressor_name(value: u8) -> &'static str {
    match value {
        1 => "BUYER",
        2 => "SELLER",
        _ => "NO_AGGRESSOR",
    }
}

fn escape_tag(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace(' ', "\\ ")
        .replace(',', "\\,")
        .replace('=', "\\=")
}

fn escape_field_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn format_trade_line(meta: &InstrumentMeta, batch: &RecordBatch, row: usize, table: &str) -> anyhow::Result<String> {
    let price_col = batch
        .column_by_name("price")
        .context("missing price column")?
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .context("price not fixed binary")?;
    let size_col = batch
        .column_by_name("size")
        .context("missing size column")?
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .context("size not fixed binary")?;
    let side_col = batch
        .column_by_name("aggressor_side")
        .context("missing aggressor_side column")?
        .as_any()
        .downcast_ref::<UInt8Array>()
        .context("aggressor_side not u8")?;
    let trade_id_col = batch
        .column_by_name("trade_id")
        .context("missing trade_id column")?
        .as_any()
        .downcast_ref::<StringArray>()
        .context("trade_id not utf8")?;
    let ts_col = batch
        .column_by_name("ts_event")
        .context("missing ts_event column")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("ts_event not u64")?;

    let price = fixed_to_f64(price_col.value(row), meta.price_precision)?;
    let size = fixed_to_f64(size_col.value(row), meta.size_precision)?;
    let side = aggressor_name(side_col.value(row));
    let trade_id = trade_id_col.value(row);
    let ts = ts_col.value(row);

    let tags = format!(
        "symbol={},venue={}",
        escape_tag(&meta.symbol),
        escape_tag(&meta.venue)
    );
    let fields = format!(
        "price={price},size={size},aggressor_side=\"{}\",trade_id=\"{}\"",
        escape_field_string(side),
        escape_field_string(trade_id)
    );
    Ok(format!("{table},{tags} {fields} {ts}"))
}

fn format_quote_line(meta: &InstrumentMeta, batch: &RecordBatch, row: usize, table: &str) -> anyhow::Result<String> {
    let bid = batch
        .column_by_name("bid_price")
        .context("missing bid_price")?
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .context("bid_price not fixed binary")?;
    let ask = batch
        .column_by_name("ask_price")
        .context("missing ask_price")?
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .context("ask_price not fixed binary")?;
    let bid_size = batch
        .column_by_name("bid_size")
        .context("missing bid_size")?
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .context("bid_size not fixed binary")?;
    let ask_size = batch
        .column_by_name("ask_size")
        .context("missing ask_size")?
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .context("ask_size not fixed binary")?;
    let ts_col = batch
        .column_by_name("ts_event")
        .context("missing ts_event")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("ts_event not u64")?;

    let tags = format!(
        "symbol={},venue={}",
        escape_tag(&meta.symbol),
        escape_tag(&meta.venue)
    );
    let fields = format!(
        "bid={},ask={},bid_size={},ask_size={}",
        fixed_to_f64(bid.value(row), meta.price_precision)?,
        fixed_to_f64(ask.value(row), meta.price_precision)?,
        fixed_to_f64(bid_size.value(row), meta.size_precision)?,
        fixed_to_f64(ask_size.value(row), meta.size_precision)?,
    );
    Ok(format!(
        "{table},{tags} {fields} {}",
        ts_col.value(row)
    ))
}

/// Decode a Nautilus Feather/Arrow IPC stream into ILP lines.
pub fn decode_feather_to_ilp(
    key: &str,
    bytes: &[u8],
    config: &NautilusIngestConfig,
) -> anyhow::Result<(String, Vec<String>)> {
    let kind = classify_object_key(key).ok_or_else(|| anyhow!("unsupported nautilus object key: {key}"))?;

    let cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None).context("invalid arrow ipc stream")?;

    let mut lines = Vec::new();
    let mut table_name = String::new();

    for batch_result in reader {
        let batch = batch_result.context("read record batch")?;
        let mut meta = schema_meta(&batch);

        if meta.symbol == "UNKNOWN" {
            if let Some(instrument) = instrument_from_path(key, kind) {
                let (symbol, venue) = parse_instrument_id(&instrument);
                meta.symbol = symbol;
                meta.venue = venue;
            }
        }

        table_name = match kind {
            NautilusDataKind::TradeTick => config.trade_table.clone(),
            NautilusDataKind::QuoteTick => config.quote_table.clone(),
        };

        for row in 0..batch.num_rows() {
            let line = match kind {
                NautilusDataKind::TradeTick => format_trade_line(&meta, &batch, row, &table_name)?,
                NautilusDataKind::QuoteTick => format_quote_line(&meta, &batch, row, &table_name)?,
            };
            lines.push(line);
        }
    }

    if lines.is_empty() {
        return Err(anyhow!("no rows decoded from {key}"));
    }

    Ok((table_name, lines))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;

    fn write_trade_feather() -> Vec<u8> {
        let metadata = std::collections::HashMap::from([
            (KEY_INSTRUMENT_ID.into(), "BTCUSDT.BITUNIX".into()),
            (KEY_PRICE_PRECISION.into(), "2".into()),
            (KEY_SIZE_PRECISION.into(), "3".into()),
        ]);
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("price", DataType::FixedSizeBinary(PRECISION_BYTES as i32), false),
                Field::new("size", DataType::FixedSizeBinary(PRECISION_BYTES as i32), false),
                Field::new("aggressor_side", DataType::UInt8, false),
                Field::new("trade_id", DataType::Utf8, false),
                Field::new("ts_event", DataType::UInt64, false),
                Field::new("ts_init", DataType::UInt64, false),
            ],
            metadata,
        );

        let price_raw = (50_000_25_i64).to_le_bytes();
        let size_raw = (1_000_i64).to_le_bytes();

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(FixedSizeBinaryArray::try_from_iter(std::iter::once(price_raw.as_slice())).unwrap()),
                Arc::new(FixedSizeBinaryArray::try_from_iter(std::iter::once(size_raw.as_slice())).unwrap()),
                Arc::new(UInt8Array::from(vec![1u8])),
                Arc::new(StringArray::from(vec!["t1"])),
                Arc::new(UInt64Array::from(vec![1_700_000_000_000_000_000u64])),
                Arc::new(UInt64Array::from(vec![1_700_000_000_000_000_001u64])),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        buf
    }

    #[test]
    fn decodes_trade_feather_to_ilp() {
        let bytes = write_trade_feather();
        let key = "live/LIVE/instance/data/trades/BTCUSDT.BITUNIX/chunk.feather";
        let config = NautilusIngestConfig::default();
        let (table, lines) = decode_feather_to_ilp(key, &bytes, &config).unwrap();
        assert_eq!(table, "trade_ticks");
        assert_eq!(lines.len(), 1);
        assert!(lines[0].starts_with("trade_ticks,symbol=BTCUSDT,venue=BITUNIX"));
        assert!(lines[0].contains("price=50000.25"));
        assert!(lines[0].contains("trade_id=\"t1\""));
    }
}
