use anyhow::Result;
use csv::Reader;
use crate::strategy::Candle;
use databento::dbn::Schema;
use std::collections::HashMap;

#[allow(dead_code)]
/// Converts CSV files into dynamic Candle structs for any Schema
pub trait SchemaHandler {
    /// Convert a CSV at `csv_path` into a vector of dynamic Candles
    fn csv_to_candles(&self, csv_path: &str) -> Result<Vec<Candle>>;
    /// Return the Schema this handler is configured for
    fn schema(&self) -> Schema;
}

/// A generic handler that treats any CSV with a header row
/// as timestamps + numeric fields + string fields.
pub struct GenericCsvHandler(pub Schema);

impl SchemaHandler for GenericCsvHandler {
    fn csv_to_candles(&self, csv_path: &str) -> Result<Vec<Candle>> {
        let mut rdr = Reader::from_path(csv_path)?;
        let headers = rdr.headers()?.clone();
        let mut candles = Vec::new();

        for record in rdr.records() {
            let rec = record?;
            // first column is timestamp/date
            let date = rec.get(0).unwrap_or(&"").to_string();
            let mut fields: HashMap<String, f64> = HashMap::with_capacity(headers.len() - 1);
            let mut string_fields: HashMap<String, String> = HashMap::new();

            // parse all other columns - try as f64 first, then store as string
            for (i, header) in headers.iter().enumerate().skip(1) {
                if let Some(val_str) = rec.get(i) {
                    if let Ok(val) = val_str.parse::<f64>() {
                        fields.insert(header.to_string(), val);
                    } else {
                        // Store as string if it can't be parsed as f64
                        string_fields.insert(header.to_string(), val_str.to_string());
                    }
                }
            }

            candles.push(Candle { date, fields, string_fields });
        }

        Ok(candles)
    }

    fn schema(&self) -> Schema {
        // now a true instance method, so `self.0` is in scope
        self.0
    }
}

/// Factory returns a GenericCsvHandler for any schema.
pub fn get_schema_handler(schema: Schema) -> Box<dyn SchemaHandler> {
    Box::new(GenericCsvHandler(schema))
}
