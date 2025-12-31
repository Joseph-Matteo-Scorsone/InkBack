use crate::event::{FootprintMsg, MarketEvent, OptionTradeMsg};
use crate::InkBackSchema;
use anyhow::{Context, Result};
use csv::Writer;
use databento::dbn::FlagSet;
use databento::{
    dbn::{
        decode::AsyncDbnDecoder, InstrumentDefMsg, MboMsg, Mbp1Msg, OhlcvMsg, RType, RecordHeader,
        SType, Schema, TradeMsg,
    },
    historical::timeseries::GetRangeToFileParams,
    HistoricalClient,
};
use futures::stream::{self, Stream};
use std::collections::{self, HashMap, HashSet};
use std::path::Path;
use std::pin::Pin;
use time::OffsetDateTime;

pub type MarketStream = Pin<Box<dyn Stream<Item = Result<MarketEvent>> + Send>>;

pub async fn get_data_stream(path_str: &str, schema: Schema) -> Result<MarketStream> {
    let path = Path::new(path_str);
    let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("");

    match extension {
        "zst" | "dbn" => {
            let decoder = AsyncDbnDecoder::from_zstd_file(path)
                .await
                .context("Failed to create AsyncDbnDecoder")?;

            // Match based on the Schema to know which struct to decode
            match schema {
                Schema::Trades => {
                    let stream = stream::unfold(decoder, |mut dec| async move {
                        match dec.decode_record::<TradeMsg>().await {
                            Ok(Some(rec)) => Some((Ok(MarketEvent::Trade(rec.clone())), dec)),
                            Ok(None) => None,
                            Err(e) => Some((Err(anyhow::anyhow!(e)), dec)),
                        }
                    });
                    Ok(Box::pin(stream))
                }
                Schema::Mbo => {
                    let stream = stream::unfold(decoder, |mut dec| async move {
                        match dec.decode_record::<MboMsg>().await {
                            Ok(Some(rec)) => Some((Ok(MarketEvent::Mbo(rec.clone())), dec)),
                            Ok(None) => None,
                            Err(e) => Some((Err(anyhow::anyhow!(e)), dec)),
                        }
                    });
                    Ok(Box::pin(stream))
                }
                Schema::Mbp1 => {
                    let stream = stream::unfold(decoder, |mut dec| async move {
                        match dec.decode_record::<Mbp1Msg>().await {
                            Ok(Some(rec)) => Some((Ok(MarketEvent::Mbp1(rec.clone())), dec)),
                            Ok(None) => None,
                            Err(e) => Some((Err(anyhow::anyhow!(e)), dec)),
                        }
                    });
                    Ok(Box::pin(stream))
                }
                Schema::Definition => {
                    let stream = stream::unfold(decoder, |mut dec| async move {
                        match dec.decode_record::<InstrumentDefMsg>().await {
                            Ok(Some(rec)) => Some((Ok(MarketEvent::Definition(rec.clone())), dec)),
                            Ok(None) => None,
                            Err(e) => Some((Err(anyhow::anyhow!(e)), dec)),
                        }
                    });
                    Ok(Box::pin(stream))
                }
                Schema::Ohlcv1S | Schema::Ohlcv1M | Schema::Ohlcv1H | Schema::Ohlcv1D => {
                    let stream = stream::unfold(decoder, |mut dec| async move {
                        match dec.decode_record::<OhlcvMsg>().await {
                            Ok(Some(rec)) => Some((Ok(MarketEvent::Ohlcv(rec.clone())), dec)),
                            Ok(None) => None,
                            Err(e) => Some((Err(anyhow::anyhow!(e)), dec)),
                        }
                    });
                    Ok(Box::pin(stream))
                }
                _ => Err(anyhow::anyhow!(
                    "Schema {:?} not yet supported in get_data_stream",
                    schema
                )),
            }
        }
        "csv" => {
            let file = std::fs::File::open(path)?;
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(true)
                .from_reader(file);

            let headers = reader.headers()?.clone();
            let is_footprint = headers.iter().any(|h| h == "footprint_data");
            let is_merged_options = headers.iter().any(|h| h == "option_type");

            let iter = reader.into_deserialize().map(move |result| {
                let record: std::collections::HashMap<String, String> =
                    result.map_err(|e| anyhow::anyhow!(e))?;

                // Helper for parsing
                let parse_f64 = |key: &str| {
                    record
                        .get(key)
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0)
                };
                let parse_u64 = |key: &str| {
                    record
                        .get(key)
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(0)
                };
                let parse_u32 = |key: &str| {
                    record
                        .get(key)
                        .and_then(|s| s.parse::<u32>().ok())
                        .unwrap_or(0)
                };

                let ts = parse_u64("ts_event");
                if is_merged_options {
                    let event_type = record.get("event_type").map(|s| s.as_str()).unwrap_or("");
                    let und_bid = parse_f64("underlying_bid");
                    let und_ask = parse_f64("underlying_ask");
                    let und_bid_sz = parse_u32("underlying_bid_sz");
                    let und_ask_sz = parse_u32("underlying_ask_sz");

                    if event_type == "OPT" {
                        Ok(MarketEvent::OptionTrade(OptionTradeMsg {
                            ts_event: ts,
                            price: parse_f64("price"),
                            size: parse_u64("size"),
                            instrument_id: parse_u64("instrument_id") as u32,
                            symbol: record.get("symbol").cloned().unwrap_or_default(),
                            strike_price: parse_f64("strike_price"),
                            expiration: parse_u64("expiration"),
                            option_type: record.get("option_type").cloned().unwrap_or_default(),
                            underlying_price: parse_f64("underlying_price"),
                            underlying_bid: und_bid,
                            underlying_ask: und_ask,
                            underlying_bid_sz: und_bid_sz,
                            underlying_ask_sz: und_ask_sz,
                        }))
                    } else {
                        let price_scaled = (parse_f64("price") * 1e9) as i64;
                        let bid_px_scaled = (parse_f64("underlying_bid") * 1e9) as i64;
                        let ask_px_scaled = (parse_f64("underlying_ask") * 1e9) as i64;
                        let size = parse_u64("size") as u32;

                        let mut levels = [databento::dbn::BidAskPair::default()];
                        levels[0] = databento::dbn::BidAskPair {
                            bid_px: bid_px_scaled,
                            ask_px: ask_px_scaled,
                            bid_sz: und_bid_sz,
                            ask_sz: und_ask_sz,
                            bid_ct: 0,
                            ask_ct: 0,
                        };

                        let msg = databento::dbn::Mbp1Msg {
                            hd: RecordHeader::new::<databento::dbn::Mbp1Msg>(
                                RType::Mbp1.into(),
                                0,
                                1,
                                ts,
                            ),
                            action: 0,
                            side: 0,
                            depth: 0,
                            price: price_scaled,
                            size,
                            flags: FlagSet::default(),
                            ts_in_delta: 0,
                            sequence: 0,
                            ts_recv: ts,
                            levels,
                        };
                        Ok(MarketEvent::Mbp1(msg))
                    }
                } else if is_footprint {
                    let footprint_data = record.get("footprint_data").cloned().unwrap_or_default();
                    Ok(MarketEvent::Footprint(FootprintMsg {
                        ts_event: ts,
                        price: parse_f64("close"), // Use close as the price anchor
                        volume: parse_u64("volume"),
                        data: footprint_data,
                    }))
                } else {
                    let msg = databento::dbn::OhlcvMsg {
                        hd: RecordHeader::new::<databento::dbn::OhlcvMsg>(
                            RType::Ohlcv1S.into(),
                            0,
                            1,
                            ts,
                        ),
                        open: (parse_f64("open") * 1e9) as i64,
                        high: (parse_f64("high") * 1e9) as i64,
                        low: (parse_f64("low") * 1e9) as i64,
                        close: (parse_f64("close") * 1e9) as i64,
                        volume: parse_u64("volume"),
                    };
                    Ok(MarketEvent::Ohlcv(msg))
                }
            });

            Ok(Box::pin(stream::iter(iter)))
        }
        _ => Err(anyhow::anyhow!("Unsupported file extension: {}", extension)),
    }
}
#[derive(Clone)]
#[allow(dead_code)]
pub struct BacktestManager {
    pub symbols: HashSet<String>,
    pub schema: Schema,
    pub data_path: String,
}

// Struct to holding Option Definition Data
struct OptionDef {
    symbol: String,
    strike_price: f64,
    expiration: u64,
    option_type: String, // "C" or "P"
}

pub async fn fetch_and_save_data(
    dataset: &str,
    stype_in: SType,
    symbol: &str,
    option_symbol: Option<&str>,
    schema: Schema,
    custom_schema: Option<InkBackSchema>,
    start: OffsetDateTime,
    end: OffsetDateTime,
) -> Result<BacktestManager> {
    let req_schema = if let Some(ref cs) = custom_schema {
        match cs {
            InkBackSchema::FootPrint => Schema::Trades,
            InkBackSchema::CombinedOptionsUnderlying => Schema::Trades,
        }
    } else {
        schema
    };

    println!(
        "Fetching {} with schema {:?} for date {} - {}",
        symbol, req_schema, start, end
    );

    let final_data_path: String = if custom_schema.is_none() {
        // Standard
        let filename = format!(
            "src/data/{}_{}_{}-{}.zst",
            symbol,
            schema,
            start.date(),
            end.date()
        );

        // If file exists, skip request
        if Path::new(&filename).exists() {
            println!("Creating cached Data found at: {}", filename);
            filename
        } else {
            let mut client = HistoricalClient::builder()
                .key_from_env()
                .context("Missing DataBento Key in .env file")?
                .build()
                .context("Failed to build DataBento client")?;

            client
                .timeseries()
                .get_range_to_file(
                    &GetRangeToFileParams::builder()
                        .dataset(dataset)
                        .stype_in(stype_in)
                        .date_time_range((start, end))
                        .symbols(symbol)
                        .schema(schema)
                        .path(&filename)
                        .build(),
                )
                .await?;

            println!("Saved Data (Standard)");
            filename
        }
    } else {
        match custom_schema.unwrap() {
            // Footprint
            InkBackSchema::FootPrint => {
                let filename = format!(
                    "src/data/footprint_{}_{}_{}-{}.zst",
                    symbol,
                    schema,
                    start.date(),
                    end.date()
                );
                let csv_filename = format!(
                    "src/data/footprint_{}_{}_{}-{}.csv",
                    symbol,
                    schema,
                    start.date(),
                    end.date()
                );

                // If the final CSV exists, we are done
                if Path::new(&csv_filename).exists() {
                    println!("Footprint CSV found at: {}", csv_filename);
                    return Ok(BacktestManager {
                        symbols: HashSet::from([symbol.to_string()]),
                        schema: req_schema,
                        data_path: csv_filename,
                    });
                }

                // If CSV is missing but ZST exists, skip download, just process
                if !Path::new(&filename).exists() {
                    // Download ZST
                    let mut client = HistoricalClient::builder()
                        .key_from_env()
                        .context("Missing DataBento Key in .env file")?
                        .build()
                        .context("Failed to build DataBento client")?;

                    client
                        .timeseries()
                        .get_range_to_file(
                            &GetRangeToFileParams::builder()
                                .dataset(dataset)
                                .stype_in(stype_in)
                                .date_time_range((start, end))
                                .symbols(symbol)
                                .schema(Schema::Trades)
                                .path(&filename)
                                .build(),
                        )
                        .await?;
                    println!("Downloaded Raw Footprint Data (ZST)");
                } else {
                    println!("Raw Footprint Data (ZST) found, skipping download.");
                }

                // Process ZST to CSV
                println!("Processing Footprint ZST to CSV...");
                let file = std::fs::File::create(&csv_filename)?;
                let mut writer = Writer::from_writer(file);
                let mut decoder = AsyncDbnDecoder::from_zstd_file(&filename).await.ok();
                let bar_interval_ns = 60_000_000_000u64;

                writer.write_record(&[
                    "ts_event",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "footprint_data",
                ])?;

                let mut current_bar_start: Option<u64> = None;
                let mut current_bar_trades: Vec<TradeMsg> = Vec::new();
                let scaling_factor = 1e-9;

                if let Some(dec) = &mut decoder {
                    while let Ok(Some(msg)) = dec.decode_record::<TradeMsg>().await {
                        let trade_time = msg.ts_recv;
                        let bar_start = (trade_time / bar_interval_ns) * bar_interval_ns;

                        if let Some(prev_bar_start) = current_bar_start {
                            if bar_start != prev_bar_start {
                                let footprint_bar =
                                    process_footprint_bar(&current_bar_trades, scaling_factor);
                                writer.write_record(&[
                                    prev_bar_start.to_string(),
                                    footprint_bar.open.to_string(),
                                    footprint_bar.high.to_string(),
                                    footprint_bar.low.to_string(),
                                    footprint_bar.close.to_string(),
                                    footprint_bar.volume.to_string(),
                                    footprint_bar.footprint_data,
                                ])?;
                                current_bar_trades.clear();
                            }
                        }
                        current_bar_start = Some(bar_start);
                        current_bar_trades.push(msg.clone());
                    }

                    // Process final bar
                    if !current_bar_trades.is_empty() {
                        if let Some(final_bar_start) = current_bar_start {
                            let footprint_bar =
                                process_footprint_bar(&current_bar_trades, scaling_factor);
                            writer.write_record(&[
                                final_bar_start.to_string(),
                                footprint_bar.open.to_string(),
                                footprint_bar.high.to_string(),
                                footprint_bar.low.to_string(),
                                footprint_bar.close.to_string(),
                                footprint_bar.volume.to_string(),
                                footprint_bar.footprint_data,
                            ])?;
                        }
                    }
                }
                writer.flush()?;
                println!("Saved Data (Footprint CSV)");
                csv_filename
            }

            // Options Underlying
            InkBackSchema::CombinedOptionsUnderlying => {
                let underlying_file = format!(
                    "src/data/{}_mbp1_{}-{}.zst",
                    symbol,
                    start.date(),
                    end.date()
                );
                let opt_def_file = format!(
                    "src/data/opt_def_{}_{}-{}.zst",
                    symbol,
                    start.date(),
                    end.date()
                );
                let opt_trades_file = format!(
                    "src/data/opt_trades_{}_{}-{}.zst",
                    symbol,
                    start.date(),
                    end.date()
                );

                let final_merged_csv = format!(
                    "src/data/MERGED_{}_{}-{}.csv",
                    symbol,
                    start.date(),
                    end.date()
                );

                // Check if merged file already exists
                if Path::new(&final_merged_csv).exists() {
                    println!("Merged CSV found at: {}", final_merged_csv);
                    return Ok(BacktestManager {
                        symbols: HashSet::from([symbol.to_string()]),
                        schema,
                        data_path: final_merged_csv,
                    });
                }

                println!("Merged data not found. Starting download and merge process...");

                if !Path::new(&underlying_file).exists() {
                    println!("Downloading Underlying...");
                    let mut client = HistoricalClient::builder().key_from_env()?.build()?;
                    client
                        .timeseries()
                        .get_range_to_file(
                            &GetRangeToFileParams::builder()
                                .dataset(dataset)
                                .stype_in(stype_in)
                                .date_time_range((start, end))
                                .symbols(symbol)
                                .schema(Schema::Mbp1)
                                .path(&underlying_file)
                                .build(),
                        )
                        .await?;
                }

                // Determine Options Dataset
                let options_dataset = match dataset {
                    "GLBX.MDP3" => "GLBX.MDP3",
                    "XNAS.ITCH" | "ARCX.PILLAR" | "BATY.PITCH" => "OPRA.PILLAR",
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Unsupported dataset for options: {}",
                            dataset
                        ))
                    }
                };

                if !Path::new(&opt_def_file).exists() {
                    println!("Downloading Option Definitions...");
                    let mut client = HistoricalClient::builder().key_from_env()?.build()?;
                    client
                        .timeseries()
                        .get_range_to_file(
                            &GetRangeToFileParams::builder()
                                .dataset(options_dataset)
                                .stype_in(SType::Parent)
                                .date_time_range((start, end))
                                .symbols(option_symbol.unwrap())
                                .schema(Schema::Definition)
                                .path(&opt_def_file)
                                .build(),
                        )
                        .await?;
                }

                println!("Building Definition Map...");
                let mut def_map: HashMap<u32, OptionDef> = HashMap::new();
                let mut def_decoder = AsyncDbnDecoder::from_zstd_file(&opt_def_file).await?;

                // We need to collect IDs to request trades
                let mut opt_ids = Vec::new();

                while let Ok(Some(rec)) = def_decoder.decode_record::<InstrumentDefMsg>().await {
                    let inst_id = rec.hd.instrument_id;
                    // Parse instrument_class for C/P
                    let type_char = rec.instrument_class as u8 as char;
                    let opt_type = if type_char == 'C' { "C" } else { "P" }.to_string();

                    let raw_strike = rec.strike_price;
                    let final_strike = if raw_strike == i64::MAX {
                        0.0 // Treat UNDEF as 0.0
                    } else {
                        (raw_strike as f64) * 1e-9
                    };

                    // Extract symbol from raw_symbol array
                    let sym_str = std::str::from_utf8(unsafe {
                        std::slice::from_raw_parts(
                            rec.raw_symbol.as_ptr() as *const u8,
                            rec.raw_symbol.len(),
                        )
                    })
                    .unwrap()
                    .trim_matches(char::from(0))
                    .to_string();

                    def_map.insert(
                        inst_id,
                        OptionDef {
                            symbol: sym_str,
                            strike_price: final_strike,
                            expiration: rec.expiration,
                            option_type: opt_type,
                        },
                    );
                    opt_ids.push(inst_id);
                }

                // Check Options Data File
                if !Path::new(&opt_trades_file).exists() {
                    // Decode Definitions for the download request
                    let mut opt_ids = collections::HashMap::new();
                    let mut def_decoder = AsyncDbnDecoder::from_zstd_file(&opt_def_file).await.ok();
                    if let Some(dec) = &mut def_decoder {
                        while let Ok(Some(msg)) = dec.decode_record::<InstrumentDefMsg>().await {
                            opt_ids.insert(msg.hd.instrument_id, msg.clone());
                        }
                    }
                    if opt_ids.is_empty() {
                        return Err(anyhow::anyhow!("No relevant options found for {}", symbol));
                    }

                    let mut opt_client = HistoricalClient::builder()
                        .key_from_env()
                        .context("Missing DataBento Key")?
                        .build()?;

                    let batch_size = 2_000;
                    let opt_req_ids: Vec<u32> = opt_ids.keys().cloned().collect();
                    for chunk in opt_req_ids.chunks(batch_size) {
                        let ids_vec: Vec<u32> = chunk.to_vec();
                        opt_client
                            .timeseries()
                            .get_range_to_file(
                                &GetRangeToFileParams::builder()
                                    .dataset(options_dataset)
                                    .stype_in(SType::InstrumentId)
                                    .date_time_range((start, end))
                                    .symbols(ids_vec)
                                    .schema(Schema::Trades)
                                    .path(&opt_trades_file)
                                    .build(),
                            )
                            .await?;
                    }
                    println!("Saved Data (Options + Underlying)");
                } else {
                    println!("Options Data found at: {}", opt_trades_file);
                }

                println!("Merging Underlying and Options into CSV...");
                merge_streams_to_csv(
                    &underlying_file,
                    &opt_trades_file,
                    &opt_def_file,
                    &final_merged_csv,
                )
                .await?;

                final_merged_csv
            }
        }
    };

    // Construct the manager
    let backtest_manager = BacktestManager {
        symbols: HashSet::from([symbol.to_string()]),
        schema: req_schema,
        data_path: final_data_path,
    };

    Ok(backtest_manager)
}

async fn merge_streams_to_csv(
    underlying_path: &str,
    options_path: &str,
    def_path: &str,
    output_path: &str,
) -> Result<()> {
    let mut writer = Writer::from_path(output_path)?;

    // Write Header
    writer.write_record(&[
        "ts_event",
        "event_type",
        "instrument_id",
        "symbol",
        "price",
        "size",
        "strike_price",
        "expiration",
        "option_type",
        "underlying_bid",
        "underlying_ask",
        "underlying_bid_sz",
        "underlying_ask_sz",
    ])?;

    // We read the entire definition file first so the map is fully populated
    // before we process a single trade.
    println!("Pre-loading definitions from {}...", def_path);
    let mut def_map: HashMap<u32, OptionDef> = HashMap::new();

    // Scope the decoder so it drops the file handle when done
    {
        let mut def_decoder = AsyncDbnDecoder::from_zstd_file(def_path)
            .await
            .context("Failed to open definition file")?;

        while let Ok(Some(def)) = def_decoder.decode_record::<InstrumentDefMsg>().await {
            let sym_str = std::str::from_utf8(unsafe {
                std::slice::from_raw_parts(
                    def.raw_symbol.as_ptr() as *const u8,
                    def.raw_symbol.len(),
                )
            })
            .unwrap_or("")
            .trim_matches(char::from(0))
            .to_string();

            let type_char = def.instrument_class as u8 as char;
            let opt_type = if type_char == 'C' { "C" } else { "P" }.to_string();

            def_map.insert(
                def.hd.instrument_id,
                OptionDef {
                    symbol: sym_str,
                    strike_price: (def.strike_price as f64) * 1e-9,
                    expiration: def.expiration,
                    option_type: opt_type,
                },
            );
        }
    }
    println!("Loaded {} definitions.", def_map.len());

    let mut und_decoder = AsyncDbnDecoder::from_zstd_file(underlying_path).await.ok();
    let mut opt_decoder = AsyncDbnDecoder::from_zstd_file(options_path).await.ok();

    enum StreamMsg {
        Underlying(Mbp1Msg),
        Option(TradeMsg),
    }

    // streams[0] = Underlying, [1] = Options
    let mut streams: [Option<(u64, StreamMsg)>; 2] = [None, None];

    if let Some(dec) = &mut und_decoder {
        if let Ok(Some(msg)) = dec.decode_record::<Mbp1Msg>().await {
            streams[0] = Some((msg.hd.ts_event, StreamMsg::Underlying(msg.clone())));
        }
    }
    if let Some(dec) = &mut opt_decoder {
        if let Ok(Some(msg)) = dec.decode_record::<TradeMsg>().await {
            streams[1] = Some((msg.hd.ts_event, StreamMsg::Option(msg.clone())));
        }
    }

    // State needed for enrichment
    let mut last_und_bid = 0.0;
    let mut last_und_ask = 0.0;
    let mut last_und_bid_sz = 0;
    let mut last_und_ask_sz = 0;

    println!("Starting Merge...");

    // K-Way Merge Loop
    loop {
        let mut min_ts = u64::MAX;
        let mut min_idx = None;

        for (i, stream_opt) in streams.iter().enumerate() {
            if let Some((ts, _)) = stream_opt {
                if *ts < min_ts {
                    min_ts = *ts;
                    min_idx = Some(i);
                }
            }
        }

        if let Some(idx) = min_idx {
            if let Some((_, msg)) = streams[idx].take() {
                match msg {
                    StreamMsg::Underlying(u) => {
                        let price = (u.price as f64) * 1e-9;
                        if !u.levels.is_empty() {
                            last_und_bid = (u.levels[0].bid_px as f64) * 1e-9;
                            last_und_ask = (u.levels[0].ask_px as f64) * 1e-9;
                            last_und_bid_sz = u.levels[0].bid_sz;
                            last_und_ask_sz = u.levels[0].ask_sz;
                        }
                        writer.write_record(&[
                            u.hd.ts_event.to_string(),
                            "UND".to_string(),
                            "0".to_string(),
                            "UNDERLYING".to_string(),
                            price.to_string(),
                            u.size.to_string(),
                            "".to_string(),
                            "".to_string(),
                            "".to_string(),
                            last_und_bid.to_string(),
                            last_und_ask.to_string(),
                            last_und_bid_sz.to_string(),
                            last_und_ask_sz.to_string(),
                        ])?;
                    }
                    StreamMsg::Option(o) => {
                        // Lookup is now safe because def_map is fully populated
                        if let Some(def) = def_map.get(&o.hd.instrument_id) {
                            let price = (o.price as f64) * 1e-9;
                            writer.write_record(&[
                                o.hd.ts_event.to_string(),
                                "OPT".to_string(),
                                o.hd.instrument_id.to_string(),
                                def.symbol.clone(),
                                price.to_string(),
                                o.size.to_string(),
                                def.strike_price.to_string(),
                                def.expiration.to_string(),
                                def.option_type.clone(),
                                last_und_bid.to_string(),
                                last_und_ask.to_string(),
                                last_und_bid_sz.to_string(),
                                last_und_ask_sz.to_string(),
                            ])?;
                        }
                        // If ID is not in map, we silently skip.
                    }
                }

                // Refill
                match idx {
                    0 => {
                        if let Some(d) = &mut und_decoder {
                            if let Ok(Some(m)) = d.decode_record::<Mbp1Msg>().await {
                                streams[0] =
                                    Some((m.hd.ts_event, StreamMsg::Underlying(m.clone())));
                            }
                        }
                    }
                    1 => {
                        if let Some(d) = &mut opt_decoder {
                            if let Ok(Some(m)) = d.decode_record::<TradeMsg>().await {
                                streams[1] = Some((m.hd.ts_event, StreamMsg::Option(m.clone())));
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
        } else {
            break;
        }
    }

    writer.flush()?;
    Ok(())
}

#[derive(Debug)]
struct FootprintBar {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: u64,
    footprint_data: String,
}

fn process_footprint_bar(trades: &[TradeMsg], scaling_factor: f64) -> FootprintBar {
    use std::collections::HashMap;

    if trades.is_empty() {
        return FootprintBar {
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0,
            footprint_data: "{:.4}".to_string(),
        };
    }

    // Calculate OHLCV
    let first_price = (trades[0].price as f64) * scaling_factor;
    let last_price = (trades[trades.len() - 1].price as f64) * scaling_factor;

    let mut high = first_price;
    let mut low = first_price;
    let mut total_volume = 0u64;

    // Map to store footprint data: price -> (buy_volume, sell_volume)
    let mut footprint_map: HashMap<String, (u64, u64)> = HashMap::new();

    for trade in trades {
        let price = (trade.price as f64) * scaling_factor;
        let size = trade.size;

        // Update OHLC
        if price > high {
            high = price;
        }
        if price < low {
            low = price;
        }
        total_volume += size as u64;

        // Determine if trade is buy or sell
        // In your data, side 66 = 'B' (buy), side 83 = 'S' (sell)
        // side 65 = 'A' (ask/sell), side 78 = 'N' (unknown - we'll ignore)
        let price_key = format!("{:.4}", price);
        let entry = footprint_map.entry(price_key).or_insert((0, 0));

        match trade.side {
            66 => entry.0 += size as u64,      // Buy side
            65 | 83 => entry.1 += size as u64, // Sell side (Ask or Sell)
            _ => {}                            // Ignore other sides (like 'N')
        }
    }

    // Convert footprint map to JSON string
    let footprint_json = serde_json::to_string(&footprint_map).unwrap_or_else(|_| "{}".to_string());

    FootprintBar {
        open: first_price,
        high,
        low,
        close: last_price,
        volume: total_volume,
        footprint_data: footprint_json,
    }
}
