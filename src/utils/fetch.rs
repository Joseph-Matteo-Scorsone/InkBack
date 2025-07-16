use anyhow::Result;
use std::fs::File;
use std::path::Path;
use csv::Writer;
use time::OffsetDateTime;

use databento::{
    dbn::{InstrumentDefMsg, CbboMsg, ImbalanceMsg, MboMsg, Mbp10Msg, Mbp1Msg, OhlcvMsg, SType, Schema, StatMsg, TbboMsg, TradeMsg},
    historical::{timeseries::GetRangeParams, symbology::ResolveParams},
    HistoricalClient,
};

use crate::InkBackSchema;

// Helper function to convert i8 arrays to strings
fn i8_array_to_string(arr: &[i8]) -> String {
    let bytes: Vec<u8> = arr.iter()
        .map(|&b| if b < 0 { 0 } else { b as u8 })
        .collect();
    
    std::str::from_utf8(&bytes)
        .unwrap_or("")
        .trim_end_matches('\0')
        .to_string()
}

// Helper function to convert single i8 to string
fn i8_to_string(val: i8) -> String {
    if val < 0 {
        String::new()
    } else {
        std::str::from_utf8(&[val as u8])
            .unwrap_or("")
            .to_string()
    }
}

pub async fn fetch_and_save_csv(
    mut client: HistoricalClient,
    dataset: &str,
    symbol: &str,
    option_symbol: Option<&str>,
    schema: Schema,
    custom_schema: Option<InkBackSchema>,
    start: OffsetDateTime,
    end: OffsetDateTime,
) -> Result<String> {

    let req_schema = if let Some(ref custom_schema) = custom_schema {
        match custom_schema {
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

    let filename = if let Some(ref custom_schema) = custom_schema {
        match custom_schema {
            InkBackSchema::FootPrint => {
                format!("src/data/{}_FootPrint_{}-{}.csv", symbol, start.date(), end.date())
            },
            InkBackSchema::CombinedOptionsUnderlying => {
                format!("src/data/{}_CombinedOptionsUnderlying_{}-{}.csv", symbol, start.date(), end.date())
            },
        }
    } else {
        format!("src/data/{}_{}_{}-{}.csv", symbol, schema, start.date(), end.date())
    };

    if Path::new(&filename).exists() {
        println!("File already exists: {filename}, skipping fetch.");
        return Ok(filename);
    }

    // if we are not using combined options
    let decoder = match custom_schema {
        Some(InkBackSchema::CombinedOptionsUnderlying) => {
            // Get the decoder later for the combined
            None
        },
        _ => {
            // Initialize decoder for FootPrint or non-custom schemas now
            Some(match dataset {
                "XNAS.ITCH" => client
                    .timeseries()
                    .get_range(
                        &GetRangeParams::builder()
                            .dataset(dataset)
                            .date_time_range((start, end))
                            .symbols(symbol)
                            .schema(req_schema)
                            .build(),
                    )
                    .await?,
                "GLBX.MDP3" => client
                    .timeseries()
                    .get_range(
                        &GetRangeParams::builder()
                            .dataset(dataset)
                            .date_time_range((start, end))
                            .symbols(symbol)
                            .stype_in(SType::Continuous)
                            .schema(req_schema)
                            .build(),
                    )
                    .await?,
                _ => return Err(anyhow::anyhow!("Unsupported dataset: {}", dataset)),
            })
        }
    };
    
    let file = File::create(&filename)?;
    let mut writer = Writer::from_writer(file);

    // Calculate scaling factor
    let scaling_factor = 10.0_f64.powi(-9 as i32); // -9 is typical

    if let Some(ref custom_schema) = custom_schema {
    match custom_schema {
        InkBackSchema::CombinedOptionsUnderlying => {
            println!("Fetching combined options and underlying data...");

            // Determine if we're dealing with futures or equities based on dataset
            let is_futures = matches!(dataset, "GLBX.MDP3" | "DBEQ.MAX" | "IFEU.IMPACT");
            let underlying_id = if is_futures {
                println!("Processing futures options for {}", symbol);
                let resolve_params = ResolveParams::builder()
                    .dataset(dataset)
                    .symbols(vec![symbol.to_string()])
                    .stype_in(SType::Continuous)
                    .stype_out(SType::InstrumentId)
                    .date_range((start.date(), end.date()))
                    .build();
                
                let symbology_result = client.symbology().resolve(&resolve_params).await?;
                
                let front_month_symbol = symbology_result
                    .mappings
                    .get(symbol)
                    .and_then(|mappings| mappings.first())
                    .map(|mapping| &mapping.symbol)
                    .ok_or_else(|| anyhow::anyhow!("Could not resolve front month contract for {}", symbol))?;

                let front_month_id = front_month_symbol.parse::<u32>()
                    .map_err(|_| anyhow::anyhow!("Could not parse instrument ID from symbol: {}", front_month_symbol))?;

                println!("Front month instrument ID: {}", front_month_id);
                front_month_id
            } else {
                println!("Processing equity options for {}", symbol);
                let resolve_params = ResolveParams::builder()
                    .dataset(dataset)
                    .symbols(vec![symbol.to_string()])
                    .stype_in(SType::RawSymbol)
                    .stype_out(SType::InstrumentId)
                    .date_range((start.date(), end.date()))
                    .build();
                
                let symbology_result = client.symbology().resolve(&resolve_params).await?;
                
                let equity_symbol = symbology_result
                    .mappings
                    .get(symbol)
                    .and_then(|mappings| mappings.first())
                    .map(|mapping| &mapping.symbol)
                    .ok_or_else(|| anyhow::anyhow!("Could not resolve instrument ID for equity {}", symbol))?;

                let equity_id = equity_symbol.parse::<u32>()
                    .map_err(|_| anyhow::anyhow!("Could not parse instrument ID from symbol: {}", equity_symbol))?;

                println!("Equity instrument ID: {}", equity_id);
                equity_id
            };

            let options_dataset = match dataset {
                "GLBX.MDP3" => "GLBX.MDP3",
                "XNAS.ITCH" => "OPRA.PILLAR",
                "ARCX.PILLAR" => "OPRA.PILLAR",
                "BATY.PITCH" => "OPRA.PILLAR",
                "BZX.PITCH" => "OPRA.PILLAR",
                "EDGA.PITCH" => "OPRA.PILLAR",
                "EDGX.PITCH" => "OPRA.PILLAR",
                "IEX.TOPS" => "OPRA.PILLAR",
                "LTSE.PITCH" => "OPRA.PILLAR",
                "MEMX.MEMOIR" => "OPRA.PILLAR",
                "MIAX.TOPS" => "OPRA.PILLAR",
                "MPRL.TOPS" => "OPRA.PILLAR",
                "NYSE.PILLAR" => "OPRA.PILLAR",
                _ => return Err(anyhow::anyhow!("Unsupported dataset for options: {}", dataset)),
            };

            println!("Using options dataset: {}", options_dataset);

            // Get option definitions
            println!("Fetching option definitions...");
            let mut opt_def_decoder = client
                .timeseries()
                .get_range(
                    &GetRangeParams::builder()
                        .dataset(options_dataset)
                        .date_time_range((start, end))
                        .symbols(option_symbol.unwrap())
                        .stype_in(SType::Parent)
                        .schema(Schema::Definition)
                        .build(),
                )
                .await?;

            // Collect option definitions and filter for the underlying
            let mut option_definitions = std::collections::HashMap::new();
            let mut relevant_option_ids = std::collections::HashSet::new();

            while let Some(definition) = opt_def_decoder.decode_record::<InstrumentDefMsg>().await? {
                let is_relevant = if is_futures {
                    definition.underlying_id == underlying_id
                } else {
                    let underlying_str = i8_array_to_string(&definition.underlying);
                    underlying_str == symbol
                };

                if is_relevant && (definition.instrument_class == b'C' as i8 || definition.instrument_class == b'P' as i8) {
                    relevant_option_ids.insert(definition.hd.instrument_id);
                    option_definitions.insert(definition.hd.instrument_id, definition.clone());
                }
            }

            println!("Found {} relevant options for underlying {}", relevant_option_ids.len(), symbol);

            if relevant_option_ids.is_empty() {
                return Err(anyhow::anyhow!("No relevant options found for {}", symbol));
            }

            // Get underlying market data
            println!("Fetching underlying market data...");
            let underlying_symbols = if is_futures {
                vec![underlying_id]
            } else {
                vec![]
            };
            
            let mut underlying_decoder = if is_futures {
                client
                    .timeseries()
                    .get_range(
                        &GetRangeParams::builder()
                            .dataset(dataset)
                            .date_time_range((start, end))
                            .symbols(underlying_symbols)
                            .stype_in(SType::InstrumentId)
                            .schema(Schema::Mbp1)
                            .build(),
                    )
                    .await?
            } else {
                client
                    .timeseries()
                    .get_range(
                        &GetRangeParams::builder()
                            .dataset(dataset)
                            .date_time_range((start, end))
                            .symbols(symbol)
                            .stype_in(SType::RawSymbol)
                            .schema(Schema::Mbp1)
                            .build(),
                    )
                    .await?
            };

            // Collect underlying market data with timestamps
            let mut underlying_data = Vec::new();
            while let Some(mbp1) = underlying_decoder.decode_record::<Mbp1Msg>().await? {
                let level = &mbp1.levels[0];
                underlying_data.push((
                    mbp1.hd.ts_event,
                    (level.bid_px as f64) * scaling_factor,
                    (level.ask_px as f64) * scaling_factor,
                ));
            }

            println!("Collected {} underlying data points", underlying_data.len());

            // Sort underlying data by timestamp for binary search
            underlying_data.sort_by_key(|&(ts, _, _)| ts);

            // Batch option trades requests
            println!("Fetching option trades in batches...");
            const BATCH_SIZE: usize = 2000; // API limit
            let mut trades_processed = 0;

            // Create CSV writer
            let file = File::create(&filename)?;
            let mut writer = Writer::from_writer(file);

            // Write header
            writer.write_record(&[
                "ts_event", "rtype", "publisher_id", "instrument_id", "action", "side", "depth", 
                "price", "size", "flags", "ts_in_delta", "sequence", "symbol", "ts_event_def", 
                "rtype_def", "publisher_id_def", "raw_symbol", "security_update_action", 
                "instrument_class", "min_price_increment", "display_factor", "expiration", 
                "activation", "high Bish_limit_price", "low_limit_price", "max_price_variation", 
                "trading_reference_price", "unit_of_measure_qty", "min_price_increment_amount", 
                "price_ratio", "inst_attrib_value", "underlying_id", "raw_instrument_id", 
                "market_depth_implied", "market_depth", "market_segment_id", "max_trade_vol", 
                "min_lot_size", "min_lot_size_block", "min_lot_size_round_lot", "min_trade_vol", 
                "contract_multiplier", "decay_quantity", "original_contract_size", 
                "trading_reference_date", "appl_id", "maturity_year", "decay_start_date", 
                "channel_id", "currency", "settl_currency", "secsubtype", "group", "exchange", 
                "asset", "cfi", "security_type", "unit_of_measure", "underlying", 
                "strike_price_currency", "strike_price", "match_algorithm", 
                "md_security_trading_status", "main_fraction", "price_display_format", 
                "settl_price_type", "sub_fraction", "underlying_product", "maturity_month", 
                "maturity_day", "maturity_week",
                "contract_multiplier_unit", "flow_schedule_type", "tick_rule", "symbol_def", 
                "underlying_bid", "underlying_ask"
            ])?;

            // Process option IDs in batches
            let relevant_option_ids_vec: Vec<u32> = relevant_option_ids.into_iter().collect();
            for chunk in relevant_option_ids_vec.chunks(BATCH_SIZE) {
                println!("Processing batch of {} option IDs", chunk.len());
                let option_ids_vec: Vec<u32> = chunk.to_vec();
                let mut opt_trades_decoder = client
                    .timeseries()
                    .get_range(
                        &GetRangeParams::builder()
                            .dataset(options_dataset)
                            .date_time_range((start, end))
                            .symbols(option_ids_vec)
                            .stype_in(SType::InstrumentId)
                            .schema(Schema::Trades)
                            .build(),
                    )
                    .await?;

                // Process trades in this batch
                while let Some(trade) = opt_trades_decoder.decode_record::<TradeMsg>().await? {
                    if let Some(definition) = option_definitions.get(&trade.hd.instrument_id) {
                        // Find most recent underlying data
                        let (underlying_bid, underlying_ask) = find_most_recent_underlying(&underlying_data, trade.ts_recv);

                        // Create symbol_def from raw_symbol
                        let symbol_def = i8_array_to_string(&definition.raw_symbol);

                        writer.write_record(&[
                            trade.ts_recv.to_string(),
                            "0".to_string(),
                            trade.hd.publisher_id.to_string(),
                            trade.hd.instrument_id.to_string(),
                            i8_to_string(trade.action),
                            i8_to_string(trade.side),
                            "0".to_string(),
                            ((trade.price as f64) * scaling_factor).to_string(),
                            trade.size.to_string(),
                            trade.flags.to_string(),
                            "0".to_string(),
                            trade.sequence.to_string(),
                            trade.hd.instrument_id.to_string(),
                            trade.hd.ts_event.to_string(),
                            "19".to_string(),
                            definition.hd.publisher_id.to_string(),
                            i8_array_to_string(&definition.raw_symbol),
                            i8_to_string(definition.security_update_action),
                            i8_to_string(definition.instrument_class),
                            ((definition.min_price_increment as f64) * scaling_factor).to_string(),
                            definition.display_factor.to_string(),
                            definition.expiration.to_string(),
                            definition.activation.to_string(),
                            ((definition.high_limit_price as f64) * scaling_factor).to_string(),
                            ((definition.low_limit_price as f64) * scaling_factor).to_string(),
                            ((definition.max_price_variation as f64) * scaling_factor).to_string(),
                            ((definition.trading_reference_price as f64) * scaling_factor).to_string(),
                            definition.unit_of_measure_qty.to_string(),
                            definition.min_price_increment_amount.to_string(),
                            definition.price_ratio.to_string(),
                            definition.inst_attrib_value.to_string(),
                            definition.underlying_id.to_string(),
                            definition.hd.instrument_id.to_string(),
                            definition.market_depth_implied.to_string(),
                            definition.market_depth.to_string(),
                            definition.market_segment_id.to_string(),
                            definition.max_trade_vol.to_string(),
                            definition.min_lot_size.to_string(),
                            definition.min_lot_size_block.to_string(),
                            definition.min_lot_size_round_lot.to_string(),
                            definition.min_trade_vol.to_string(),
                            definition.contract_multiplier.to_string(),
                            definition.decay_quantity.to_string(),
                            definition.original_contract_size.to_string(),
                            definition.trading_reference_date.to_string(),
                            definition.appl_id.to_string(),
                            definition.maturity_year.to_string(),
                            definition.decay_start_date.to_string(),
                            definition.channel_id.to_string(),
                            i8_array_to_string(&definition.currency),
                            i8_array_to_string(&definition.settl_currency),
                            i8_array_to_string(&definition.secsubtype),
                            i8_array_to_string(&definition.group),
                            i8_array_to_string(&definition.exchange),
                            i8_array_to_string(&definition.asset),
                            i8_array_to_string(&definition.cfi),
                            i8_array_to_string(&definition.security_type),
                            i8_array_to_string(&definition.unit_of_measure),
                            i8_array_to_string(&definition.underlying),
                            i8_array_to_string(&definition.strike_price_currency),
                            ((definition.strike_price as f64) * scaling_factor).to_string(),
                            i8_to_string(definition.match_algorithm),
                            definition.md_security_trading_status.to_string(),
                            definition.main_fraction.to_string(),
                            definition.price_display_format.to_string(),
                            definition.settl_price_type.to_string(),
                            definition.sub_fraction.to_string(),
                            definition.underlying_product.to_string(),
                            definition.maturity_month.to_string(),
                            definition.maturity_day.to_string(),
                            definition.maturity_week.to_string(),
                            definition.contract_multiplier_unit.to_string(),
                            definition.flow_schedule_type.to_string(),
                            definition.tick_rule.to_string(),
                            symbol_def,
                            underlying_bid.to_string(),
                            underlying_ask.to_string(),
                        ])?;

                        trades_processed += 1;
                    }
                }
            }

            println!("Processed {} option trades", trades_processed);
            writer.flush()?;
        },
        InkBackSchema::FootPrint => {
            
            // Define bar interval (1 minute = 60_000_000_000 nanoseconds)
            let bar_interval_ns = 60_000_000_000u64; // 1 minute
            
            writer.write_record(&["ts_event", "open", "high", "low", "close", "volume", "footprint_data"])?;
            
            let mut current_bar_start: Option<u64> = None;
            let mut current_bar_trades: Vec<TradeMsg> = Vec::new();
            
            let mut decoder = decoder.unwrap();
            while let Some(trade) = decoder.decode_record::<TradeMsg>().await? {
                let trade_time = trade.ts_recv;
                
                // Determine which bar this trade belongs to
                let bar_start = (trade_time / bar_interval_ns) * bar_interval_ns;
                
                // If this is a new bar, process the previous bar
                if let Some(prev_bar_start) = current_bar_start {
                    if bar_start != prev_bar_start {
                        // Process the completed bar
                        if !current_bar_trades.is_empty() {
                            let footprint_bar = process_footprint_bar(&current_bar_trades, scaling_factor);
                            writer.write_record(&[
                                prev_bar_start.to_string(),
                                footprint_bar.open.to_string(),
                                footprint_bar.high.to_string(),
                                footprint_bar.low.to_string(),
                                footprint_bar.close.to_string(),
                                footprint_bar.volume.to_string(),
                                footprint_bar.footprint_data,
                            ])?;
                        }
                        current_bar_trades.clear();
                    }
                }
                
                current_bar_start = Some(bar_start);
                current_bar_trades.push(trade.clone());
            }
            
            // Process the final bar if it has trades
            if !current_bar_trades.is_empty() {
                if let Some(final_bar_start) = current_bar_start {
                    let footprint_bar = process_footprint_bar(&current_bar_trades, scaling_factor);
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
    }

    } else {
            match schema {
                Schema::Ohlcv1S | Schema::Ohlcv1M | Schema::Ohlcv1H | Schema::Ohlcv1D | Schema::OhlcvEod => {
                    writer.write_record(&["ts_event", "open", "high", "low", "close", "volume"])?;
                    let mut date = start.date();
                    
                    let mut decoder = decoder.unwrap();
                    while let Some(ohlcv) = decoder.decode_record::<OhlcvMsg>().await? {
                        writer.write_record(&[
                            date.to_string(),
                            ((ohlcv.open as f64) * scaling_factor).to_string(),
                            ((ohlcv.high as f64) * scaling_factor).to_string(),
                            ((ohlcv.low as f64) * scaling_factor).to_string(),
                            ((ohlcv.close as f64) * scaling_factor).to_string(),
                            ohlcv.volume.to_string(),
                        ])?;

                        date = date.next_day().unwrap();
                    }
                }
                
                Schema::Mbo => {
                    writer.write_record(&["ts_event", "ts_recv", "sequence", "flags", "side", "price", "size", "channel_id", "order_id", "action"])?;
                    
                    let mut decoder = decoder.unwrap();
                    while let Some(mbo) = decoder.decode_record::<MboMsg>().await? {
                        writer.write_record(&[
                            mbo.hd.ts_event.to_string(),
                            mbo.ts_recv.to_string(),
                            mbo.sequence.to_string(),
                            mbo.flags.to_string(),
                            mbo.side.to_string(),
                            ((mbo.price as f64) * scaling_factor).to_string(),
                            mbo.size.to_string(),
                            mbo.channel_id.to_string(),
                            mbo.order_id.to_string(),
                            mbo.action.to_string(),
                        ])?;
                    }
                }
                
                Schema::Mbp1 => {
                    writer.write_record(&["ts_recv", "sequence", "flags", "bid_price", "ask_price", "bid_size", "ask_size", "bid_count", "ask_count"])?;
                    
                    let mut decoder = decoder.unwrap();
                    while let Some(mbp1) = decoder.decode_record::<Mbp1Msg>().await? {
                        // MBP1 has one level, so we take the first (and only) level
                        let level = &mbp1.levels[0];
                        
                        writer.write_record(&[
                            mbp1.ts_recv.to_string(),
                            mbp1.sequence.to_string(),
                            mbp1.flags.to_string(),
                            ((level.bid_px as f64) * scaling_factor).to_string(),
                            ((level.ask_px as f64) * scaling_factor).to_string(),
                            level.bid_sz.to_string(),
                            level.ask_sz.to_string(),
                            level.bid_ct.to_string(),
                            level.ask_ct.to_string(),
                        ])?;
                    }
                }
                
                Schema::Mbp10 => {
                    writer.write_record(&["ts_recv", "sequence", "flags", "level_0_bid_price", "level_0_bid_size", "level_0_bid_count", "level_0_ask_price", "level_0_ask_size", "level_0_ask_count", "level_1_bid_price", "level_1_bid_size", "level_1_bid_count", "level_1_ask_price", "level_1_ask_size", "level_1_ask_count", "level_2_bid_price", "level_2_bid_size", "level_2_bid_count", "level_2_ask_price", "level_2_ask_size", "level_2_ask_count", "level_3_bid_price", "level_3_bid_size", "level_3_bid_count", "level_3_ask_price", "level_3_ask_size", "level_3_ask_count", "level_4_bid_price", "level_4_bid_size", "level_4_bid_count", "level_4_ask_price", "level_4_ask_size", "level_4_ask_count", "level_5_bid_price", "level_5_bid_size", "level_5_bid_count", "level_5_ask_price", "level_5_ask_size", "level_5_ask_count", "level_6_bid_price", "level_6_bid_size", "level_6_bid_count", "level_6_ask_price", "level_6_ask_size", "level_6_ask_count", "level_7_bid_price", "level_7_bid_size", "level_7_bid_count", "level_7_ask_price", "level_7_ask_size", "level_7_ask_count", "level_8_bid_price", "level_8_bid_size", "level_8_bid_count", "level_8_ask_price", "level_8_ask_size", "level_8_ask_count", "level_9_bid_price", "level_9_bid_size", "level_9_bid_count", "level_9_ask_price", "level_9_ask_size", "level_9_ask_count"])?;
                    
                    let mut decoder = decoder.unwrap();
                    while let Some(mbp10) = decoder.decode_record::<Mbp10Msg>().await? {
                        let mut record = vec![
                            mbp10.ts_recv.to_string(),
                            mbp10.sequence.to_string(),
                            mbp10.flags.to_string(),
                        ];
                        
                        // Iterate through all 10 levels (0-9 for bids, 10-19 for asks typically)
                        for i in 0..10 {
                            let level = &mbp10.levels[i];
                            
                            record.push(((level.bid_px as f64) * scaling_factor).to_string());
                            record.push(level.bid_sz.to_string());
                            record.push(level.bid_ct.to_string());
                            record.push(((level.ask_px as f64) * scaling_factor).to_string());
                            record.push(level.ask_sz.to_string());
                            record.push(level.ask_ct.to_string());
                        }
                        
                        writer.write_record(&record)?;
                    }
                }
                
                Schema::Tbbo => {
                    writer.write_record(&["ts_event", "sequence", "flags", "bid_price", "ask_price", "bid_size", "ask_size", "bid_count", "ask_count"])?;
                    
                    let mut decoder = decoder.unwrap();
                    while let Some(tbbo) = decoder.decode_record::<TbboMsg>().await? {

                        let level = &tbbo.levels[0];

                        writer.write_record(&[
                            tbbo.ts_recv.to_string(),
                            tbbo.sequence.to_string(),
                            tbbo.flags.to_string(),
                            ((level.bid_px as f64) * scaling_factor).to_string(),
                            ((level.ask_px as f64) * scaling_factor).to_string(),
                            level.bid_sz.to_string(),
                            level.ask_sz.to_string(),
                            level.bid_ct.to_string(),
                            level.ask_ct.to_string(),
                        ])?;
                    }
                }
                
                Schema::Trades => {
                    writer.write_record(&["ts_event", "sequence", "flags", "price", "size", "action", "side"])?;
                    
                    let mut decoder = decoder.unwrap();
                    while let Some(trade) = decoder.decode_record::<TradeMsg>().await? {
                        writer.write_record(&[
                            trade.ts_recv.to_string(),
                            trade.sequence.to_string(),
                            trade.flags.to_string(),
                            ((trade.price as f64) * scaling_factor).to_string(),
                            trade.size.to_string(),
                            trade.action.to_string(),
                            trade.side.to_string(),
                        ])?;
                    }
                }
                
                Schema::Statistics => {
                    writer.write_record(&["ts_event","sequence", "stat_type", "ts_ref"])?;
                    
                    let mut decoder = decoder.unwrap();
                    while let Some(stat) = decoder.decode_record::<StatMsg>().await? {
                        writer.write_record(&[
                            stat.ts_recv.to_string(),
                            stat.ts_recv.to_string(),
                            stat.sequence.to_string(),
                            stat.stat_type.to_string(),
                            stat.ts_ref.to_string(),
                        ])?;
                    }
                }
                
                Schema::Imbalance => {
                    writer.write_record(&["ts_event",  "ref_price", "cont_book_clr_price", "auct_interest_clr_price", "ssr_filling_price", "ind_match_price", "upper_collar", "lower_collar", "paired_qty", "total_imbalance_qty", "market_imbalance_qty", "unpaired_qty", "auction_type", "side", "auction_status", "freeze_status", "num_extensions", "unpaired_side", "significant_imbalance"])?;
                    
                    let mut decoder = decoder.unwrap();
                    while let Some(imbalance) = decoder.decode_record::<ImbalanceMsg>().await? {
                        writer.write_record(&[
                            imbalance.ts_recv.to_string(),
                            ((imbalance.ref_price as f64) * scaling_factor).to_string(),
                            ((imbalance.cont_book_clr_price as f64) * scaling_factor).to_string(),
                            ((imbalance.auct_interest_clr_price as f64) * scaling_factor).to_string(),
                            ((imbalance.ssr_filling_price as f64) * scaling_factor).to_string(),
                            ((imbalance.ind_match_price as f64) * scaling_factor).to_string(),
                            imbalance.upper_collar.to_string(),
                            imbalance.lower_collar.to_string(),
                            imbalance.paired_qty.to_string(),
                            imbalance.total_imbalance_qty.to_string(),
                            imbalance.market_imbalance_qty.to_string(),
                            imbalance.unpaired_qty.to_string(),
                            imbalance.auction_type.to_string(),
                            imbalance.side.to_string(),
                            imbalance.auction_status.to_string(),
                            imbalance.freeze_status.to_string(),
                            imbalance.num_extensions.to_string(),
                            imbalance.unpaired_side.to_string(),
                            imbalance.significant_imbalance.to_string(),
                        ])?;
                    }
                }
                
                Schema::Cbbo | Schema::Cbbo1S | Schema::Cbbo1M => {
                    writer.write_record(&["ts_event", "sequence", "flags", "bid_price", "ask_price", "bid_size", "ask_size", "bid_pb", "ask_pb"])?;
                    
                    let mut decoder = decoder.unwrap();
                    while let Some(cbbo) = decoder.decode_record::<CbboMsg>().await? {
                        let level = &cbbo.levels[0];

                        writer.write_record(&[
                            cbbo.ts_recv.to_string(),
                            cbbo.sequence.to_string(),
                            cbbo.flags.to_string(),
                            ((level.bid_px as f64) * scaling_factor).to_string(),
                            ((level.ask_px as f64) * scaling_factor).to_string(),
                            level.bid_sz.to_string(),
                            level.ask_sz.to_string(),
                            level.bid_pb.to_string(),
                            level.ask_pb.to_string(),
                        ])?;
                    }
                }
                
                Schema::Tcbbo => {
                    writer.write_record(&["ts_event", "sequence", "flags", "bid_price", "ask_price", "bid_size", "ask_size", "bid_pb", "ask_pb"])?;
                    
                    let mut decoder = decoder.unwrap();
                    while let Some(tcbbo) = decoder.decode_record::<CbboMsg>().await? {
                        let level = &tcbbo.levels[0];

                        writer.write_record(&[
                            tcbbo.ts_recv.to_string(),
                            tcbbo.sequence.to_string(),
                            tcbbo.flags.to_string(),
                            ((level.bid_px as f64) * scaling_factor).to_string(),
                            ((level.ask_px as f64) * scaling_factor).to_string(),
                            level.bid_sz.to_string(),
                            level.ask_sz.to_string(),
                            level.bid_pb.to_string(),
                            level.ask_pb.to_string(),
                        ])?;
                    }
                }
                
                Schema::Bbo1S | Schema::Bbo1M => {
                    writer.write_record(&["ts_event", "sequence", "flags", "bid_price", "ask_price", "bid_size", "ask_size", "bid_pb", "ask_pb"])?;
                    
                    let mut decoder = decoder.unwrap();
                    while let Some(bbo) = decoder.decode_record::<CbboMsg>().await? {
                        let level = &bbo.levels[0];

                        writer.write_record(&[
                            bbo.ts_recv.to_string(),
                            bbo.sequence.to_string(),
                            bbo.flags.to_string(),
                            ((level.bid_px as f64) * scaling_factor).to_string(),
                            ((level.ask_px as f64) * scaling_factor).to_string(),
                            level.bid_sz.to_string(),
                            level.ask_sz.to_string(),
                            level.bid_pb.to_string(),
                            level.ask_pb.to_string(),
                        ])?;
                    }
                }

                Schema::Status | Schema::Definition => {
                    writer.write_record(&["message"])?;
                    writer.write_record(&["Definition schema not fully implemented - requires DefinitionMsg type"])?;
                }
                
            }
        }

    writer.flush()?;
    println!("Saved CSV: {filename}");
    Ok(filename)
}

// Helper function to find most recent underlying data (equivalent to merge_asof)
fn find_most_recent_underlying(underlying_data: &[(u64, f64, f64)], target_time: u64) -> (f64, f64) {
    match underlying_data.binary_search_by_key(&target_time, |&(ts, _, _)| ts) {
        Ok(index) => (underlying_data[index].1, underlying_data[index].2),
        Err(index) => {
            if index == 0 {
                (0.0, 0.0) // No data before this time
            } else {
                (underlying_data[index - 1].1, underlying_data[index - 1].2)
            }
        }
    }
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
            footprint_data: "{}".to_string(),
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
        if price > high { high = price; }
        if price < low { low = price; }
        total_volume += size as u64;
        
        // Determine if trade is buy or sell
        // In your data, side 66 = 'B' (buy), side 83 = 'S' (sell)
        // side 65 = 'A' (ask/sell), side 78 = 'N' (unknown - we'll ignore)
        let price_key = format!("{:.4}", price);
        let entry = footprint_map.entry(price_key).or_insert((0, 0));
        
        match trade.side {
            66 => entry.0 += size as u64, // Buy side
            65 | 83 => entry.1 += size as u64, // Sell side (Ask or Sell)
            _ => {} // Ignore other sides (like 'N')
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
