use anyhow::Result;
use std::fs::File;
use std::path::Path;
use csv::Writer;
use time::OffsetDateTime;

use databento::{
    dbn::{CbboMsg, ImbalanceMsg, MboMsg, Mbp10Msg, Mbp1Msg, OhlcvMsg, SType, Schema, StatMsg, TbboMsg, TradeMsg},
    historical::timeseries::GetRangeParams,
    HistoricalClient,
};

pub async fn fetch_and_save_csv(
    mut client: HistoricalClient,
    dataset: &str,
    symbol: &str,
    schema: Schema,
    start: OffsetDateTime,
    end: OffsetDateTime,
) -> Result<String> {
    println!(
        "Fetching {} with schema {:?} for date {} - {}",
        symbol, schema, start, end
    );

    let filename = format!("src/data/{}_{}_{}-{}.csv", symbol, schema, start.date(), end.date());
    if Path::new(&filename).exists() {
        println!("File already exists: {filename}, skipping fetch.");
        return Ok(filename);
    }

    let mut decoder = match dataset {
        "XNAS.ITCH" => client
            .timeseries()
            .get_range(
                &GetRangeParams::builder()
                    .dataset(dataset)
                    .date_time_range((start, end))
                    .symbols(symbol)
                    .schema(schema)
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
                    .schema(schema)
                    .build(),
            )
            .await?,

        _ => return Err(anyhow::anyhow!("Unsupported dataset: {}", dataset)),
    };

    let file = File::create(&filename)?;
    let mut writer = Writer::from_writer(file);

    // Calculate scaling factor
    let scaling_factor = 10.0_f64.powi(-9 as i32); // -9 is typical

    match schema {
        Schema::Ohlcv1S | Schema::Ohlcv1M | Schema::Ohlcv1H | Schema::Ohlcv1D | Schema::OhlcvEod => {
            writer.write_record(&["ts_event", "open", "high", "low", "close", "volume"])?;
            let mut date = start.date();
            
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
    
    writer.flush()?;
    println!("Saved CSV: {filename}");
    Ok(filename)
}
