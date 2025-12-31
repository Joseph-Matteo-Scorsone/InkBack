use databento::dbn::{InstrumentDefMsg, MboMsg, Mbp1Msg, OhlcvMsg, TradeMsg};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Clone)]
pub struct FootprintMsg {
    pub ts_event: u64,
    pub price: f64,
    pub volume: u64,
    pub data: String, // The JSON string
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionTradeMsg {
    pub ts_event: u64,
    pub price: f64,
    pub size: u64,
    pub instrument_id: u32,
    pub symbol: String,
    pub strike_price: f64,
    pub expiration: u64,     // UNIX timestamp in nanoseconds
    pub option_type: String, // "C" or "P"
    pub underlying_bid: f64,
    pub underlying_ask: f64,
    pub underlying_price: f64,
    pub underlying_bid_sz: u32,
    pub underlying_ask_sz: u32,
}

#[derive(Debug, Clone)]
pub enum MarketEvent {
    Trade(TradeMsg),
    Mbp1(Mbp1Msg),
    Ohlcv(OhlcvMsg),
    Mbo(MboMsg),
    Footprint(FootprintMsg),
    OptionTrade(OptionTradeMsg),
    Definition(InstrumentDefMsg),
}

impl MarketEvent {
    pub fn price(&self) -> f64 {
        const SCALE: f64 = 1e-9;
        match self {
            MarketEvent::Trade(m) => m.price as f64 * SCALE,
            MarketEvent::Mbp1(m) => m.price as f64 * SCALE,
            MarketEvent::Ohlcv(m) => m.close as f64 * SCALE,
            MarketEvent::Mbo(m) => m.price as f64 * SCALE,
            MarketEvent::Footprint(m) => m.price,
            MarketEvent::OptionTrade(m) => m.price,
            MarketEvent::Definition(_) => todo!(),
        }
    }

    pub fn volume(&self) -> u64 {
        match self {
            MarketEvent::Trade(m) => m.size as u64,
            MarketEvent::Mbp1(m) => m.size as u64,
            MarketEvent::Ohlcv(m) => m.volume,
            MarketEvent::Mbo(m) => m.size as u64,
            MarketEvent::Footprint(m) => m.volume,
            MarketEvent::OptionTrade(m) => m.size,
            MarketEvent::Definition(_) => todo!(),
        }
    }

    pub fn high(&self) -> f64 {
        const SCALE: f64 = 1e-9;
        match self {
            MarketEvent::Ohlcv(m) => m.high as f64 * SCALE,
            _ => self.price(),
        }
    }

    pub fn low(&self) -> f64 {
        const SCALE: f64 = 1e-9;
        match self {
            MarketEvent::Ohlcv(m) => m.low as f64 * SCALE,
            _ => self.price(),
        }
    }

    pub fn timestamp(&self) -> u64 {
        match self {
            MarketEvent::Trade(m) => m.hd.ts_event,
            MarketEvent::Mbp1(m) => m.hd.ts_event,
            MarketEvent::Ohlcv(m) => m.hd.ts_event,
            MarketEvent::Mbo(m) => m.hd.ts_event,
            MarketEvent::Footprint(m) => m.ts_event,
            MarketEvent::OptionTrade(m) => m.ts_event,
            MarketEvent::Definition(m) => m.hd.ts_event,
        }
    }

    pub fn date_string(&self) -> String {
        let ts = self.timestamp();
        match OffsetDateTime::from_unix_timestamp_nanos(ts as i128) {
            Ok(odt) => odt.date().to_string(),
            Err(_) => "UNKNOWN".to_string(),
        }
    }

    // Helper to get underlying quotes to MBP1 and OptionTrade
    #[allow(dead_code)]
    pub fn get(&self, key: &str) -> Option<f64> {
        const SCALE: f64 = 1e-9;
        match self {
            // If it's an MBP1 message, Underlying
            MarketEvent::Mbp1(msg) => match key {
                "underlying_bid" => Some(msg.levels[0].bid_px as f64 * SCALE),
                "underlying_ask" => Some(msg.levels[0].ask_px as f64 * SCALE),
                "underlying_price" => Some(msg.price as f64 * SCALE),
                _ => None,
            },
            MarketEvent::OptionTrade(msg) => match key {
                "strike_price" => Some(msg.strike_price),
                "underlying_price" => Some(msg.underlying_price),
                "underlying_bid" => Some(msg.underlying_bid),
                "underlying_ask" => Some(msg.underlying_ask),
                "price" => Some(msg.price),
                _ => None,
            },
            _ => None,
        }
    }

    /// Generic getter for u64 fields
    #[allow(dead_code)]
    pub fn get_u64(&self, key: &str) -> Option<u64> {
        match self {
            MarketEvent::Mbp1(msg) => match key {
                "underlying_bid_sz" => Some(msg.levels[0].bid_sz as u64),
                "underlying_ask_sz" => Some(msg.levels[0].ask_sz as u64),
                _ => None,
            },
            MarketEvent::OptionTrade(msg) => match key {
                "expiration" => Some(msg.expiration),
                "instrument_id" => Some(msg.instrument_id as u64),
                "underlying_bid_sz" => Some(msg.underlying_bid_sz as u64),
                "underlying_ask_sz" => Some(msg.underlying_ask_sz as u64),
                _ => None,
            },
            _ => None,
        }
    }

    /// Generic getter for string fields
    pub fn get_string(&self, key: &str) -> Option<String> {
        match self {
            MarketEvent::Footprint(msg) if key == "footprint_data" => Some(msg.data.clone()),
            MarketEvent::OptionTrade(msg) => match key {
                "instrument_class" | "option_type" => Some(msg.option_type.clone()),
                "symbol" => Some(msg.symbol.clone()),
                _ => None,
            },
            _ => None,
        }
    }
}
