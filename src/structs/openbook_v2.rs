use crate::utils::{to_timestampz, OPENBOOK_KEY};
use anchor_lang::prelude::*;
use chrono::{DateTime, Utc};
use num_traits::Pow;
use tokio_postgres::Row;

pub const FEES_SCALE_FACTOR: i128 = 1_000_000;

#[derive(Clone, Debug, PartialEq)]
pub struct OpenBookMarketMetadata {
    pub creation_datetime: DateTime<Utc>,
    pub program_pk: String,
    pub market_pk: String,
    pub market_name: String,
    pub base_mint: String,
    pub quote_mint: String,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub base_lot_size: i64,
    pub quote_lot_size: i64,
    pub scraper_active: bool,
}
impl OpenBookMarketMetadata {
    pub fn from_event(log: MarketMetaDataLog, block_datetime: DateTime<Utc>) -> Self {
        OpenBookMarketMetadata {
            creation_datetime: block_datetime,
            program_pk: OPENBOOK_KEY.to_string(),
            market_pk: log.market.to_string(),
            market_name: log.market.to_string(),
            base_mint: log.base_mint.to_string(),
            quote_mint: log.quote_mint.to_string(),
            base_decimals: log.base_decimals,
            quote_decimals: log.quote_decimals,
            base_lot_size: log.base_lot_size,
            quote_lot_size: log.quote_lot_size,
            scraper_active: false,
        }
    }

    pub fn from_row(row: Row) -> Self {
        let base_decimals_raw = row.get::<usize, i32>(6);
        let quote_decimals_raw = row.get::<usize, i32>(7);
        OpenBookMarketMetadata {
            creation_datetime: row.get(0),
            program_pk: row.get(1),
            market_pk: row.get(2),
            market_name: row.get(3),
            base_mint: row.get(4),
            quote_mint: row.get(5),
            base_decimals: base_decimals_raw as u8,
            quote_decimals: quote_decimals_raw as u8,
            base_lot_size: row.get(8),
            quote_lot_size: row.get(9),
            scraper_active: row.get(10),
        }
    }
}

#[event]
pub struct MarketMetaDataLog {
    pub market: Pubkey,
    pub name: String,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub base_lot_size: i64,
    pub quote_lot_size: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OpenBookFill {
    pub block_datetime: DateTime<Utc>,
    pub slot: u64,
    pub market_pk: String,

    pub seq_num: u64, // note: usize same as u64
    pub maker: String,
    pub maker_client_order_id: u64,
    pub maker_fee: f64,

    // Timestamp of when the maker order was placed
    pub maker_datetime: DateTime<Utc>,

    pub taker: String,
    pub taker_client_order_id: u64,
    pub taker_fee: f64,

    pub taker_side: u8, // side from the taker's POV
    pub maker_slot: u8,
    pub maker_out: bool, // true if maker order quantity == 0

    pub price: f64,
    pub quantity: f64, // number of base lots
}

impl OpenBookFill {
    pub fn from_log(
        log: FillLog,
        market: &OpenBookMarketMetadata,
        slot: u64,
        block_datetime: DateTime<Utc>,
    ) -> Self {
        let match_quote = log.quantity / log.price * market.quote_lot_size;
        let maker_fees_quote_lots = match_quote * (log.maker_fee) / (FEES_SCALE_FACTOR as i64);
        let taker_fees_quote_lots = match_quote * (log.taker_fee) / (FEES_SCALE_FACTOR as i64);

        OpenBookFill {
            block_datetime,
            slot,
            market_pk: log.market.to_string(),
            seq_num: log.seq_num,
            maker: log.maker.to_string(),
            maker_client_order_id: log.maker_client_order_id,
            maker_fee: ui_quote_quantity(maker_fees_quote_lots, &market),
            maker_datetime: to_timestampz(log.maker_timestamp),
            taker: log.taker.to_string(),
            taker_client_order_id: log.taker_client_order_id,
            taker_fee: ui_quote_quantity(taker_fees_quote_lots, &market),
            taker_side: log.taker_side,
            maker_slot: log.maker_slot,
            maker_out: log.maker_out,
            price: ui_price(log.price, &market),
            quantity: ui_base_quantity(log.quantity, &market),
        }
    }

    pub fn from_row(row: Row) -> Self {
        let slot_raw = row.get::<usize, i32>(1);
        let seq_num_raw = row.get::<usize, i32>(3);
        let maker_client_order_id_raw: String = row.get(4);
        let taker_client_order_id_raw: String = row.get(9);
        let taker_side_raw = row.get::<usize, i32>(11);
        let maker_slot_raw = row.get::<usize, i32>(12);

        OpenBookFill {
            block_datetime: row.get(0),
            slot: slot_raw as u64,
            market_pk: row.get(2),
            seq_num: seq_num_raw as u64,
            maker: row.get(4),
            maker_client_order_id: maker_client_order_id_raw.parse::<u64>().unwrap(),
            maker_fee: row.get(6),
            maker_datetime: row.get(7),
            taker: row.get(8),
            taker_client_order_id: taker_client_order_id_raw.parse::<u64>().unwrap(),
            taker_fee: row.get(10),
            taker_side: taker_side_raw as u8,
            maker_slot: maker_slot_raw as u8,
            maker_out: row.get(13),
            price: row.get(14),
            quantity: row.get(15),
        }
    }
}

#[event]
#[derive(Debug)]
pub struct FillLog {
    pub market: Pubkey,
    pub taker_side: u8, // side from the taker's POV
    pub maker_slot: u8,
    pub maker_out: bool, // true if maker order quantity == 0
    pub timestamp: u64,
    pub seq_num: u64, // note: usize same as u64

    pub maker: Pubkey,
    pub maker_client_order_id: u64,
    pub maker_fee: i64, // quote native

    // Timestamp of when the maker order was placed; copied over from the LeafNode
    pub maker_timestamp: u64,

    pub taker: Pubkey,
    pub taker_client_order_id: u64,
    pub taker_fee: i64, // quote native

    pub price: i64,
    pub quantity: i64, // number of base lots
}

pub fn ui_price(price: i64, market: &OpenBookMarketMetadata) -> f64 {
    let price_lots = price as f64;
    let base_multiplier = token_factor(market.base_decimals);
    let quote_multiplier = token_factor(market.quote_decimals);
    let base_lot_size = market.base_lot_size as f64;
    let quote_lot_size = market.quote_lot_size as f64;
    (price_lots * quote_lot_size * base_multiplier) / (base_lot_size * quote_multiplier)
}

pub fn ui_base_quantity(quantity: i64, market: &OpenBookMarketMetadata) -> f64 {
    let base_lot_size = market.base_lot_size as f64;
    let base_multiplier = token_factor(market.base_decimals);
    quantity as f64 * base_lot_size / base_multiplier
}

pub fn ui_quote_quantity(quantity: i64, market: &OpenBookMarketMetadata) -> f64 {
    let quote_lot_size = market.quote_lot_size as f64;
    let quote_multiplier = token_factor(market.quote_decimals);
    quantity as f64 * quote_lot_size / quote_multiplier
}

pub fn token_factor(decimals: u8) -> f64 {
    10f64.pow(decimals as f64)
}
