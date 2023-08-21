use anchor_lang::prelude::*;
use chrono::{DateTime, Utc};

use crate::utils::OPENBOOK_KEY;

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
