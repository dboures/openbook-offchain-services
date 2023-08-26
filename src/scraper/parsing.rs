use log::warn;
use solana_client::client_error::Result as ClientResult;
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedConfirmedTransactionWithStatusMeta, UiInstruction,
    UiParsedInstruction, UiTransactionStatusMeta,
};
use std::{collections::HashMap, io::Error};

use crate::{
    structs::openbook_v2::{FillLog, MarketMetaDataLog, OpenBookFill, OpenBookMarketMetadata},
    utils::to_timestampz,
    worker::metrics::METRIC_RPC_ERRORS_TOTAL,
};

const PROGRAM_DATA: &str = "Program data: ";

pub fn parse_openbook_txns(
    txns: &mut Vec<ClientResult<EncodedConfirmedTransactionWithStatusMeta>>,
    mut sig_strings: Vec<String>,
    target_markets: &HashMap<String, OpenBookMarketMetadata>,
) -> (Vec<OpenBookFill>, Vec<OpenBookMarketMetadata>, Vec<String>) {
    let mut fills_vector = Vec::<OpenBookFill>::new();
    let mut markets_vector = Vec::<OpenBookMarketMetadata>::new();
    let mut failed_sigs = vec![];
    for (idx, txn) in txns.iter_mut().enumerate() {
        match txn {
            Ok(t) => {
                if let Some(m) = &t.transaction.meta {
                    let maybe_new_market = try_parse_new_market(m, t.block_time.unwrap());
                    match &m.log_messages {
                        OptionSerializer::Some(logs) => {
                            match try_parse_openbook_fills_from_logs(
                                logs,
                                target_markets,
                                t.block_time.unwrap(),
                                t.slot,
                            ) {
                                Some(mut events) => fills_vector.append(&mut events),
                                None => {}
                            }
                        }
                        OptionSerializer::None => {}
                        OptionSerializer::Skip => {}
                    }
                    match maybe_new_market {
                        Some(mut markets) => markets_vector.append(&mut markets),
                        None => {}
                    }
                }
            }
            Err(e) => {
                warn!("rpc error in get_transaction {}", e);
                failed_sigs.push(sig_strings[idx].clone());
                METRIC_RPC_ERRORS_TOTAL
                    .with_label_values(&["getTransaction"])
                    .inc();
            }
        }
    }
    sig_strings.retain(|s| !failed_sigs.contains(s));
    (fills_vector, markets_vector, sig_strings)
}

pub fn try_parse_openbook_fills_from_logs(
    logs: &Vec<String>,
    target_markets: &HashMap<String, OpenBookMarketMetadata>,
    block_time: i64,
    slot: u64,
) -> Option<Vec<OpenBookFill>> {
    let mut fills_vector = Vec::<OpenBookFill>::new();
    for l in logs.iter() {
        match l.strip_prefix(PROGRAM_DATA) {
            Some(log) => {
                let borsh_bytes = match anchor_lang::__private::base64::decode(log) {
                    Ok(borsh_bytes) => borsh_bytes,
                    _ => continue,
                };
                let mut slice: &[u8] = &borsh_bytes[8..];
                let fill_log: Result<FillLog, Error> =
                    anchor_lang::AnchorDeserialize::deserialize(&mut slice);

                match fill_log {
                    Ok(f) => {
                        if target_markets.contains_key(&f.market.to_string()) {
                            let market_metadata =
                                target_markets.get(&f.market.to_string()).unwrap();
                            let fill_event = OpenBookFill::from_log(
                                f,
                                market_metadata,
                                slot,
                                to_timestampz(block_time as u64),
                            );
                            fills_vector.push(fill_event);
                        }
                    }
                    _ => continue,
                }
            }
            _ => (),
        }
    }

    if !fills_vector.is_empty() {
        Some(fills_vector)
    } else {
        None
    }
}

fn try_parse_new_market(
    txn_meta: &UiTransactionStatusMeta,
    block_time: i64,
) -> Option<Vec<OpenBookMarketMetadata>> {
    let mut markets_vector = Vec::<OpenBookMarketMetadata>::new();
    match &txn_meta.log_messages {
        OptionSerializer::Some(logs) => {
            if logs.iter().any(|s| s.contains("CreateMarket")) {
                match &txn_meta.inner_instructions {
                    OptionSerializer::Some(ixs) => {
                        ixs.iter().for_each(|x| {
                            x.instructions.iter().for_each(|i| match i {
                                UiInstruction::Compiled(ix) => {
                                    let maybe_market =
                                        parse_market_from_data(ix.data.clone(), block_time);
                                    match maybe_market {
                                        Some(market) => markets_vector.push(market),
                                        None => {}
                                    }
                                }
                                UiInstruction::Parsed(ix) => match ix {
                                    UiParsedInstruction::Parsed(x) => match x.parsed.get("data") {
                                        Some(d) => match d {
                                            serde_json::Value::String(data) => {
                                                let maybe_market = parse_market_from_data(
                                                    data.to_string(),
                                                    block_time,
                                                );
                                                match maybe_market {
                                                    Some(market) => markets_vector.push(market),
                                                    None => {}
                                                }
                                            }
                                            _ => {}
                                        },
                                        None => {}
                                    },
                                    UiParsedInstruction::PartiallyDecoded(d) => {
                                        let maybe_market =
                                            parse_market_from_data(d.data.clone(), block_time);
                                        match maybe_market {
                                            Some(market) => markets_vector.push(market),
                                            None => {}
                                        }
                                    }
                                },
                            })
                        });
                    }
                    OptionSerializer::None => {}
                    OptionSerializer::Skip => {}
                };
            };
        }
        OptionSerializer::None => {}
        OptionSerializer::Skip => {}
    }
    if markets_vector.len() == 0 {
        None
    } else {
        Some(markets_vector)
    }
}

fn parse_market_from_data(data: String, block_time: i64) -> Option<OpenBookMarketMetadata> {
    let bytes = match bs58::decode(data).into_vec() {
        Ok(b) => b,
        Err(_) => return None,
    };
    let mut slice: &[u8] = &bytes[16..];

    let event: Result<MarketMetaDataLog, Error> =
        anchor_lang::AnchorDeserialize::deserialize(&mut slice);

    match event {
        Ok(e) => {
            let datetime = to_timestampz(block_time as u64);
            let new_market = OpenBookMarketMetadata::from_event(e, datetime);
            return Some(new_market);
        }
        _ => return None,
    }
}
