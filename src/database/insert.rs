use deadpool_postgres::Pool;

use crate::{
    structs::{
        candle::Candle,
        openbook_v2::{OpenBookFill, OpenBookMarketMetadata},
        transaction::PgTransaction,
    },
    utils::AnyhowWrap,
};

pub async fn insert_atomically(
    pool: &Pool,
    worker_id: i32,
    fills: Vec<OpenBookFill>,
    markets: Vec<OpenBookMarketMetadata>,
    signatures: Vec<String>,
) -> anyhow::Result<()> {
    let mut client = pool.get().await?;

    let db_txn = client.build_transaction().start().await?;

    // 1. Insert fills
    if !fills.is_empty() {
        let fills_statement = build_fills_upsert_statement(fills);
        db_txn
            .execute(&fills_statement, &[])
            .await
            .map_err_anyhow()
            .unwrap();
    }

    // 2. Insert markets
    if !markets.is_empty() {
        let markets_statement = build_markets_insert_statement(markets);
        db_txn
            .execute(&markets_statement, &[])
            .await
            .map_err_anyhow()
            .unwrap();
    }

    // 3. Update txns table as processed
    let transactions_statement =
        build_transactions_processed_update_statement(worker_id, signatures);
    db_txn
        .execute(&transactions_statement, &[])
        .await
        .map_err_anyhow()
        .unwrap();

    db_txn.commit().await?;

    Ok(())
}

pub fn build_fills_upsert_statement(fills: Vec<OpenBookFill>) -> String {
    let mut stmt = String::from("INSERT INTO fills (block_datetime, slot, market_pk, seq_num, maker, maker_client_order_id, maker_fee, maker_datetime, taker, taker_client_order_id, taker_fee, taker_side, maker_slot, maker_out, price, quantity) VALUES");
    for (idx, fill) in fills.iter().enumerate() {
        let val_str = format!(
            "(\'{}\', {}, \'{}\', {}, \'{}\', \'{}\', {}, \'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {})",
            fill.block_datetime.to_rfc3339(),
            fill.slot,
            fill.market_pk,
            fill.seq_num,
            fill.maker,
            fill.maker_client_order_id,
            fill.maker_fee,
            fill.maker_datetime.to_rfc3339(),
            fill.taker,
            fill.taker_client_order_id,
            fill.taker_fee,
            fill.taker_side,
            fill.maker_slot,
            fill.maker_out,
            fill.price,
            fill.quantity
        );

        if idx == 0 {
            stmt = format!("{} {}", &stmt, val_str);
        } else {
            stmt = format!("{}, {}", &stmt, val_str);
        }
    }

    let handle_conflict = "ON CONFLICT DO NOTHING";

    stmt = format!("{} {}", stmt, handle_conflict);
    stmt
}

pub fn build_candles_upsert_statement(candles: &Vec<Candle>) -> String {
    let mut stmt = String::from("INSERT INTO candles (market_name, start_time, end_time, resolution, open, close, high, low, volume, complete) VALUES");
    for (idx, candle) in candles.iter().enumerate() {
        let val_str = format!(
            "(\'{}\', \'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {})",
            candle.market_name,
            candle.start_time.to_rfc3339(),
            candle.end_time.to_rfc3339(),
            candle.resolution,
            candle.open,
            candle.close,
            candle.high,
            candle.low,
            candle.volume,
            candle.complete,
        );

        if idx == 0 {
            stmt = format!("{} {}", &stmt, val_str);
        } else {
            stmt = format!("{}, {}", &stmt, val_str);
        }
    }

    let handle_conflict = "ON CONFLICT (market_name, start_time, resolution) 
    DO UPDATE SET 
    open=excluded.open, 
    close=excluded.close, 
    high=excluded.high, 
    low=excluded.low,
    volume=excluded.volume,
    complete=excluded.complete
    ";

    stmt = format!("{} {}", stmt, handle_conflict);
    stmt
}

pub fn build_transactions_insert_statement(transactions: Vec<PgTransaction>) -> String {
    let mut stmt = String::from("INSERT INTO transactions (signature, program_pk, block_datetime, slot, err, processed, worker_partition) VALUES");
    for (idx, txn) in transactions.iter().enumerate() {
        let val_str = format!(
            "(\'{}\', \'{}\', \'{}\', \'{}\', {}, {}, {})",
            txn.signature,
            txn.program_pk,
            txn.block_datetime.to_rfc3339(),
            txn.slot,
            txn.err,
            txn.processed,
            txn.worker_partition,
        );

        if idx == 0 {
            stmt = format!("{} {}", &stmt, val_str);
        } else {
            stmt = format!("{}, {}", &stmt, val_str);
        }
    }

    let handle_conflict = "ON CONFLICT DO NOTHING";

    stmt = format!("{} {}", stmt, handle_conflict);
    stmt
}

fn build_markets_insert_statement(markets: Vec<OpenBookMarketMetadata>) -> String {
    let mut stmt = String::from("INSERT INTO public.market_metadata
    (creation_datetime, program_pk, market_pk, market_name, base_mint, quote_mint, base_decimals, quote_decimals, base_lot_size, quote_lot_size, scraper_active)
    VALUES");
    for (idx, market) in markets.iter().enumerate() {
        let val_str = format!(
            "(\'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {})",
            market.creation_datetime.to_rfc3339(),
            market.program_pk,
            market.market_pk,
            market.market_name,
            market.base_mint,
            market.quote_mint,
            market.base_decimals,
            market.quote_decimals,
            market.base_lot_size,
            market.quote_lot_size,
            false
        );

        if idx == 0 {
            stmt = format!("{} {}", &stmt, val_str);
        } else {
            stmt = format!("{}, {}", &stmt, val_str);
        }
    }
    let handle_conflict = "ON CONFLICT DO NOTHING";
    stmt = format!("{} {}", stmt, handle_conflict);
    stmt
}

pub fn build_transactions_processed_update_statement(
    worker_id: i32,
    processed_signatures: Vec<String>,
) -> String {
    let mut stmt = String::from(
        "UPDATE transactions
    SET processed = true
    WHERE transactions.signature IN (",
    );
    for (idx, sig) in processed_signatures.iter().enumerate() {
        let val_str = if idx == processed_signatures.len() - 1 {
            format!("\'{}\'", sig,)
        } else {
            format!("\'{}\',", sig,)
        };
        stmt = format!("{} {}", &stmt, val_str);
    }

    let worker_stmt = format!(") AND worker_partition = {} ", worker_id);

    stmt = format!("{} {}", stmt, worker_stmt);
    stmt
}
