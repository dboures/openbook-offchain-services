use crate::structs::{
    candle::Candle,
    coingecko::{PgCoinGecko24HighLow, PgCoinGecko24HourVolume},
    openbook_v2::{OpenBookFill, OpenBookMarketMetadata},
    resolution::Resolution,
    trader::Trader,
    transaction::PgTransaction,
};
use chrono::{DateTime, Utc};
use deadpool_postgres::{GenericClient, Pool};

pub async fn fetch_earliest_fill(
    pool: &Pool,
    market_address_string: &str,
) -> anyhow::Result<Option<OpenBookFill>> {
    let client = pool.get().await?;
    let stmt = r#"SELECT 
        block_datetime as "block_datetime",
        slot as "slot",
        market_pk as "market_pk",
        seq_num as "seq_num",
        maker as "maker",
        maker_client_order_id as "maker_client_order_id",
        maker_fee as "maker_fee",
        maker_datetime as "maker_datetime",
        taker as "taker",
        taker_client_order_id as "taker_client_order_id",
        taker_fee as "taker_fee",
        taker_side as "taker_side",
        maker_slot as "maker_slot",
        maker_out as "maker_out",
        price as "price",
        quantity as "quantity"
        from fills
        where market_pk = $1
        ORDER BY block_datetime asc LIMIT 1"#;

    let row = client.query_opt(stmt, &[&market_address_string]).await?;

    match row {
        Some(r) => Ok(Some(OpenBookFill::from_row(r))),
        None => Ok(None),
    }
}

pub async fn fetch_fills_from(
    pool: &Pool,
    market_address_string: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<OpenBookFill>> {
    let client = pool.get().await?;
    // TODO: reevaluate
    let stmt = r#"SELECT 
        block_datetime as "block_datetime",
        slot as "slot",
        market_pk as "market_pk",
        seq_num as "seq_num",
        maker as "maker",
        maker_client_order_id as "maker_client_order_id",
        maker_fee as "maker_fee",
        maker_datetime as "maker_datetime",
        taker as "taker",
        taker_client_order_id as "taker_client_order_id",
        taker_fee as "taker_fee",
        taker_side as "taker_side",
        maker_slot as "maker_slot",
        maker_out as "maker_out",
        price as "price",
        quantity as "quantity"
        from fills 
         where market_pk = $1
         and block_datetime >= $2::timestamptz
         and block_datetime < $3::timestamptz
         ORDER BY block_datetime asc"#;

    let rows = client
        .query(stmt, &[&market_address_string, &start_time, &end_time])
        .await?;
    Ok(rows.into_iter().map(OpenBookFill::from_row).collect())
}

pub async fn fetch_latest_finished_candle(
    pool: &Pool,
    market_name: &str,
    resolution: Resolution,
) -> anyhow::Result<Option<Candle>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
        market_name as "market_name!",
        start_time as "start_time!",
        end_time as "end_time!",
        resolution as "resolution!",
        open as "open!",
        close as "close!",
        high as "high!",
        low as "low!",
        volume as "volume!",
        complete as "complete!"
        from candles
        where market_name = $1
        and resolution = $2
        and complete = true
        ORDER BY start_time desc LIMIT 1"#;

    let row = client
        .query_opt(stmt, &[&market_name, &resolution.to_string()])
        .await?;

    match row {
        Some(r) => Ok(Some(Candle::from_row(r))),
        None => Ok(None),
    }
}

/// Fetches all of the candles for the given market and resolution, starting from the earliest.
/// Note that this function will fetch at most 2000 candles.
pub async fn fetch_earliest_candles(
    pool: &Pool,
    market_name: &str,
    resolution: Resolution,
) -> anyhow::Result<Vec<Candle>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
        market_name as "market_name!",
        start_time as "start_time!",
        end_time as "end_time!",
        resolution as "resolution!",
        open as "open!",
        close as "close!",
        high as "high!",
        low as "low!",
        volume as "volume!",
        complete as "complete!"
        from candles
        where market_name = $1
        and resolution = $2
        ORDER BY start_time asc
        LIMIT 2000"#;

    let rows = client
        .query(stmt, &[&market_name, &resolution.to_string()])
        .await?;

    Ok(rows.into_iter().map(Candle::from_row).collect())
}

pub async fn fetch_candles_from(
    pool: &Pool,
    market_name: &str,
    resolution: Resolution,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<Candle>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
        market_name as "market_name!",
        start_time as "start_time!",
        end_time as "end_time!",
        resolution as "resolution!",
        open as "open!",
        close as "close!",
        high as "high!",
        low as "low!",
        volume as "volume!",
        complete as "complete!"
        from candles
        where market_name = $1
        and resolution = $2
        and start_time >= $3
        and end_time <= $4
        ORDER BY start_time asc"#;

    let rows = client
        .query(
            stmt,
            &[
                &market_name,
                &resolution.to_string(),
                &start_time,
                &end_time,
            ],
        )
        .await?;

    Ok(rows.into_iter().map(Candle::from_row).collect())
}

pub async fn fetch_top_traders_by_base_volume_from(
    pool: &Pool,
    market_address_string: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<Trader>> {
    let client = pool.get().await?;

    let stmt = r#"
            SELECT
            trader,
            SUM(quantity) AS total_quantity
        FROM (
            SELECT
                maker AS trader,
                quantity
            FROM
                fills
            WHERE  market_pk = $1
                AND block_datetime >= $2
                AND block_datetime < $3
            UNION ALL
            SELECT
                taker AS trader,
                quantity
            FROM
                fills
            WHERE  market_pk = $1
                AND block_datetime >= $2
                AND block_datetime < $3
        ) AS all_trades
        GROUP BY
            trader
        ORDER BY
            total_quantity DESC
        LIMIT 1000"#;

    let rows = client
        .query(stmt, &[&market_address_string, &start_time, &end_time])
        .await?;

    Ok(rows.into_iter().map(Trader::from_row).collect())
}

pub async fn fetch_top_traders_by_quote_volume_from(
    pool: &Pool,
    market_address_string: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<Trader>> {
    let client = pool.get().await?;

    let stmt = r#"
            SELECT
            trader,
            SUM(quantity) AS total_quantity
        FROM (
            SELECT
                maker AS trader,
                price * quantity as quantity
            FROM
                fills
            WHERE  market_pk = $1
                AND block_datetime >= $2
                AND block_datetime < $3
            UNION ALL
            SELECT
                taker AS trader,
                price * quantity as quantity
            FROM
                fills
            WHERE  market_pk = $1
                AND block_datetime >= $2
                AND block_datetime < $3
        ) AS all_trades
        GROUP BY
            trader
        ORDER BY
            total_quantity DESC
        LIMIT 1000"#;

    let rows = client
        .query(stmt, &[&market_address_string, &start_time, &end_time])
        .await?;

    Ok(rows.into_iter().map(Trader::from_row).collect())
}

pub async fn fetch_coingecko_24h_volume(
    //TODO
    pool: &Pool,
    market_address_strings: &Vec<&str>,
) -> anyhow::Result<Vec<PgCoinGecko24HourVolume>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
            t1.market, 
            COALESCE(t2.base_size, 0) as "base_size",
            COALESCE(t3.quote_size, 0) as "quote_size"
        FROM (
            SELECT unnest($1::text[]) as market 
        ) t1
        LEFT JOIN (
            select market,
            sum(native_quantity_received) as "native_quantity_received",
            sum(native_quantity_paid) as "native_quantity_paid"
            from openbook.openbook_fill_events 
            where "time" >= current_timestamp - interval '1 day' 
            and bid = true
            group by market
        ) t3 ON t1.market = t3.market"#;

    let rows = client.query(stmt, &[&market_address_strings]).await?;

    Ok(rows
        .into_iter()
        .map(PgCoinGecko24HourVolume::from_row)
        .collect())
}

pub async fn fetch_coingecko_24h_high_low(
    pool: &Pool,
    market_names: &Vec<&str>,
) -> anyhow::Result<Vec<PgCoinGecko24HighLow>> {
    let client = pool.get().await?;

    let stmt = r#"select 
            r.market_name as "market_name!", 
            coalesce(c.high, r.high) as "high!", 
            coalesce(c.low, r.low) as "low!", 
            r."close" as "close!"
          from 
            (
              SELECT *
              from 
                candles 
               where (market_name, start_time, resolution) in (
                select market_name, max(start_time), resolution 
                from candles 
                where "resolution" = '1M' 
                and market_name = any($1)
                group by market_name, resolution
            )
            ) as r 
            left join (
            SELECT 
                market_name, 
                max(start_time) as "start_time", 
                max(high) as "high", 
                min(low) as "low"
              from 
                candles 
              where 
                "resolution" = '1M' 
                and "start_time" >= current_timestamp - interval '1 day'
                group by market_name
            ) c on r.market_name = c.market_name"#;

    let rows = client.query(stmt, &[&market_names]).await?;

    Ok(rows
        .into_iter()
        .map(PgCoinGecko24HighLow::from_row)
        .collect())
}

/// Fetches unprocessed, non-error transactions for the specified worker partition.
/// Pulls at most 50 transactions at a time.
pub async fn fetch_worker_transactions(
    worker_id: i32,
    pool: &Pool,
) -> anyhow::Result<Vec<PgTransaction>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT signature, program_pk, block_datetime, slot, err, "processed", worker_partition
            FROM transactions
            where worker_partition = $1
            and err = false 
            and processed = false
            LIMIT 50"#;

    let rows = client.query(stmt, &[&worker_id]).await?;

    Ok(rows.into_iter().map(PgTransaction::from_row).collect())
}

pub async fn fetch_active_markets(pool: &Pool) -> anyhow::Result<Vec<OpenBookMarketMetadata>> {
    let client = pool.get().await?;

    let stmt = r#"
    SELECT 
        creation_datetime, 
        program_pk, 
        market_pk, 
        market_name, 
        base_mint, 
        quote_mint, 
        base_decimals, 
        quote_decimals, 
        base_lot_size, 
        quote_lot_size, 
        scraper_active
    FROM public.market_metadata
        where scraper_active = true"#;

    let rows = client.query(stmt, &[]).await?;

    Ok(rows
        .into_iter()
        .map(OpenBookMarketMetadata::from_row)
        .collect())
}
