use crate::structs::{candle::Candle, openbook::PgOpenBookFill};
use chrono::{DateTime, Utc};
use deadpool_postgres::{GenericClient, Object};

pub async fn fetch_earliest_fill_multiple_markets(
    conn_object: &Object,
    market_address_strings: &Vec<String>,
) -> anyhow::Result<Option<PgOpenBookFill>> {
    let stmt = r#"SELECT 
        block_datetime as "time",
        market as "market_key",
        bid as "bid",
        maker as "maker",
        price as "price",
        size as "size"
        from openbook.openbook_fill_events 
        where market = ANY($1)
        and maker = true
        ORDER BY time asc LIMIT 1"#;

    let row = conn_object
        .query_opt(stmt, &[&market_address_strings])
        .await?;

    match row {
        Some(r) => Ok(Some(PgOpenBookFill::from_row(r))),
        None => Ok(None),
    }
}

pub async fn fetch_fills_multiple_markets_from(
    conn_object: &Object,
    market_address_strings: &Vec<String>,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<PgOpenBookFill>> {
    let stmt = r#"SELECT 
         block_datetime as "time",
         market as "market_key",
         bid as "bid",
         maker as "maker",
         price as "price",
         size as "size"
         from openbook.openbook_fill_events 
         where market = ANY($1)
         and block_datetime >= $2::timestamptz
         and block_datetime < $3::timestamptz
         and maker = true
         ORDER BY time asc"#;

    let rows = conn_object
        .query(stmt, &[&market_address_strings, &start_time, &end_time])
        .await?;
    Ok(rows.into_iter().map(PgOpenBookFill::from_row).collect())
}

pub async fn fetch_last_minute_candles(conn_object: &Object) -> anyhow::Result<Vec<Candle>> {
    let stmt = r#"SELECT 
        c.market_name as "market_name",
        c.start_time as "start_time",
        c.end_time as "end_time",
        c.resolution as "resolution",
        c.open as "open",
        c.close as "close",
        c.high as "high",
        c.low as "low",
        c.volume as "volume",
        c.complete as "complete"
         from   
         (
            select market_name, max(start_time) as max_start_time from openbook.candles
            where resolution = '1M'
            group by market_name
        ) mkts
        left join openbook.candles c 
            on mkts.market_name = c.market_name 
            and mkts.max_start_time = c.start_time
        where c.resolution ='1M'"#;

    let rows = conn_object.query(stmt, &[]).await?;
    Ok(rows.into_iter().map(Candle::from_row).collect())
}
