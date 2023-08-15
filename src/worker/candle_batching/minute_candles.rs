use std::{cmp::min, collections::HashMap};

use chrono::{DateTime, Duration, DurationRound, Utc};
use deadpool_postgres::Pool;
use itertools::Itertools;
use log::debug;

use crate::database::backfill::{
    fetch_earliest_fill_multiple_markets, fetch_fills_multiple_markets_from,
    fetch_last_minute_candles,
};
use crate::{
    database::{
        fetch::{fetch_earliest_fill, fetch_fills_from, fetch_latest_finished_candle},
        insert::build_candles_upsert_statement,
    },
    structs::{
        candle::{Candle},
        markets::MarketInfo,
        openbook::{calculate_fill_price_and_size, PgOpenBookFill},
        resolution::{day, Resolution},
    },
    utils::{f64_max, f64_min, AnyhowWrap},
};

pub async fn batch_1m_candles(pool: &Pool, market: &MarketInfo) -> anyhow::Result<Vec<Candle>> {
    let market_name = &market.name;
    let market_address = &market.address;
    let latest_candle = fetch_latest_finished_candle(pool, market_name, Resolution::R1m).await?;

    match latest_candle {
        Some(candle) => {
            let start_time = candle.end_time;
            let end_time = min(
                start_time + day(),
                (Utc::now() + Duration::minutes(1)).duration_trunc(Duration::minutes(1))?,
            );
            let mut fills = fetch_fills_from(pool, market_address, start_time, end_time).await?;

            let candles = combine_fills_into_1m_candles(
                &mut fills,
                market,
                start_time,
                end_time,
                Some(candle.close),
            );
            Ok(candles)
        }
        None => {
            let earliest_fill = fetch_earliest_fill(pool, market_address).await?;

            if earliest_fill.is_none() {
                debug!("No fills found for: {:?}", market_name);
                return Ok(Vec::new());
            }

            let start_time = earliest_fill
                .unwrap()
                .time
                .duration_trunc(Duration::minutes(1))?;
            let end_time = min(
                start_time + day(),
                Utc::now().duration_trunc(Duration::minutes(1))?,
            );
            let mut fills = fetch_fills_from(pool, market_address, start_time, end_time).await?;
            if !fills.is_empty() {
                let candles =
                    combine_fills_into_1m_candles(&mut fills, market, start_time, end_time, None);
                Ok(candles)
            } else {
                Ok(Vec::new())
            }
        }
    }
}

fn combine_fills_into_1m_candles(
    fills: &mut Vec<PgOpenBookFill>,
    market: &MarketInfo,
    st: DateTime<Utc>,
    et: DateTime<Utc>,
    maybe_last_price: Option<f64>,
) -> Vec<Candle> {
    let empty_candle = Candle::create_empty_candle(market.name.clone(), Resolution::R1m);

    let minutes = (et - st).num_minutes();
    let mut candles = vec![empty_candle; minutes as usize];

    let mut fills_iter = fills.iter_mut().peekable();
    let mut start_time = st;
    let mut end_time = start_time + Duration::minutes(1);

    let mut last_price = match maybe_last_price {
        Some(p) => p,
        None => {
            let first = fills_iter.peek().unwrap();
            let (price, _) =
                calculate_fill_price_and_size(**first, market.base_decimals, market.quote_decimals);
            price
        }
    };

    for i in 0..candles.len() {
        candles[i].open = last_price;
        candles[i].close = last_price;
        candles[i].low = last_price;
        candles[i].high = last_price;

        while matches!(fills_iter.peek(), Some(f) if f.time < end_time) {
            let fill = fills_iter.next().unwrap();
            let (price, volume) =
                calculate_fill_price_and_size(*fill, market.base_decimals, market.quote_decimals);

            candles[i].close = price;
            candles[i].low = f64_min(price, candles[i].low);
            candles[i].high = f64_max(price, candles[i].high);
            candles[i].volume += volume;

            last_price = price;
        }

        candles[i].start_time = start_time;
        candles[i].end_time = end_time;
        candles[i].complete = matches!(fills_iter.peek(), Some(f) if f.time > end_time)
            || end_time < Utc::now() - Duration::minutes(10);
        start_time = end_time;
        end_time += Duration::minutes(1);
    }

    candles
}

/// Goes from the earliest fill to the most recent. Will mark candles as complete if there are missing gaps of fills between the start and end.
pub async fn backfill_batch_1m_candles(
    pool: &Pool,
    markets: Vec<MarketInfo>,
) -> anyhow::Result<()> {
    let market_address_strings: Vec<String> = markets.iter().map(|m| m.address.clone()).collect();
    let mut candle_container = HashMap::new();
    let client = pool.get().await?;

    let earliest_fill =
        fetch_earliest_fill_multiple_markets(&client, &market_address_strings).await?;
    if earliest_fill.is_none() {
        println!("No fills found for backfill");
        return Ok(());
    }
    println!("Found earliset fill for backfill");

    let mut start_time = earliest_fill
        .unwrap()
        .time
        .duration_trunc(Duration::minutes(1))?;
    while start_time < Utc::now() {
        let end_time = min(
            start_time + day(),
            Utc::now().duration_trunc(Duration::minutes(1))?,
        );
        let last_candles = fetch_last_minute_candles(&client).await?;
        let all_fills = fetch_fills_multiple_markets_from(
            &client,
            &market_address_strings,
            start_time,
            end_time,
        )
        .await?;
        // println!("{:?} {:?}", start_time, end_time);
        // println!("all fills len : {:?}", all_fills.len());
        println!("{:?}", all_fills[0]);
        // println!("{:?}", all_fills[1]);
        // println!("Fetched multiple fills for backfill");
        let fills_groups = all_fills
            .into_iter()
            .sorted_by(|a, b| Ord::cmp(&a.market_key, &b.market_key))
            .group_by(|f| f.market_key.clone());

        let fills_by_market: Vec<(String, Vec<PgOpenBookFill>)> = fills_groups
            .into_iter()
            .map(|(m, group)| (m, group.collect()))
            .collect();

        println!("fbm len : {:?}", fills_by_market.len());
        // sort fills by market, make candles
        for (_, mut fills) in fills_by_market {
            let market = markets
                .iter()
                .find(|m| m.address == fills[0].market_key)
                .unwrap();
            let minute_candles =
                combine_fills_into_1m_candles(&mut fills, market, start_time, end_time, None);
            candle_container.insert(&market.address, minute_candles);
        }

        // where no candles, make empty ones
        for (k, v) in candle_container.iter_mut() {
            if v.is_empty() {
                let market = markets.iter().find(|m| &m.address == *k).unwrap();
                let last_candle = last_candles
                    .iter()
                    .find(|c| c.market_name == market.name)
                    .unwrap();
                let empty_candles = combine_fills_into_1m_candles(
                    &mut vec![],
                    market,
                    start_time,
                    end_time,
                    Some(last_candle.close),
                );
                *v = empty_candles;
            }
        }

        // insert candles in batches
        for candles in candle_container.values() {
            let candle_chunks: Vec<Vec<Candle>> =
                candles.chunks(1500).map(|chunk| chunk.to_vec()).collect(); // 1440 minutes in a day
            for c in candle_chunks {
                let upsert_statement = build_candles_upsert_statement(&c);
                client
                    .execute(&upsert_statement, &[])
                    .await
                    .map_err_anyhow()?;
            }
        }
        // reset entries but keep markets we've seen for blank candles
        for (_, v) in candle_container.iter_mut() {
            *v = vec![];
        }
        println!("day done");
        start_time += day();
    }
    Ok(())
}
