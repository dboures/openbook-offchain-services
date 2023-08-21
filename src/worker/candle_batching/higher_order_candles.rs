use chrono::{DateTime, Duration, DurationRound, Utc};
use deadpool_postgres::Pool;
use log::debug;
use std::cmp::{max, min};
use strum::IntoEnumIterator;

use crate::{
    database::{
        fetch::{fetch_candles_from, fetch_earliest_candles, fetch_latest_finished_candle},
        insert::build_candles_upsert_statement,
    },
    structs::{
        candle::Candle,
        resolution::{day, Resolution},
    },
    utils::{f64_max, f64_min, AnyhowWrap},
};

pub async fn batch_higher_order_candles(
    pool: &Pool,
    market_name: &str,
    resolution: Resolution,
) -> anyhow::Result<Vec<Candle>> {
    let latest_candle = fetch_latest_finished_candle(pool, market_name, resolution).await?;

    match latest_candle {
        Some(candle) => {
            let start_time = candle.end_time;
            let end_time = start_time + day();
            let mut constituent_candles = fetch_candles_from(
                pool,
                market_name,
                resolution.get_constituent_resolution(),
                start_time,
                end_time,
            )
            .await?;
            if constituent_candles.is_empty() {
                return Ok(Vec::new());
            }
            let combined_candles =
                combine_into_higher_order_candles(&mut constituent_candles, resolution, start_time);
            Ok(combined_candles)
        }
        None => {
            let mut constituent_candles =
                fetch_earliest_candles(pool, market_name, resolution.get_constituent_resolution())
                    .await?;
            if constituent_candles.is_empty() {
                debug!(
                    "Batching {}, but no candles found for: {:?}, {}",
                    resolution,
                    market_name,
                    resolution.get_constituent_resolution()
                );
                return Ok(Vec::new());
            }
            let start_time = constituent_candles[0].start_time.duration_trunc(day())?;

            if constituent_candles.is_empty() {
                return Ok(Vec::new());
            }

            let combined_candles =
                combine_into_higher_order_candles(&mut constituent_candles, resolution, start_time);

            Ok(trim_candles(
                combined_candles,
                constituent_candles[0].start_time,
            ))
        }
    }
}

fn combine_into_higher_order_candles(
    constituent_candles: &Vec<Candle>,
    target_resolution: Resolution,
    st: DateTime<Utc>,
) -> Vec<Candle> {
    debug!("combining for target_resolution: {}", target_resolution);

    let duration = target_resolution.get_duration();

    let empty_candle = Candle::create_empty_candle(
        constituent_candles[0].market_name.clone(),
        target_resolution,
    );
    let now = Utc::now().duration_trunc(Duration::minutes(1)).unwrap();
    let candle_window = min(now - st, day());
    let num_candles = max(
        1,
        (candle_window.num_minutes() / duration.num_minutes()) as usize + 1,
    );

    let mut combined_candles = vec![empty_candle; num_candles];

    let mut last_close = constituent_candles[0].close;
    let mut con_iter = constituent_candles.iter().peekable();
    let mut start_time = st;
    let mut end_time = start_time + duration;

    for i in 0..combined_candles.len() {
        combined_candles[i].open = last_close;
        combined_candles[i].low = last_close;
        combined_candles[i].close = last_close;
        combined_candles[i].high = last_close;

        while matches!(con_iter.peek(), Some(c) if c.end_time <= end_time) {
            let unit_candle = con_iter.next().unwrap();
            combined_candles[i].high = f64_max(combined_candles[i].high, unit_candle.high);
            combined_candles[i].low = f64_min(combined_candles[i].low, unit_candle.low);
            combined_candles[i].close = unit_candle.close;
            combined_candles[i].volume += unit_candle.volume;
            combined_candles[i].complete = unit_candle.complete;
            combined_candles[i].end_time = unit_candle.end_time;
        }

        combined_candles[i].start_time = start_time;
        combined_candles[i].end_time = end_time;

        start_time = end_time;
        end_time += duration;

        last_close = combined_candles[i].close;
    }

    combined_candles
}

fn trim_candles(mut c: Vec<Candle>, start_time: DateTime<Utc>) -> Vec<Candle> {
    let mut i = 0;
    while i < c.len() {
        if c[i].end_time <= start_time {
            c.remove(i);
        } else {
            i += 1;
        }
    }
    c
}

pub async fn backfill_batch_higher_order_candles(
    pool: &Pool,
    market_name: &str,
) -> anyhow::Result<()> {
    let earliest_candles = fetch_earliest_candles(pool, market_name, Resolution::R1m).await?;
    let mut start_time = earliest_candles[0].start_time.duration_trunc(day())?;
    while start_time < Utc::now() {
        let mut candles = vec![];
        let mut constituent_candles = fetch_candles_from(
            pool,
            market_name,
            Resolution::R1m,
            start_time,
            start_time + day(),
        )
        .await?;

        for resolution in Resolution::iter() {
            if resolution == Resolution::R1m {
                continue;
            }
            let mut combined_candles =
                combine_into_higher_order_candles(&mut constituent_candles, resolution, start_time);
            candles.append(&mut combined_candles);
        }

        let upsert_statement = build_candles_upsert_statement(&candles);
        let client = pool.get().await.unwrap();
        client
            .execute(&upsert_statement, &[])
            .await
            .map_err_anyhow()?;
        // println!("{:?} {:?} done", market_name, start_time);
        start_time += day();
    }

    Ok(())
}
