use log::{error, info};
use openbook_candles::database::fetch::fetch_active_markets;
use openbook_candles::database::insert::build_fills_upsert_statement;
use openbook_candles::scraper::parsing::try_parse_openbook_fills_from_logs;
use openbook_candles::scraper::scrape::{scrape_signatures, scrape_transactions};
use openbook_candles::structs::markets::{fetch_market_infos, load_markets};
use openbook_candles::structs::openbook_v2::OpenBookMarketMetadata;
use openbook_candles::structs::transaction::NUM_TRANSACTION_PARTITIONS;
use openbook_candles::utils::{AnyhowWrap, Config};
use openbook_candles::worker::metrics::{
    serve_metrics, METRIC_DB_POOL_AVAILABLE, METRIC_DB_POOL_SIZE,
};
use openbook_candles::{
    database::initialize::{connect_to_database, setup_database},
    worker::candle_batching::batch_for_market,
};
use std::env;
use std::{collections::HashMap, str::FromStr, time::Duration as WaitDuration};

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    dotenv::dotenv().ok();

    let rpc_url: String = dotenv::var("RPC_URL").unwrap();

    let pool = connect_to_database().await?;
    setup_database(&pool).await?;
    let mut handles = vec![];

    // fetch markets
    let markets = fetch_active_markets(&pool).await?;

    let target_markets: HashMap<String, OpenBookMarketMetadata> = markets
        .into_iter()
        .map(|m| (m.market_pk.clone(), m))
        .collect();

    // signature scraping
    let rpc_clone = rpc_url.clone();
    let pool_clone = pool.clone();
    handles.push(tokio::spawn(async move {
        scrape_signatures(rpc_clone, &pool_clone).await.unwrap();
    }));

    // transaction scraping
    for id in 0..NUM_TRANSACTION_PARTITIONS {
        let rpc_clone = rpc_url.clone();
        let pool_clone = pool.clone();
        let markets_clone = target_markets.clone();
        handles.push(tokio::spawn(async move {
            scrape_transactions(id as i32, rpc_clone, &pool_clone, &markets_clone)
                .await
                .unwrap();
        }));
    }

    handles.push(tokio::spawn(async move {
        serve_metrics().await.unwrap().await.unwrap();
    }));

    futures::future::join_all(handles).await;

    Ok(())
}
