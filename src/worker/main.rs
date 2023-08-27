use log::{error, info};
use openbook_offchain_services::database::fetch::fetch_active_markets;
use openbook_offchain_services::scraper::scrape::{scrape_signatures, scrape_transactions};
use openbook_offchain_services::structs::openbook_v2::OpenBookMarketMetadata;
use openbook_offchain_services::structs::transaction::NUM_TRANSACTION_PARTITIONS;
use openbook_offchain_services::worker::metrics::{
    serve_metrics, METRIC_DB_POOL_AVAILABLE, METRIC_DB_POOL_SIZE,
};
use openbook_offchain_services::{
    database::initialize::{connect_to_database, setup_database},
    worker::candle_batching::batch_for_market,
};
use std::{collections::HashMap, time::Duration as WaitDuration};

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    dotenv::dotenv().ok();

    let rpc_url: String = dotenv::var("RPC_URL").unwrap();
    let pool = connect_to_database().await?;
    setup_database(&pool).await?;

    let markets = fetch_active_markets(&pool).await?;
    let target_markets: HashMap<String, OpenBookMarketMetadata> = markets
        .into_iter()
        .map(|m| (m.market_pk.clone(), m))
        .collect();
    info!("{:?}", target_markets);
    let mut handles = vec![];

    // signature scraping
    let rpc_clone = rpc_url.clone();
    let pool_clone = pool.clone();
    handles.push(tokio::spawn(async move {
        scrape_signatures(rpc_clone, &pool_clone).await.unwrap();
    }));

    // transaction/fill scraping
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

    // candle batching
    let cloned_markets = target_markets.clone();
    for (_, market) in cloned_markets.into_iter() {
        let batch_pool = pool.clone();
        handles.push(tokio::spawn(async move {
            batch_for_market(&batch_pool, &market).await.unwrap();
            error!("batching halted for market {}", &market.market_name);
        }));
    }

    let monitor_pool = pool.clone();
    handles.push(tokio::spawn(async move {
        loop {
            let pool_status = monitor_pool.status();
            METRIC_DB_POOL_AVAILABLE.set(pool_status.available as i64);
            METRIC_DB_POOL_SIZE.set(pool_status.size as i64);

            tokio::time::sleep(WaitDuration::from_secs(10)).await;
        }
    }));

    handles.push(tokio::spawn(async move {
        serve_metrics().await.unwrap().await.unwrap();
    }));

    futures::future::join_all(handles).await;

    Ok(())
}
