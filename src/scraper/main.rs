use log::{error, info};
use openbook_candles::scraper::parsing::try_parse_new_market;
use openbook_candles::scraper::scrape::{scrape_signatures, scrape_transactions};
use openbook_candles::structs::markets::{fetch_market_infos, load_markets};
use openbook_candles::structs::transaction::NUM_TRANSACTION_PARTITIONS;
use openbook_candles::utils::Config;
use openbook_candles::worker::metrics::{
    serve_metrics, METRIC_DB_POOL_AVAILABLE, METRIC_DB_POOL_SIZE,
};
use openbook_candles::{
    database::initialize::{connect_to_database, setup_database},
    worker::candle_batching::batch_for_market,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::option_serializer::OptionSerializer;
use solana_transaction_status::{UiInstruction, UiParsedInstruction};
use std::env;
use std::{collections::HashMap, str::FromStr, time::Duration as WaitDuration};

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    dotenv::dotenv().ok();

    let args: Vec<String> = env::args().collect();
    // assert!(args.len() == 2);
    let rpc_url: String = dotenv::var("RPC_URL").unwrap();

    println!("{:?}", rpc_url);

    let config = Config {
        rpc_url: rpc_url.clone(),
    };

    let pool = connect_to_database().await?;
    setup_database(&pool).await?;
    let mut handles = vec![];

    // fetch markets
    // let markets =

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
        // let markets_clone = target_markets.clone();
        handles.push(tokio::spawn(async move {
            scrape_transactions(id as i32, rpc_clone, &pool_clone, &HashMap::new()) // TODO
                .await
                .unwrap();
        }));
    }

    // let rpc_client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());
    // let txn_config = RpcTransactionConfig {
    //     encoding: None,
    //     commitment: Some(CommitmentConfig::confirmed()),
    //     max_supported_transaction_version: Some(0),
    // };
    // let s = Signature::from_str(
    //     "rM3fb8WmCwTddptf4YPzqkXAGLvSetEt3xbuwLg3mLPR5XFBqWSKLNDkZ3mEfYr9amsoU5PRiBdujNxjxMbRZVK",
    // )
    // .unwrap();
    // println!("{:?}", s);
    // let x = rpc_client
    //     .get_transaction_with_config(&s, txn_config)
    //     .await
    //     .unwrap();

    // let m = x.transaction.meta.unwrap();
    // let goo = try_parse_new_market(&m, 1);
    // print!("{:?}", goo);

    handles.push(tokio::spawn(async move {
        // TODO: this is ugly af
        serve_metrics().await.unwrap().await.unwrap();
    }));

    futures::future::join_all(handles).await;

    Ok(())
}
