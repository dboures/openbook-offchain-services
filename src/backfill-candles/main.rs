use openbook_candles::{
    database::initialize::connect_to_database,
    structs::markets::{fetch_market_infos, load_markets},
    utils::Config,
    worker::candle_batching::{
        higher_order_candles::backfill_batch_higher_order_candles,
        minute_candles::backfill_batch_1m_candles,
    },
};
use std::env;

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let args: Vec<String> = env::args().collect();
    assert!(args.len() == 2);

    let path_to_markets_json = &args[1];
    let rpc_url: String = dotenv::var("RPC_URL").unwrap();

    let config = Config {
        rpc_url: rpc_url.clone(),
    };
    let markets = load_markets(path_to_markets_json);
    let market_infos = fetch_market_infos(&config, markets.clone()).await?;
    println!("Backfilling candles for {:?}", markets);

    let pool = connect_to_database().await?;
    backfill_batch_1m_candles(&pool, market_infos.clone()).await?;

    let mut handles = vec![];
    let mi = market_infos.clone();
    for market in mi.into_iter() {
        let pc = pool.clone();
        handles.push(tokio::spawn(async move {
            backfill_batch_higher_order_candles(&pc, &market.name)
                .await
                .unwrap();
        }));
    }

    futures::future::join_all(handles).await;
    Ok(())
}
