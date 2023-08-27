use actix_web::{
    http::StatusCode,
    middleware::Logger,
    rt::System,
    web::{self, Data},
    App, HttpServer,
};
use actix_web_prom::PrometheusMetricsBuilder;
use candles::get_candles;
use prometheus::Registry;

use markets::get_markets;
use openbook_offchain_services::{
    database::{fetch::fetch_active_markets, initialize::connect_to_database},
    utils::WebContext,
};
use std::thread;
use traders::{get_top_traders_by_base_volume, get_top_traders_by_quote_volume};

mod candles;
mod coingecko;
mod markets;
mod server_error;
mod traders;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    let rpc_url: String = dotenv::var("RPC_URL").unwrap();
    let bind_addr: String = dotenv::var("SERVER_BIND_ADDR").expect("reading bind addr from env");

    let pool = connect_to_database().await.unwrap();
    let markets = fetch_active_markets(&pool).await.unwrap();

    let registry = Registry::new();
    // For serving metrics on a private port
    let private_metrics =
        PrometheusMetricsBuilder::new("openbook_offchain_services_server_private")
            .registry(registry.clone())
            .exclude("/metrics")
            .exclude_status(StatusCode::NOT_FOUND)
            .endpoint("/metrics")
            .build()
            .unwrap();
    // For collecting metrics on the public api, excluding 404s
    let public_metrics = PrometheusMetricsBuilder::new("openbook_offchain_services_server")
        .registry(registry)
        .exclude_status(StatusCode::NOT_FOUND)
        .build()
        .unwrap();

    let context = Data::new(WebContext {
        rpc_url,
        pool,
        markets,
    });

    println!("Starting server");
    // Thread to serve public API
    let public_server = thread::spawn(move || {
        let sys = System::new();
        let srv = HttpServer::new(move || {
            App::new()
                .wrap(Logger::default())
                .wrap(public_metrics.clone())
                .app_data(context.clone())
                .service(
                    web::scope("/api")
                        .service(get_candles)
                        .service(get_top_traders_by_base_volume)
                        .service(get_top_traders_by_quote_volume)
                        .service(get_markets)
                        .service(coingecko::service()),
                )
        })
        .bind(&bind_addr)
        .unwrap()
        .run();
        sys.block_on(srv).unwrap();
    });

    // Thread to serve metrics endpoint privately
    let private_server = thread::spawn(move || {
        let sys = System::new();
        let srv = HttpServer::new(move || App::new().wrap(private_metrics.clone()))
            .bind("0.0.0.0:9091")
            .unwrap()
            .run();
        sys.block_on(srv).unwrap();
    });

    private_server.join().unwrap();
    public_server.join().unwrap();
    Ok(())
}
