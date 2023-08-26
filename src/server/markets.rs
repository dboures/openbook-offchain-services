use crate::server_error::ServerError;
use actix_web::{get, web, HttpResponse};
use openbook_offchain_services::utils::WebContext;

#[get("/markets")]
pub async fn get_markets(context: web::Data<WebContext>) -> Result<HttpResponse, ServerError> {
    let markets = &context.markets;
    Ok(HttpResponse::Ok().json(markets))
}
