use tokio::time::{sleep, Duration};
use reqwest;
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tracing::{info, error};
use chrono::Utc;
use dotenvy::dotenv;

// --- Models ---

#[derive(Debug, Clone)]
struct StockPrice {
    symbol: String,
    price: f64,
    source: String,
    timestamp: i64,
}

#[derive(Deserialize, Debug)]
struct GlobalQuote {
    #[serde(rename = "Global Quote")]
    quote: Quote,
}

#[derive(Deserialize, Debug)]
struct Quote {
    #[serde(rename = "01. symbol")]
    symbol: String,
    #[serde(rename = "05. price")]
    price: String,
}

// --- API Call ---

async fn fetch_alpha_vantage(symbol: &str) -> Result<StockPrice, Box<dyn std::error::Error>> {
    let api_key = std::env::var("ALPHA_VANTAGE_KEY")?;

    let url = format!(
        "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={api_key}"
    );

    let resp = reqwest::get(&url)
        .await?
        .json::<GlobalQuote>()
        .await?;

    let price = resp.quote.price.parse::<f64>()?;

    Ok(StockPrice {
        symbol: resp.quote.symbol,
        price,
        source: "alpha_vantage".to_string(),
        timestamp: Utc::now().timestamp(),
    })
}

// --- Save to DB ---

async fn save_price(pool: &PgPool, p: &StockPrice) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO stock_prices (symbol, price, source, timestamp)
        VALUES ($1, $2, $3, $4)
        "#
    )
    .bind(&p.symbol)
    .bind(p.price)
    .bind(&p.source)
    .bind(p.timestamp)
    .execute(pool)
    .await?;

    Ok(())
}

// --- Main ---

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    info!("Starting TD1...");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;

    let symbols = ["AAPL", "GOOGL", "MSFT"];

    for sym in symbols {
        match fetch_alpha_vantage(sym).await {
            Ok(price) => {
                info!("Fetched {sym}: ${}", price.price);
                if let Err(e) = save_price(&pool, &price).await {
                    error!("DB error: {e}");
                }
            }
            Err(err) => error!("Fetch error: {err}"),
        }

        sleep(Duration::from_millis(500)).await;
    }

    Ok(())
}
