//! Kalshi-only predictive trading bot main entry point.
//!
//! This skeleton removes all Polymarket logic and sets up the main loop for predictive trading using Kalshi, Grok-4, and NewsAPI.

mod kalshi;
mod predictor; // New module for AI/forecasting logic
mod position_tracker;
mod circuit_breaker;
mod config;

use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use time::{OffsetDateTime, Duration as TimeDuration, Date};
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::{info, warn};
use crate::kalshi::{KalshiConfig, KalshiApiClient};
use crate::circuit_breaker::CircuitBreaker;
use crate::position_tracker::PositionTracker;
use predictor::GrokCache;
use crate::config::ENABLED_LEAGUES;

// NewsAPI context cache and call tracking
struct NewsApiCache {
    map: RwLock<HashMap<String, (String, OffsetDateTime)>>,
    calls_today: AtomicU32,
    last_reset: RwLock<Date>,
}

impl NewsApiCache {
    fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            calls_today: AtomicU32::new(0),
            last_reset: RwLock::new(OffsetDateTime::now_utc().date()),
        }
    }
    async fn get_context(&self, ticker: &str, question: &str, volume: Option<u64>) -> String {
        // Filter: only fetch for high-liquidity or relevant markets
        let should_fetch = volume.unwrap_or(0) > 1000
            || ticker.contains("CPI")
            || ticker.contains("Election")
            || ticker.contains("Fed")
            || question.to_lowercase().contains("inflation")
            || question.to_lowercase().contains("president");
        if !should_fetch {
            tracing::trace!("NewsAPI: Skipping {} (not relevant)", ticker);
            return String::new();
        }
        // Check/reset daily call counter
        let today = OffsetDateTime::now_utc().date();
        {
            let mut last_reset = self.last_reset.write().await;
            if *last_reset != today {
                self.calls_today.store(0, Ordering::Relaxed);
                *last_reset = today;
            }
        }
        let calls = self.calls_today.load(Ordering::Relaxed);
        if calls >= 90 {
            tracing::warn!("NewsAPI: Daily call limit reached ({} used)", calls);
            return String::new();
        }
        // Check cache
        let mut map: HashMap<String, (String, OffsetDateTime)> = self.map.write().await;
        if let Some((ctx, expiry)) = map.get(ticker) {
            if *expiry > OffsetDateTime::now_utc() {
                tracing::trace!("NewsAPI: Cache hit for {}", ticker);
                return ctx.clone();
            } else {
                tracing::trace!("NewsAPI: Cache expired for {}", ticker);
            }
        } else {
            tracing::trace!("NewsAPI: Cache miss for {}", ticker);
        }
        // Fetch from NewsAPI
        self.calls_today.fetch_add(1, Ordering::Relaxed);
        drop(map); // Release lock before fetch
        let ctx: String = fetch_news_context(question, ticker).await;
        let expiry = OffsetDateTime::now_utc() + TimeDuration::seconds(3600);
        let mut map: HashMap<String, (String, OffsetDateTime)> = self.map.write().await;
        map.insert(ticker.to_string(), (ctx.clone(), expiry));
        ctx
    }
}

async fn fetch_news_context(question: &str, ticker: &str) -> String {
    let api_key = std::env::var("NEWSAPI_KEY").unwrap_or_else(|_| "fa04428548ef4a1f86f9f7888358dc5c".to_string());
    let keywords = if !question.is_empty() { question } else { ticker };
    let url = format!(
        "https://newsapi.org/v2/everything?q={}&pageSize=5&sortBy=publishedAt&apiKey={}",
        urlencoding::encode(keywords),
        api_key
    );
    let client = reqwest::Client::new();
    match client.get(&url).send().await {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(json) => {
                let mut context = String::new();
                if let Some(articles) = json["articles"].as_array() {
                    for article in articles {
                        if let Some(summary) = article["description"].as_str() {
                            context.push_str(summary);
                            context.push_str("\n");
                        }
                    }
                }
                context
            }
            Err(_) => String::new(),
        },
        Err(_) => String::new(),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt().init();
    info!("🚀 Kalshi Predictive Trading Bot");
    info!("   Monitored leagues: {:?}", ENABLED_LEAGUES);

    // Check for dry run mode
    let dry_run = std::env::var("DRY_RUN").map(|v| v == "1" || v == "true").unwrap_or(true);
    if dry_run {
        info!("   Mode: DRY RUN (set DRY_RUN=0 to execute)");
    } else {
        warn!("   Mode: LIVE EXECUTION");
    }

    // Load Kalshi credentials
    let kalshi_config = KalshiConfig::from_env()?;
    let kalshi_api = Arc::new(KalshiApiClient::new(kalshi_config));

    // Load xAI API key for Grok-4
    let xai_api_key = std::env::var("XAI_API_KEY")
        .expect("XAI_API_KEY must be set in environment");

    // Load bankroll from env or config
    let bankroll: f64 = std::env::var("BANKROLL")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10000.0);

    // Initialize risk management and position tracking
    let _circuit_breaker = Arc::new(CircuitBreaker::from_env());
    let position_tracker = Arc::new(PositionTracker::load());

    // NewsAPI cache
    let news_cache = Arc::new(NewsApiCache::new());

    // Grok cache
    let grok_cache = Arc::new(GrokCache::new());

    // === Main Predictive Trading Loop ===
    loop {
        // 1. Fetch active Kalshi markets
        let markets = kalshi_api.get_active_markets().await?;
        for market in markets {
            // --- NewsAPI filter, cache, and rate limit ---
            let context = news_cache.get_context(&market.ticker, &market.question, market.volume).await;
            // 2b. Query Grok-4 for probability forecast
            let predicted_prob = grok_cache.get_prediction(
                dry_run,
                &xai_api_key,
                &market.ticker,
                &market.question,
                &context,
                market.volume
            ).await?;
            // 2c. Calculate edge (predicted_prob - market_price)
            let market_price = market.price; // TODO: Use correct field for market price
            let edge = predicted_prob - market_price;
            // 2d. Decide: BUY/SELL/SKIP based on edge and threshold
            let _edge_threshold = 0.07; // Example: 7% edge required
            // --- Kelly sizing and risk checks ---
            let kelly_fraction = if market_price > 0.0 && market_price < 1.0 {
                edge / (1.0 / market_price - 1.0)
            } else {
                0.0
            };
            let mut bet_size = (kelly_fraction * bankroll).abs();
            let max_bet = 0.05 * bankroll;
            if bet_size > max_bet {
                bet_size = max_bet;
            }
            let size = bet_size.round() as i32;
            // Cap open positions
            if position_tracker.len() >= 15 {
                warn!("SKIP: Max open positions reached (15)");
                continue;
            }
            if size < 1 {
                info!("SKIP: Kelly size < 1 contract");
                continue;
            }
            // 2e. Use Kelly criterion for bet sizing, cap at 5% per trade, max 15 open positions
            // TODO: Implement Kelly sizing and risk checks
            // 2f. If dry_run, simulate trade; else, execute order
            // TODO: Integrate with circuit_breaker and position_tracker
            // 2g. Log all actions and predictions
        }
        // 3. Sleep or wait for next scan interval
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        // 4. Handle graceful shutdown, error retries, and rate limits
    }
}

// Simulate and log a trade in dry run mode
fn simulate_log_trade(ticker: &str, size: i32, price: f64, side: &str) {
    info!("SIMULATED {}: {} (size: {}, price: {:.2})", side.to_uppercase(), ticker, size, price);
}
