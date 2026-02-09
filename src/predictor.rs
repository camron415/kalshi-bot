//! predictor.rs
//!
//! Predictive model integration for Kalshi-only bot.
//!
//! This module will:
//! - Query Grok-4 (xAI) or other LLMs for event probability forecasts
//! - Optionally use NewsAPI or other data sources for context
//! - Provide a simple async API for main.rs to get probability predictions

use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
// ...existing code...
use tokio::sync::RwLock;
use time::{OffsetDateTime, Date, Duration as TimeDuration};
use std::sync::atomic::{AtomicU32, Ordering};

pub struct GrokCache {
    map: RwLock<HashMap<String, (f64, OffsetDateTime)>>,
    pub calls_today: AtomicU32,
    pub tokens_today: AtomicU32,
    pub last_reset: RwLock<Date>,
}

impl GrokCache {
    pub fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            calls_today: AtomicU32::new(0),
            tokens_today: AtomicU32::new(0),
            last_reset: RwLock::new(OffsetDateTime::now_utc().date()),
        }
    }
    pub async fn get_prediction(&self, dry_run: bool, api_key: &str, ticker: &str, question: &str, context: &str, volume: Option<u64>) -> Result<f64> {
        // Filter: only call Grok for high-liquidity or relevant markets
        let should_call = volume.unwrap_or(0) > 1000
            || ticker.contains("CPI")
            || ticker.contains("Election")
            || ticker.contains("Fed")
            || question.to_lowercase().contains("inflation")
            || question.to_lowercase().contains("election");
        if !should_call {
            tracing::trace!("Grok: Skipping {} (not relevant)", ticker);
            return Ok(0.5);
        }
        // Check/reset daily call/token counters
        let today = OffsetDateTime::now_utc().date();
        {
            let mut last_reset = self.last_reset.write().await;
            if *last_reset != today {
                self.calls_today.store(0, Ordering::Relaxed);
                self.tokens_today.store(0, Ordering::Relaxed);
                *last_reset = today;
            }
        }
        let calls = self.calls_today.load(Ordering::Relaxed);
        let tokens = self.tokens_today.load(Ordering::Relaxed);
        if calls >= 200 || tokens >= 1_000_000 {
            tracing::warn!("Grok: Daily call/token limit reached ({} calls, {} tokens)", calls, tokens);
            return Ok(0.5);
        }
        // Check cache
        let mut map = self.map.write().await;
        if let Some((prob, expiry)) = map.get(ticker) {
            if *expiry > OffsetDateTime::now_utc() {
                tracing::trace!("Grok: Cache hit for {}", ticker);
                return Ok(*prob);
            } else {
                tracing::trace!("Grok: Cache expired for {}", ticker);
            }
        } else {
            tracing::trace!("Grok: Cache miss for {}", ticker);
        }
        // Dry run: simulate call
        if dry_run {
            tracing::info!("Grok: SIMULATED call for {}", ticker);
            map.insert(ticker.to_string(), (0.5, OffsetDateTime::now_utc() + TimeDuration::seconds(1800)));
            return Ok(0.5);
        }
        drop(map); // Release lock before API call
        // Estimate tokens
        let input_tokens = (question.len() + context.len()) as u32 / 4;
        let output_tokens = 200;
        self.calls_today.fetch_add(1, Ordering::Relaxed);
        self.tokens_today.fetch_add(input_tokens + output_tokens, Ordering::Relaxed);
        // Use cheapest Grok model
        let prompt = format!("{}\nContext: {}\nProbability (0-1):", question, context);
        let req = serde_json::json!({
            "model": "grok-4-fast-non-reasoning",
            "prompt": prompt
        });
        let client = reqwest::Client::new();
        let resp = client
            .post("https://api.xai.com/v1/grok4/predict")
            .bearer_auth(api_key)
            .json(&req)
            .send()
            .await?;
        let parsed: GrokResponse = resp.json().await?;
        let expiry = OffsetDateTime::now_utc() + TimeDuration::seconds(1800);
        let mut map = self.map.write().await;
        map.insert(ticker.to_string(), (parsed.probability, expiry));
        tracing::info!("Grok: API call for {} (prob: {:.2}, tokens: {}, est. cost: ${:.4})", ticker, parsed.probability, input_tokens + output_tokens, ((input_tokens as f64 * 0.0000002) + (output_tokens as f64 * 0.0000005)));
        Ok(parsed.probability)
    }
}

/// Trait for a predictive model (LLM, API, etc.)
pub trait Predictor: Send + Sync {
    /// Given a market question and context, return a probability (0.0-1.0)
    fn predict(&self, question: &str, context: &str) -> Result<f64>;
}

/// Dummy implementation (to be replaced with real API integration)
pub struct DummyPredictor;

impl Predictor for DummyPredictor {
    fn predict(&self, _question: &str, _context: &str) -> Result<f64> {
        Ok(0.5)
    }
}

/// Grok-4/xAI API integration (async)
pub struct GrokPredictor {
    pub api_key: String,
    pub client: Client,
}

#[derive(Serialize)]
struct GrokRequest<'a> {
    prompt: &'a str,
}

#[derive(Deserialize)]
struct GrokResponse {
    probability: f64,
}

impl GrokPredictor {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key,
            client: Client::new(),
        }
    }

    /// Query Grok-4/xAI for a probability prediction
    pub async fn predict_async(&self, question: &str, context: &str) -> Result<f64> {
        let prompt = format!("{}\nContext: {}\nProbability (0-1):", question, context);
        let req = GrokRequest { prompt: &prompt };
        let resp = self
            .client
            .post("https://api.xai.com/v1/grok4/predict")
            .bearer_auth(&self.api_key)
            .json(&req)
            .send()
            .await?;
        let parsed: GrokResponse = resp.json().await?;
        Ok(parsed.probability)
    }
}

/// Async wrapper for main.rs
pub async fn get_prediction(api_key: &str, question: &str, context: &str) -> Result<f64> {
    let predictor = GrokPredictor::new(api_key.to_string());
    predictor.predict_async(question, context).await
}
