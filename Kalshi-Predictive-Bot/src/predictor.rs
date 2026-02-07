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
