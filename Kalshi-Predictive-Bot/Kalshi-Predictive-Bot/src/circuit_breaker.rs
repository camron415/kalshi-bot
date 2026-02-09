//! Risk management and circuit breaker system (Kalshi-only).
//!
//! This module provides configurable risk limits, position tracking, and
//! automatic trading halt mechanisms to protect against excessive losses.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{error, warn, info};

/// Circuit breaker configuration from environment
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub max_position_per_market: i64,
    pub max_total_position: i64,
    pub max_daily_loss: f64,
    pub max_consecutive_errors: u32,
    pub cooldown_secs: u64,
    pub enabled: bool,
// Removed extra closing brace

impl CircuitBreakerConfig {
    pub fn from_env() -> Self {
        Self {
            max_position_per_market: std::env::var("CB_MAX_POSITION_PER_MARKET")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(50000),
            max_total_position: std::env::var("CB_MAX_TOTAL_POSITION")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100000),
            max_daily_loss: std::env::var("CB_MAX_DAILY_LOSS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(500.0),
            max_consecutive_errors: std::env::var("CB_MAX_CONSECUTIVE_ERRORS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5),
            cooldown_secs: std::env::var("CB_COOLDOWN_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
            enabled: std::env::var("CB_ENABLED")
                .map(|v| v == "1" || v == "true")
                .unwrap_or(true),
        }
    }

}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self::from_env()
    }
}
}

#[derive(Debug, Clone, PartialEq)]
pub enum TripReason {
    MaxPositionPerMarket { market: String, position: i64, limit: i64 },
    MaxTotalPosition { position: i64, limit: i64 },
    MaxDailyLoss { loss: f64, limit: f64 },
    ConsecutiveErrors { count: u32, limit: u32 },
    ManualHalt,
}

impl std::fmt::Display for TripReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TripReason::MaxPositionPerMarket { market, position, limit } => {
                write!(f, "Max position per market: {} has {} contracts (limit: {})", market, position, limit)
            }
            TripReason::MaxTotalPosition { position, limit } => {
                write!(f, "Max total position: {} contracts (limit: {})", position, limit)
            }
            TripReason::MaxDailyLoss { loss, limit } => {
                write!(f, "Max daily loss: ${:.2} (limit: ${:.2})", loss, limit)
            }
            TripReason::ConsecutiveErrors { count, limit } => {
                write!(f, "Consecutive errors: {} (limit: {})", count, limit)
            }
            TripReason::ManualHalt => {
                write!(f, "Manual halt triggered")
            }
        }
    }
}

/// Position tracking for a single market (Kalshi-only)
#[derive(Debug, Default)]
pub struct MarketPosition {
    pub kalshi_yes: i64,
    pub kalshi_no: i64,
}

impl MarketPosition {
    pub fn net_position(&self) -> i64 {
        self.kalshi_yes - self.kalshi_no
    }
    pub fn total_contracts(&self) -> i64 {
        self.kalshi_yes + self.kalshi_no
    }
}

/// Circuit breaker state (Kalshi-only)
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    halted: AtomicBool,
    tripped_at: RwLock<Option<Instant>>,
    trip_reason: RwLock<Option<TripReason>>,
    consecutive_errors: AtomicI64,
    daily_pnl_cents: AtomicI64,
    positions: RwLock<std::collections::HashMap<String, MarketPosition>>,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        info!("[CB] Circuit breaker initialized:");
        info!("[CB]   Enabled: {}", config.enabled);
        info!("[CB]   Max position per market: {} contracts", config.max_position_per_market);
        info!("[CB]   Max total position: {} contracts", config.max_total_position);
        info!("[CB]   Max daily loss: ${:.2}", config.max_daily_loss);
        info!("[CB]   Max consecutive errors: {}", config.max_consecutive_errors);
        info!("[CB]   Cooldown: {}s", config.cooldown_secs);
        Self {
            config,
            halted: AtomicBool::new(false),
            tripped_at: RwLock::new(None),
            trip_reason: RwLock::new(None),
            consecutive_errors: AtomicI64::new(0),
            daily_pnl_cents: AtomicI64::new(0),
            positions: RwLock::new(std::collections::HashMap::new()),
        }
    }

    pub fn from_env() -> Self {
        // Placeholder: implement actual env loading logic
        Self::new(CircuitBreakerConfig::default())
    }
}
