//! Risk management and circuit breaker system (Kalshi-only).
//!
//! This module provides configurable risk limits, position tracking, and
//! automatic trading halt mechanisms to protect against excessive losses.

use std::sync::atomic::{AtomicBool, AtomicI64};
use std::time::{Instant};
use tokio::sync::RwLock;
use tracing::info;

/// Circuit breaker configuration from environment
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub max_position_per_market: i64,
    pub max_total_position: i64,
    pub max_daily_loss: f64,
    pub max_consecutive_errors: u32,
    pub cooldown_secs: u64,
    pub enabled: bool,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        CircuitBreakerConfig {
            max_position_per_market: 50000,
            max_total_position: 100000,
            max_daily_loss: 500.0,
            max_consecutive_errors: 5,
            cooldown_secs: 300,
            enabled: true,
            // Add defaults for all other fields in your CircuitBreakerConfig struct here
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
        use std::env;
        let max_position_per_market = env::var("CB_MAX_POSITION_PER_MARKET").ok().and_then(|v| v.parse().ok()).unwrap_or(50000);
        let max_total_position = env::var("CB_MAX_TOTAL_POSITION").ok().and_then(|v| v.parse().ok()).unwrap_or(100000);
        let max_daily_loss = env::var("CB_MAX_DAILY_LOSS").ok().and_then(|v| v.parse().ok()).unwrap_or(500.0);
        let max_consecutive_errors = env::var("CB_MAX_CONSECUTIVE_ERRORS").ok().and_then(|v| v.parse().ok()).unwrap_or(5);
        let cooldown_secs = env::var("CB_COOLDOWN_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(300);
        let enabled = env::var("CB_ENABLED").ok().and_then(|v| v.parse::<bool>().ok()).unwrap_or(true);
        let config = CircuitBreakerConfig {
            max_position_per_market,
            max_total_position,
            max_daily_loss,
            max_consecutive_errors,
            cooldown_secs,
            enabled,
        };
        Self::new(config)
    }

    /// Update position for a market
    pub async fn update_position(&self, market: &str, yes: i64, no: i64) {
        let mut positions = self.positions.write().await;
        positions.insert(market.to_string(), MarketPosition { kalshi_yes: yes, kalshi_no: no });
    }

    /// Add to daily PnL (in cents)
    pub fn add_daily_pnl(&self, pnl_cents: i64) {
        self.daily_pnl_cents.fetch_add(pnl_cents, std::sync::atomic::Ordering::SeqCst);
    }

    /// Increment consecutive error count
    pub fn increment_errors(&self) {
        self.consecutive_errors.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// Reset consecutive error count
    pub fn reset_errors(&self) {
        self.consecutive_errors.store(0, std::sync::atomic::Ordering::SeqCst);
    }

    /// Check risk limits and trip if exceeded
    pub async fn check_and_trip(&self) -> Option<TripReason> {
        if !self.config.enabled { return None; }
        let positions = self.positions.read().await;
        // Check max position per market
        for (market, pos) in positions.iter() {
            if pos.total_contracts() > self.config.max_position_per_market {
                self.trip(TripReason::MaxPositionPerMarket {
                    market: market.clone(),
                    position: pos.total_contracts(),
                    limit: self.config.max_position_per_market,
                }).await;
                return Some(TripReason::MaxPositionPerMarket {
                    market: market.clone(),
                    position: pos.total_contracts(),
                    limit: self.config.max_position_per_market,
                });
            }
        }
        // Check max total position
        let total_position: i64 = positions.values().map(|p| p.total_contracts()).sum();
        if total_position > self.config.max_total_position {
            self.trip(TripReason::MaxTotalPosition {
                position: total_position,
                limit: self.config.max_total_position,
            }).await;
            return Some(TripReason::MaxTotalPosition {
                position: total_position,
                limit: self.config.max_total_position,
            });
        }
        // Check max daily loss
        let daily_loss = self.daily_pnl_cents.load(std::sync::atomic::Ordering::SeqCst) as f64 / 100.0;
        if daily_loss < -self.config.max_daily_loss {
            self.trip(TripReason::MaxDailyLoss {
                loss: daily_loss,
                limit: self.config.max_daily_loss,
            }).await;
            return Some(TripReason::MaxDailyLoss {
                loss: daily_loss,
                limit: self.config.max_daily_loss,
            });
        }
        // Check consecutive errors
        let errors = self.consecutive_errors.load(std::sync::atomic::Ordering::SeqCst) as u32;
        if errors > self.config.max_consecutive_errors {
            self.trip(TripReason::ConsecutiveErrors {
                count: errors,
                limit: self.config.max_consecutive_errors,
            }).await;
            return Some(TripReason::ConsecutiveErrors {
                count: errors,
                limit: self.config.max_consecutive_errors,
            });
        }
        None
    }

    /// Trip the circuit breaker
    pub async fn trip(&self, reason: TripReason) {
        self.halted.store(true, std::sync::atomic::Ordering::SeqCst);
        let mut tripped_at = self.tripped_at.write().await;
        *tripped_at = Some(Instant::now());
        let mut trip_reason = self.trip_reason.write().await;
        *trip_reason = Some(reason.clone());
        info!("[CB] Circuit breaker tripped: {}", reason);
    }

    /// Manually halt trading
    pub async fn manual_halt(&self) {
        self.trip(TripReason::ManualHalt).await;
    }

    /// Check if circuit breaker is currently halted
    pub fn is_halted(&self) -> bool {
        self.halted.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Check if cooldown period has elapsed and reset if so
    pub async fn check_cooldown_and_reset(&self) -> bool {
        let mut tripped_at = self.tripped_at.write().await;
        if let Some(when) = *tripped_at {
            let elapsed = when.elapsed().as_secs();
            if elapsed > self.config.cooldown_secs {
                self.halted.store(false, std::sync::atomic::Ordering::SeqCst);
                *tripped_at = None;
                let mut trip_reason = self.trip_reason.write().await;
                *trip_reason = None;
                self.reset_errors();
                info!("[CB] Circuit breaker reset after cooldown");
                return true;
            }
        }
        false
    }

    /// Get trip reason
    pub async fn get_trip_reason(&self) -> Option<TripReason> {
        let trip_reason = self.trip_reason.read().await;
        trip_reason.clone()
    }
}
