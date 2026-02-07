//! Position tracking and P&L calculation system (Kalshi-only).
//!
//! This module tracks all Kalshi positions, calculates cost basis,
//! and maintains real-time profit and loss calculations.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn};

const POSITION_FILE: &str = "positions.json";

/// A single position leg on Kalshi
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PositionLeg {
    /// Number of contracts held
    pub contracts: f64,
    /// Total cost paid (in dollars)
    pub cost_basis: f64,
    /// Average price per contract
    pub avg_price: f64,
}

impl PositionLeg {
    pub fn add(&mut self, contracts: f64, price: f64) {
        let new_cost = contracts * price;
        self.cost_basis += new_cost;
        self.contracts += contracts;
        if self.contracts > 0.0 {
            self.avg_price = self.cost_basis / self.contracts;
        }
    }
    /// Unrealized P&L based on current market price
    pub fn unrealized_pnl(&self, current_price: f64) -> f64 {
        let current_value = self.contracts * current_price;
        current_value - self.cost_basis
    }
    /// Value if this position wins (pays $1 per contract)
    pub fn value_if_win(&self) -> f64 {
        self.contracts * 1.0
    }
    /// Profit if this position wins
    pub fn profit_if_win(&self) -> f64 {
        self.value_if_win() - self.cost_basis
    }
}

/// A Kalshi-only position (YES/NO)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArbPosition {
    /// Market identifier (Kalshi ticker)
    pub market_id: String,
    /// Description for logging
    pub description: String,
    /// Kalshi YES position
    pub kalshi_yes: PositionLeg,
    /// Kalshi NO position
    pub kalshi_no: PositionLeg,
    /// Total fees paid (Kalshi fees)
    pub total_fees: f64,
    /// Timestamp when position was opened
    pub opened_at: String,
    /// Status: "open", "closed", "resolved"
    pub status: String,
    /// Realized P&L (set when position closes/resolves)
    pub realized_pnl: Option<f64>,
}

impl ArbPosition {
    pub fn new(market_id: &str, description: &str) -> Self {
        Self {
            market_id: market_id.to_string(),
            description: description.to_string(),
            status: "open".to_string(),
            opened_at: chrono::Utc::now().to_rfc3339(),
            ..Default::default()
        }
    }
    /// Total contracts across both legs
    pub fn total_contracts(&self) -> f64 {
        self.kalshi_yes.contracts + self.kalshi_no.contracts
    }
    /// Total cost basis across both legs
    pub fn total_cost(&self) -> f64 {
        self.kalshi_yes.cost_basis + self.kalshi_no.cost_basis + self.total_fees
    }
    /// For a proper Kalshi position (YES + NO), one side always wins
    /// This calculates the guaranteed profit assuming the position is balanced
    pub fn guaranteed_profit(&self) -> f64 {
        let balanced_contracts = self.matched_contracts();
        balanced_contracts - self.total_cost()
    }
    /// Number of matched contract pairs (min of YES and NO)
    pub fn matched_contracts(&self) -> f64 {
        self.kalshi_yes.contracts.min(self.kalshi_no.contracts)
    }
    /// Unmatched exposure (contracts without offsetting position)
    pub fn unmatched_exposure(&self) -> f64 {
        (self.kalshi_yes.contracts - self.kalshi_no.contracts).abs()
    }
    /// Mark position as resolved with outcome
    pub fn resolve(&mut self, outcome_yes_won: bool) {
        let payout = if outcome_yes_won {
            self.kalshi_yes.contracts
        } else {
            self.kalshi_no.contracts
        };
        self.realized_pnl = Some(payout - self.total_cost());
        self.status = "resolved".to_string();
    }
}

/// Summary of all positions
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PositionSummary {
    /// Total cost basis across all open positions
    pub total_cost_basis: f64,
    /// Total guaranteed profit from matched positions
    pub total_guaranteed_profit: f64,
    /// Total unmatched exposure (risk)
    pub total_unmatched_exposure: f64,
    /// Total realized P&L from closed/resolved positions
    pub realized_pnl: f64,
    /// Number of open positions
    pub open_positions: usize,
    /// Number of resolved positions
    pub resolved_positions: usize,
    /// Total contracts held
    pub total_contracts: f64,
}

/// Position tracker with persistence
#[derive(Debug, Serialize, Deserialize)]
pub struct PositionTracker {
    /// All positions keyed by market_id
    positions: HashMap<String, ArbPosition>,
    /// Daily realized P&L
    pub daily_realized_pnl: f64,
    /// Daily trading date (for reset)
    pub trading_date: String,
    /// Cumulative all-time P&L
    pub all_time_pnl: f64,
}

#[derive(Serialize)]
struct SaveData {
    positions: HashMap<String, ArbPosition>,
    daily_realized_pnl: f64,
    trading_date: String,
    all_time_pnl: f64,
}

impl Default for PositionTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl PositionTracker {
    pub fn new() -> Self {
        Self {
            positions: HashMap::new(),
            daily_realized_pnl: 0.0,
            trading_date: today_string(),
            all_time_pnl: 0.0,
        }
    }
    /// Load from file or create new
    pub fn load() -> Self {
        Self::load_from(POSITION_FILE)
    }
    pub fn load_from<P: AsRef<Path>>(path: P) -> Self {
        match std::fs::read_to_string(path.as_ref()) {
            Ok(contents) => {
                match serde_json::from_str::<Self>(&contents) {
                    Ok(mut tracker) => {
                        let today = today_string();
                        if tracker.trading_date != today {
                            info!("[POSITIONS] New trading day, resetting daily P&L");
                            tracker.daily_realized_pnl = 0.0;
                            tracker.trading_date = today;
                        }
                        info!("[POSITIONS] Loaded {} positions from {:?}", 
                              tracker.positions.len(), path.as_ref());
                        tracker
                    }
                    Err(e) => {
                        warn!("[POSITIONS] Failed to parse positions file: {}", e);
                        Self::new()
                    }
                }
            }
            Err(_) => {
                info!("[POSITIONS] No positions file found, starting fresh");
                Self::new()
            }
        }
    }
    /// Save to file
    pub fn save(&self) -> Result<()> {
        self.save_to(POSITION_FILE)
    }
    pub fn save_to<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
    /// Save positions
    pub fn save_async(&self) {
        let data = SaveData {
            positions: self.positions.clone(),
            daily_realized_pnl: self.daily_realized_pnl,
            trading_date: self.trading_date.clone(),
            all_time_pnl: self.all_time_pnl,
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                if let Ok(json) = serde_json::to_string_pretty(&data) {
                    let _ = tokio::fs::write(POSITION_FILE, json).await;
                }
            });
        } else if let Ok(json) = serde_json::to_string_pretty(&data) {
            let _ = std::fs::write(POSITION_FILE, json);
        }
    }
    /// Record a fill
    pub fn record_fill(&mut self, fill: &FillRecord) {
        self.record_fill_internal(fill);
        self.save_async();
    }
    /// Record a fill without saving
    pub fn record_fill_internal(&mut self, fill: &FillRecord) {
        let position = self.positions
            .entry(fill.market_id.clone())
            .or_insert_with(|| ArbPosition::new(&fill.market_id, &fill.description));
        match fill.side.as_str() {
            "yes" => position.kalshi_yes.add(fill.contracts, fill.price),
            "no" => position.kalshi_no.add(fill.contracts, fill.price),
            _ => warn!("[POSITIONS] Unknown side: {}", fill.side),
        }
        position.total_fees += fill.fees;
        info!("[POSITIONS] Recorded fill: Kalshi {} {} @{:.1}¢ x{:.0} (fees: ${:.4})",
              fill.side, fill.market_id, fill.price * 100.0, fill.contracts, fill.fees);
    }
    /// Get or create position for a market
    pub fn get_or_create(&mut self, market_id: &str, description: &str) -> &mut ArbPosition {
        self.positions
            .entry(market_id.to_string())
            .or_insert_with(|| ArbPosition::new(market_id, description))
    }
    /// Get position (if exists)
    pub fn get(&self, market_id: &str) -> Option<&ArbPosition> {
        self.positions.get(market_id)
    }
    /// Mark a position as resolved
    pub fn resolve_position(&mut self, market_id: &str, yes_won: bool) -> Option<f64> {
        if let Some(position) = self.positions.get_mut(market_id) {
            position.resolve(yes_won);
            let pnl = position.realized_pnl.unwrap_or(0.0);
            self.daily_realized_pnl += pnl;
            self.all_time_pnl += pnl;
            info!("[POSITIONS] Resolved {}: {} won, P&L: ${:.2}",
                  market_id, if yes_won { "YES" } else { "NO" }, pnl);
            self.save_async();
            Some(pnl)
        } else {
            None
        }
    }
    /// Get summary statistics
    pub fn summary(&self) -> PositionSummary {
        let mut summary = PositionSummary::default();
        for position in self.positions.values() {
            match position.status.as_str() {
                "open" => {
                    summary.open_positions += 1;
                    summary.total_cost_basis += position.total_cost();
                    summary.total_guaranteed_profit += position.guaranteed_profit();
                    summary.total_unmatched_exposure += position.unmatched_exposure();
                    summary.total_contracts += position.total_contracts();
                }
                "resolved" => {
                    summary.resolved_positions += 1;
                    summary.realized_pnl += position.realized_pnl.unwrap_or(0.0);
                }
                _ => {}
            }
        }
        summary
    }
    /// Get all open positions
    pub fn open_positions(&self) -> Vec<&ArbPosition> {
        self.positions.values()
            .filter(|p| p.status == "open")
            .collect()
    }
    /// Daily P&L (realized only)
    pub fn daily_pnl(&self) -> f64 {
        self.daily_realized_pnl
    }
    /// Reset daily counters (call at midnight)
    pub fn reset_daily(&mut self) {
        self.daily_realized_pnl = 0.0;
        self.trading_date = today_string();
        self.save_async();
    }
}

/// Record of a single fill
#[derive(Debug, Clone)]
pub struct FillRecord {
    pub market_id: String,
    pub description: String,
    pub platform: String,   // always "kalshi"
    pub side: String,       // "yes" or "no"
    pub contracts: f64,
    pub price: f64,
    pub fees: f64,
    pub order_id: String,
    pub timestamp: String,
}

impl FillRecord {
    pub fn new(
        market_id: &str,
        description: &str,
        side: &str,
        contracts: f64,
        price: f64,
        fees: f64,
        order_id: &str,
    ) -> Self {
        Self {
            market_id: market_id.to_string(),
            description: description.to_string(),
            platform: "kalshi".to_string(),
            side: side.to_string(),
            contracts,
            price,
            fees,
            order_id: order_id.to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }
}

pub type SharedPositionTracker = Arc<RwLock<PositionTracker>>;

pub fn create_position_tracker() -> SharedPositionTracker {
    Arc::new(RwLock::new(PositionTracker::load()))
}

fn today_string() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

#[derive(Clone)]
pub struct PositionChannel {
    tx: mpsc::UnboundedSender<FillRecord>,
}

impl PositionChannel {
    pub fn new(tx: mpsc::UnboundedSender<FillRecord>) -> Self {
        Self { tx }
    }
    #[inline]
    pub fn record_fill(&self, fill: FillRecord) {
        let _ = self.tx.send(fill);
    }
}

pub fn create_position_channel() -> (PositionChannel, mpsc::UnboundedReceiver<FillRecord>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (PositionChannel::new(tx), rx)
}

pub async fn position_writer_loop(
    mut rx: mpsc::UnboundedReceiver<FillRecord>,
    tracker: Arc<RwLock<PositionTracker>>,
) {
    let mut batch = Vec::with_capacity(16);
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            biased;
            Some(fill) = rx.recv() => {
                batch.push(fill);
                if batch.len() >= 16 {
                    let mut guard = tracker.write().await;
                    for fill in batch.drain(..) {
                        guard.record_fill_internal(&fill);
                    }
                    guard.save_async();
                }
            }
            _ = interval.tick() => {
                if !batch.is_empty() {
                    let mut guard = tracker.write().await;
                    for fill in batch.drain(..) {
                        guard.record_fill_internal(&fill);
                    }
                    guard.save_async();
                }
            }
        }
    }
}

// (Tests for Kalshi-only logic can be added here)
