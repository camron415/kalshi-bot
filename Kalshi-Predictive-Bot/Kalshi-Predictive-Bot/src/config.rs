//! System configuration and league mapping definitions (Kalshi-only).
//!
//! This module contains all configuration constants, league mappings, and
//! environment variable parsing for the Kalshi predictive trading system.

/// Kalshi WebSocket URL
pub const KALSHI_WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";

/// Kalshi REST API base URL
pub const KALSHI_API_BASE: &str = "https://api.elections.kalshi.com/trade-api/v2";

/// Kalshi API rate limit delay (milliseconds between requests)
/// Kalshi limit: 20 req/sec = 50ms minimum. We use 60ms for safety margin.
pub const KALSHI_API_DELAY_MS: u64 = 60;

/// WebSocket reconnect delay (seconds)
pub const WS_RECONNECT_DELAY_SECS: u64 = 5;

/// Which leagues to monitor (empty slice = all)
pub const ENABLED_LEAGUES: &[&str] = &[];

/// Price logging enabled (set PRICE_LOGGING=1 to enable)
pub fn price_logging_enabled() -> bool {
    static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("PRICE_LOGGING")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false)
    })
}

/// League configuration for market discovery (Kalshi-only)
#[derive(Debug, Clone)]
pub struct LeagueConfig {
    pub league_code: &'static str,
    pub kalshi_series_game: &'static str,
    pub kalshi_series_spread: Option<&'static str>,
    pub kalshi_series_total: Option<&'static str>,
    pub kalshi_series_btts: Option<&'static str>,
}

/// Get all supported leagues with their configurations
pub fn get_league_configs() -> Vec<LeagueConfig> {
    vec![
        LeagueConfig {
            league_code: "epl",
            kalshi_series_game: "KXEPLGAME",
            kalshi_series_spread: Some("KXEPLSPREAD"),
            kalshi_series_total: Some("KXEPLTOTAL"),
            kalshi_series_btts: Some("KXEPLBTTS"),
        },
        LeagueConfig {
            league_code: "bundesliga",
            kalshi_series_game: "KXBUNDESLIGAGAME",
            kalshi_series_spread: Some("KXBUNDESLIGASPREAD"),
            kalshi_series_total: Some("KXBUNDESLIGATOTAL"),
            kalshi_series_btts: Some("KXBUNDESLIGABTTS"),
        },
        LeagueConfig {
            league_code: "laliga",
            kalshi_series_game: "KXLALIGAGAME",
            kalshi_series_spread: Some("KXLALIGASPREAD"),
            kalshi_series_total: Some("KXLALIGATOTAL"),
            kalshi_series_btts: Some("KXLALIGABTTS"),
        },
        // ... (add other leagues as needed, Kalshi-only)
    ]
}

/// Get config for a specific league
pub fn get_league_config(league: &str) -> Option<LeagueConfig> {
    get_league_configs()
        .into_iter()
        .find(|c| c.league_code == league)
}
