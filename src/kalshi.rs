/// Start monitoring all open markets, refreshing every 5 minutes
pub async fn start_monitoring(client: Arc<KalshiApiClient>, config: KalshiConfig) -> Result<()> {
    loop {
        let markets = fetch_all_open_markets(&client).await?;
        let tickers: Vec<String> = markets.into_iter().map(|m| m.ticker).collect();
        run_ws(&config, tickers, client.clone()).await?;
        tokio::time::sleep(std::time::Duration::from_secs(300)).await;
    }
}
/// Fetch all open markets from Kalshi API with pagination
pub async fn fetch_all_open_markets(client: &KalshiApiClient) -> anyhow::Result<Vec<KalshiMarket>> {
    let mut markets = Vec::new();
    let mut cursor: Option<String> = None;
    let mut retries = 0;
    const MAX_RETRIES: u32 = 5;
    loop {
        let mut url = format!("{}/markets?status=open&limit=1000", KALSHI_API_BASE);
        if let Some(ref c) = cursor {
            url.push_str(&format!("&cursor={}", c));
        }
        let resp = client.http.get(&url)
            .header("KALSHI-ACCESS-KEY", &client.config.api_key_id)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                retries += 1;
                if retries > MAX_RETRIES {
                    anyhow::bail!("Kalshi API rate limited after {} retries", MAX_RETRIES);
                }
                let backoff_ms = 2000 * (1 << retries);
                tracing::debug!("[KALSHI] Rate limited, backing off {}ms (retry {}/{})", backoff_ms, retries, MAX_RETRIES);
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                continue;
            }
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Kalshi API error {}: {}", status, body);
        }
        let data: KalshiMarketsResponse = resp.json().await?;
        let count = data.markets.len();
        markets.extend(data.markets);
        let next_cursor = if data.cursor.is_empty() { None } else { Some(data.cursor.clone()) };
        if next_cursor.is_none() || data.markets.is_empty() {
            break;
        }
        cursor = next_cursor;
    }
    info!("Fetched {} open markets", markets.len());
    if !markets.is_empty() {
        info!("Sample tickers: {:?}", markets.iter().take(5).map(|m| &m.ticker).collect::<Vec<_>>());
    }
    Ok(markets)
}
// Kalshi platform integration client (Kalshi-only).
//
// This module provides REST API and WebSocket clients for interacting with
// the Kalshi prediction market platform, including order execution and
// real-time price feed management.

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use pkcs1::DecodeRsaPrivateKey;
use rsa::{
    pss::SigningKey,
    sha2::Sha256,
    signature::{RandomizedSigner, SignatureEncoding},
    RsaPrivateKey,
};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, tungstenite::{http::Request}};
use tracing::{debug, error, info};

use crate::config::{KALSHI_WS_URL, KALSHI_API_BASE, KALSHI_API_DELAY_MS};

use std::borrow::Cow;
use std::fmt::Write;
use arrayvec::ArrayString;

#[derive(Debug, Clone, Serialize)]
pub struct KalshiOrderRequest<'a> {
    pub ticker: Cow<'a, str>,
    pub action: &'static str,
    pub side: &'static str,
    #[serde(rename = "type")]
    pub order_type: &'static str,
    pub count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub yes_price: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no_price: Option<i64>,
    pub client_order_id: Cow<'a, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_ts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<&'static str>,
}

impl<'a> KalshiOrderRequest<'a> {
    pub fn ioc_buy(ticker: Cow<'a, str>, side: &'static str, price_cents: i64, count: i64, client_order_id: Cow<'a, str>) -> Self {
        let (yes_price, no_price) = if side == "yes" {
            (Some(price_cents), None)
        } else {
            (None, Some(price_cents))
        };
        Self {
            ticker,
            action: "buy",
            side,
            order_type: "limit",
            count,
            yes_price,
            no_price,
            client_order_id,
            expiration_ts: None,
            time_in_force: Some("immediate_or_cancel"),
        }
    }
    pub fn ioc_sell(ticker: Cow<'a, str>, side: &'static str, price_cents: i64, count: i64, client_order_id: Cow<'a, str>) -> Self {
        let (yes_price, no_price) = if side == "yes" {
            (Some(price_cents), None)
        } else {
            (None, Some(price_cents))
        };
        Self {
            ticker,
            action: "sell",
            side,
            order_type: "limit",
            count,
            yes_price,
            no_price,
            client_order_id,
            expiration_ts: None,
            time_in_force: Some("immediate_or_cancel"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct KalshiOrderResponse {
    pub order: KalshiOrderDetails,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KalshiOrderDetails {
    pub order_id: String,
    pub ticker: String,
    pub status: String,        // "resting", "canceled", "executed", "pending"
    #[serde(default)]
    pub remaining_count: Option<i64>,
    #[serde(default)]
    pub queue_position: Option<i64>,
    pub action: String,
    pub side: String,
    #[serde(rename = "type")]
    pub order_type: String,
    pub yes_price: Option<i64>,
    pub no_price: Option<i64>,
    pub created_time: Option<String>,
    #[serde(default)]
    pub taker_fill_count: Option<i64>,
    #[serde(default)]
    pub maker_fill_count: Option<i64>,
    #[serde(default)]
    pub place_count: Option<i64>,
    #[serde(default)]
    pub taker_fill_cost: Option<i64>,
    #[serde(default)]
    pub maker_fill_cost: Option<i64>,
}

impl KalshiOrderDetails {
    pub fn filled_count(&self) -> i64 {
        self.taker_fill_count.unwrap_or(0) + self.maker_fill_count.unwrap_or(0)
    }
    pub fn is_filled(&self) -> bool {
        self.status == "executed" || self.remaining_count == Some(0)
    }
    pub fn is_partial(&self) -> bool {
        self.filled_count() > 0 && !self.is_filled()
    }
}

// === Kalshi Auth Config ===

pub struct KalshiConfig {
    pub api_key_id: String,
    pub private_key: RsaPrivateKey,
}

impl KalshiConfig {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();
        let api_key_id = std::env::var("KALSHI_API_KEY_ID").context("KALSHI_API_KEY_ID not set")?;
        let key_path = std::env::var("KALSHI_PRIVATE_KEY_PATH")
            .or_else(|_| std::env::var("KALSHI_PRIVATE_KEY_FILE"))
            .unwrap_or_else(|_| "kalshi_private_key.txt".to_string());
        let private_key_pem = std::fs::read_to_string(&key_path)
            .with_context(|| format!("Failed to read private key from {}", key_path))?
            .trim()
            .to_owned();
        let private_key = RsaPrivateKey::from_pkcs1_pem(&private_key_pem)
            .context("Failed to parse private key PEM")?;
        Ok(Self { api_key_id, private_key })
    }
    pub fn sign(&self, message: &str) -> Result<String> {
        tracing::debug!("[KALSHI-DEBUG] Signing message: {}", message);
        let signing_key = SigningKey::<Sha256>::new(self.private_key.clone());
        let signature = signing_key.sign_with_rng(&mut rand::thread_rng(), message.as_bytes());
        let sig_b64 = BASE64.encode(signature.to_bytes());
        tracing::debug!("[KALSHI-DEBUG] Signature (first 50 chars): {}...", &sig_b64[..50.min(sig_b64.len())]);
        Ok(sig_b64)
    }
}

// === Kalshi REST API Client ===

const ORDER_TIMEOUT: Duration = Duration::from_secs(5);
use std::sync::atomic::{AtomicU32, Ordering};
static ORDER_COUNTER: AtomicU32 = AtomicU32::new(0);

pub struct KalshiApiClient {
    http: reqwest::Client,
    pub config: KalshiConfig,
}

impl KalshiApiClient {
    pub fn new(config: KalshiConfig) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("Failed to build HTTP client"),
            config,
        }
    }
    #[inline]
    fn next_order_id() -> ArrayString<24> {
        let counter = ORDER_COUNTER.fetch_add(1, Ordering::Relaxed);
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let mut buf = ArrayString::<24>::new();
        let _ = write!(&mut buf, "a{}{}", ts, counter);
        buf
    }
    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let mut retries = 0;
        const MAX_RETRIES: u32 = 5;
        loop {
            let url = format!("{}{}", KALSHI_API_BASE, path);
            let timestamp_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let full_path = format!("/trade-api/v2{}", path);
            let signature = self.config.sign(&format!("{}GET{}", timestamp_ms, full_path))?;
            let resp = self.http
                .get(&url)
                .header("KALSHI-ACCESS-KEY", &self.config.api_key_id)
                .header("KALSHI-ACCESS-SIGNATURE", &signature)
                .header("KALSHI-ACCESS-TIMESTAMP", timestamp_ms.to_string())
                .send()
                .await?;
            let status = resp.status();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                retries += 1;
                if retries > MAX_RETRIES {
                    anyhow::bail!("Kalshi API rate limited after {} retries", MAX_RETRIES);
                }
                let backoff_ms = 2000 * (1 << retries);
                debug!("[KALSHI] Rate limited, backing off {}ms (retry {}/{})", 
                       backoff_ms, retries, MAX_RETRIES);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                continue;
            }
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("Kalshi API error {}: {}", status, body);
            }
            let data: T = resp.json().await?;
            tokio::time::sleep(Duration::from_millis(KALSHI_API_DELAY_MS)).await;
            return Ok(data);
        }
    }
    pub async fn get_events(&self, series_ticker: &str, limit: u32) -> Result<Vec<KalshiEvent>> {
        let path = format!("/events?series_ticker={}&limit={}&status=open", series_ticker, limit);
        let resp: KalshiEventsResponse = self.get(&path).await?;
        Ok(resp.events)
    }
    pub async fn get_markets(&self, event_ticker: &str) -> Result<Vec<KalshiMarket>> {
        let path = format!("/markets?event_ticker={}", event_ticker);
        let resp: KalshiMarketsResponse = self.get(&path).await?;
        Ok(resp.markets)
    }
    async fn post<T: serde::de::DeserializeOwned, B: Serialize>(&self, path: &str, body: &B) -> Result<T> {
        let url = format!("{}{}", KALSHI_API_BASE, path);
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let full_path = format!("/trade-api/v2{}", path);
        let msg = format!("{}POST{}", timestamp_ms, full_path);
        let signature = self.config.sign(&msg)?;
        let resp = self.http
            .post(&url)
            .header("KALSHI-ACCESS-KEY", &self.config.api_key_id)
            .header("KALSHI-ACCESS-SIGNATURE", &signature)
            .header("KALSHI-ACCESS-TIMESTAMP", timestamp_ms.to_string())
            .header("Content-Type", "application/json")
            .timeout(ORDER_TIMEOUT)
            .json(body)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Kalshi API error {}: {}", status, body);
        }
        let data: T = resp.json().await?;
        Ok(data)
    }
    pub async fn create_order(&self, order: &KalshiOrderRequest<'_>) -> Result<KalshiOrderResponse> {
        let path = "/portfolio/orders";
        self.post(path, order).await
    }
    pub async fn buy_ioc(&self, ticker: &str, side: &str, price_cents: i64, count: i64) -> Result<KalshiOrderResponse> {
        debug_assert!(!ticker.is_empty(), "ticker must not be empty");
        debug_assert!(price_cents >= 1 && price_cents <= 99, "price must be 1-99");
        debug_assert!(count >= 1, "count must be >= 1");
        let side_static: &'static str = if side == "yes" { "yes" } else { "no" };
        let order_id = Self::next_order_id();
        let order = KalshiOrderRequest::ioc_buy(
            Cow::Borrowed(ticker),
            side_static,
            price_cents,
            count,
            Cow::Borrowed(&order_id)
        );
        debug!("[KALSHI] IOC {} {} @{}¢ x{}", side, ticker, price_cents, count);
        let resp = self.create_order(&order).await?;
        debug!("[KALSHI] {} filled={}", resp.order.status, resp.order.filled_count());
        Ok(resp)
    }
    pub async fn sell_ioc(&self, ticker: &str, side: &str, price_cents: i64, count: i64) -> Result<KalshiOrderResponse> {
        debug_assert!(!ticker.is_empty(), "ticker must not be empty");
        debug_assert!(price_cents >= 1 && price_cents <= 99, "price must be 1-99");
        debug_assert!(count >= 1, "count must be >= 1");
        let side_static: &'static str = if side == "yes" { "yes" } else { "no" };
        let order_id = Self::next_order_id();
        let order = KalshiOrderRequest::ioc_sell(
            Cow::Borrowed(ticker),
            side_static,
            price_cents,
            count,
            Cow::Borrowed(&order_id)
        );
        debug!("[KALSHI] SELL {} {} @{}¢ x{}", side, ticker, price_cents, count);
        let resp = self.create_order(&order).await?;
        debug!("[KALSHI] {} filled={}", resp.order.status, resp.order.filled_count());
        Ok(resp)
    }
}

// WebSocket message types and runner (Kalshi-only)
#[derive(Deserialize, Debug)]
pub struct KalshiWsMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub msg: Option<KalshiWsMsgBody>,
}

#[derive(Deserialize, Debug)]
pub struct KalshiWsMsgBody {
    pub market_ticker: Option<String>,
    pub yes: Option<Vec<Vec<i64>>>,
    pub no: Option<Vec<Vec<i64>>>,
    pub price: Option<i64>,
    pub delta: Option<i64>,
    pub side: Option<String>,
}

#[derive(Serialize)]
struct SubscribeCmd {
    id: i32,
    cmd: &'static str,
    params: SubscribeParams,
}

#[derive(Serialize)]
struct SubscribeParams {
    channels: Vec<&'static str>,
    market_tickers: Vec<String>,
}

// WebSocket runner (Kalshi-only, no arbitrage logic)
pub async fn run_ws(
    config: &KalshiConfig,
    market_tickers: Vec<String>,
    client: Arc<KalshiApiClient>,
) -> Result<()> {
    let mut attempt = 0;
    loop {
        if market_tickers.is_empty() {
            warn!("[KALSHI] No markets to monitor");
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            return Ok(());
        }
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .to_string();
        let signature = config.sign(&format!("{}GET/trade-api/ws/v2", timestamp))?;
        let request = Request::builder()
            .uri(KALSHI_WS_URL)
            .header("KALSHI-ACCESS-KEY", &config.api_key_id)
            .header("KALSHI-ACCESS-SIGNATURE", &signature)
            .header("KALSHI-ACCESS-TIMESTAMP", &timestamp)
            .header("Host", "api.elections.kalshi.com")
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", tokio_tungstenite::tungstenite::handshake::client::generate_key())
            .body(())?;
        match connect_async(request).await {
            Ok((ws_stream, _)) => {
                let ws_stream: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> = ws_stream;
                info!("[KALSHI] Connected");
                let (mut ws_sink, mut ws_stream): (futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, tokio_tungstenite::tungstenite::Message>, futures_util::stream::SplitStream<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>) = ws_stream.split();
                let subscribe_msg = SubscribeCmd {
                    id: 1,
                    cmd: "subscribe",
                    params: SubscribeParams {
                        channels: vec!["orderbook_delta"],
                        market_tickers: market_tickers.clone(),
                    },
                };
                ws_sink.send(Message::Text(serde_json::to_string(&subscribe_msg)?)).await?;
                info!("[KALSHI] Subscribed to {} markets", market_tickers.len());
                while let Some(msg) = ws_stream.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            match serde_json::from_str::<KalshiWsMessage>(&text) {
                                Ok(kalshi_msg) => {
                                    if let Some(body) = kalshi_msg.msg {
                                        info!("[WS] Update for ticker {:?}: yes={:?}, no={:?}, price={:?}, delta={:?}", body.market_ticker, body.yes, body.no, body.price, body.delta);
                                        if let Some(delta) = body.delta {
                                            if delta.abs() > 5 {
                                                info!("[PREDICT] Opportunity on {:?} - significant delta {}", body.market_ticker, delta);
                                                // TODO: trigger trade via client
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::trace!("[KALSHI] WS parse error: {} (msg: {}...)", e, &text[..text.len().min(100)]);
                                    client.circuit.record_error();
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = ws_sink.send(Message::Pong(data)).await;
                        }
                        Err(e) => {
                            error!("[KALSHI] WebSocket error: {}", e);
                            client.circuit.record_error();
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!("Reconnect attempt {} failed: {}", attempt, e);
                client.circuit.record_error();
                attempt += 1;
                tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt.min(5)))).await;
                continue;
            }
        }
        break;
    }
    Ok(())
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct KalshiEvent {
    pub ticker: String,
    pub question: String,
    pub volume: Option<u64>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct KalshiEventsResponse {
    pub events: Vec<KalshiEvent>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct KalshiMarket {
    pub ticker: String,
    pub question: String,
    pub price: f64,
    pub volume: Option<u64>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct KalshiMarketsResponse {
    pub markets: Vec<KalshiMarket>,
    pub cursor: String,
}

