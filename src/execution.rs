use reqwest::Client;
use hmac::{Hmac, Mac, KeyInit};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use rust_decimal::Decimal;
use std::str::FromStr;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct SymbolInfo {
    pub symbol: String,
    pub step_size: Decimal,
    pub tick_size: Decimal,
}

pub struct BinanceExecutionClient {
    api_key: String,
    api_secret: String,
    client: Client,
    base_url: String,
}

impl BinanceExecutionClient {
    pub fn new(api_key: &str, api_secret: &str) -> Self {
        let base_url = std::env::var("BINANCE_BASE_URL")
            .unwrap_or_else(|_| "https://fapi.binance.com".to_string()); // 默认使用正式主网
            
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            client: Client::new(),
            base_url, 
        }
    }

    fn generate_signature(&self, payload: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes()).unwrap();
        mac.update(payload.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    // 设置杠杆倍数
    pub async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<(), String> {
        let endpoint = "/fapi/v1/leverage";
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
        
        let payload = format!("symbol={}&leverage={}&recvWindow=60000&timestamp={}", symbol, leverage, timestamp);
        let signature = self.generate_signature(&payload);
        let url = format!("{}{}?{}&signature={}", self.base_url, endpoint, payload, signature);

        let res = self.client.post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if res.status().is_success() {
            Ok(())
        } else {
            let error_text = res.text().await.unwrap_or_default();
            Err(format!("设置杠杆失败: {}", error_text))
        }
    }

    pub async fn set_margin_type(&self, symbol: &str, margin_type: &str) -> Result<(), String> {
        let endpoint = "/fapi/v1/marginType";
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
        
        let payload = format!("symbol={}&marginType={}&recvWindow=60000&timestamp={}", symbol, margin_type, timestamp);
        let signature = self.generate_signature(&payload);
        let url = format!("{}{}?{}&signature={}", self.base_url, endpoint, payload, signature);

        let res = self.client.post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        if status.is_success() || text.contains("-4046") {
            Ok(())
        } else {
            Err(format!("设置保证金模式失败: {}", text))
        }
    }

    // 下单并返回真实的成交均价 (Fill Price)
    pub async fn place_order(
        &self,
        symbol: &str,
        side: &str,
        order_type: &str,
        quantity: &str,
        reduce_only: bool,
    ) -> Result<Decimal, String> {
        let endpoint = "/fapi/v1/order";
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

        let payload = format!(
            "symbol={}&side={}&type={}&quantity={}&reduceOnly={}&recvWindow=60000&timestamp={}",
            symbol, side, order_type, quantity, reduce_only, timestamp
        );

        let signature = self.generate_signature(&payload);
        let url = format!("{}{}?{}&signature={}", self.base_url, endpoint, payload, signature);

        let res = self.client.post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let status = res.status();
        let text = res.text().await.map_err(|e| e.to_string())?;

        if status.is_success() {
            // 解析返回的 JSON 获取真正的成交均价 avgPrice
            let v: serde_json::Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
            if let Some(avg_price_str) = v["avgPrice"].as_str() {
                let avg_price = Decimal::from_str(avg_price_str).unwrap_or_default();
                if avg_price > Decimal::ZERO {
                    return Ok(avg_price);
                }
            }
            Ok(Decimal::ZERO)
        } else {
            Err(format!("下单失败: {}", text))
        }
    }

    pub async fn place_limit_order(
        &self,
        symbol: &str,
        side: &str,
        quantity: &str,
        price: &str,
    ) -> Result<String, String> {
        let endpoint = "/fapi/v1/order";
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

        let payload = format!(
            "symbol={}&side={}&type=LIMIT&quantity={}&price={}&timeInForce=GTC&recvWindow=60000&timestamp={}",
            symbol, side, quantity, price, timestamp
        );

        let signature = self.generate_signature(&payload);
        let url = format!("{}{}?{}&signature={}", self.base_url, endpoint, payload, signature);

        let res = self.client.post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let status = res.status();
        let text = res.text().await.map_err(|e| e.to_string())?;

        if status.is_success() {
            Ok(text)
        } else {
            Err(format!("限价单下单失败: {}", text))
        }
    }

    pub async fn check_account(&self) -> Result<String, reqwest::Error> {
        let endpoint = "/fapi/v2/account";
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
        let payload = format!("recvWindow=60000&timestamp={}", timestamp);
        let signature = self.generate_signature(&payload);
        let url = format!("{}{}?{}&signature={}", self.base_url, endpoint, payload, signature);

        let res = self.client.get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        res.text().await
    }

    pub async fn check_positions(&self) -> Result<String, reqwest::Error> {
        let endpoint = "/fapi/v2/positionRisk";
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
        let payload = format!("recvWindow=60000&timestamp={}", timestamp);
        let signature = self.generate_signature(&payload);
        let url = format!("{}{}?{}&signature={}", self.base_url, endpoint, payload, signature);

        let res = self.client.get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        res.text().await
    }

    pub async fn get_income_history(&self, start_time: u64, end_time: u64) -> Result<String, reqwest::Error> {
        let endpoint = "/fapi/v1/income";
        let payload = format!("startTime={}&endTime={}&limit=1000&recvWindow=60000&timestamp={}", 
            start_time, end_time, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis());
        let signature = self.generate_signature(&payload);
        let url = format!("{}{}?{}&signature={}", self.base_url, endpoint, payload, signature);

        let res = self.client.get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        res.text().await
    }

    pub async fn get_user_trades(&self, symbol: &str, limit: u32) -> Result<String, reqwest::Error> {
        let endpoint = "/fapi/v1/userTrades";
        let payload = format!("symbol={}&limit={}&timestamp={}",
            symbol, limit, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis());
        let signature = self.generate_signature(&payload);
        let url = format!("{}{}?{}&signature={}", self.base_url, endpoint, payload, signature);

        let res = self.client.get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        res.text().await
    }

    pub async fn fetch_funding_rate(&self, symbol: &str) -> Result<Decimal, String> {
        let endpoint = "/fapi/v1/premiumIndex";
        let url = format!("{}{}?symbol={}", self.base_url, endpoint, symbol);

        let res = self.client.get(&url)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if res.status().is_success() {
            let text = res.text().await.map_err(|e| e.to_string())?;
            let v: serde_json::Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
            if let Some(rate_str) = v["lastFundingRate"].as_str() {
                return Ok(Decimal::from_str(rate_str).unwrap_or_default());
            }
            Ok(Decimal::ZERO)
        } else {
            Err(format!("获取资金费率失败: {:?}", res.status()))
        }
    }

    pub async fn fetch_kline_open_price(&self, symbol: &str, interval: &str) -> Result<Decimal, String> {
        let endpoint = "/fapi/v1/klines";
        let url = format!("{}{}?symbol={}&interval={}&limit=2", self.base_url, endpoint, symbol, interval);

        let res = self.client.get(&url)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if res.status().is_success() {
            let text = res.text().await.map_err(|e| e.to_string())?;
            let v: serde_json::Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
            if let Some(klines) = v.as_array() {
                if let Some(first_kline) = klines.first() {
                    if let Some(open_price_str) = first_kline.get(1).and_then(|v| v.as_str()) {
                        return Ok(Decimal::from_str(open_price_str).unwrap_or_default());
                    }
                }
            }
            Err("无法解析 K 线数据".to_string())
        } else {
            let error_text = res.text().await.unwrap_or_default();
            Err(format!("获取K线失败: {}", error_text))
        }
    }

    pub async fn fetch_exchange_info(&self) -> Result<std::collections::HashMap<String, SymbolInfo>, String> {
        let url = format!("{}/fapi/v1/exchangeInfo", self.base_url);
        let res = self.client.get(&url).send().await.map_err(|e| e.to_string())?;
        if res.status().is_success() {
            let text = res.text().await.map_err(|e| e.to_string())?;
            let v: serde_json::Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
            let mut map = std::collections::HashMap::new();
            if let Some(symbols) = v["symbols"].as_array() {
                for sym in symbols {
                    if let Some(s) = sym["symbol"].as_str() {
                        let mut step_size = Decimal::ONE;
                        let mut tick_size = Decimal::ONE;
                        if let Some(filters) = sym["filters"].as_array() {
                            for f in filters {
                                if f["filterType"] == "LOT_SIZE" {
                                    if let Some(ss) = f["stepSize"].as_str() {
                                        step_size = Decimal::from_str(ss).unwrap_or(Decimal::ONE);
                                    }
                                }
                                if f["filterType"] == "PRICE_FILTER" {
                                    if let Some(ts) = f["tickSize"].as_str() {
                                        tick_size = Decimal::from_str(ts).unwrap_or(Decimal::ONE);
                                    }
                                }
                            }
                        }
                        map.insert(s.to_string(), SymbolInfo { symbol: s.to_string(), step_size, tick_size });
                    }
                }
            }
            Ok(map)
        } else {
            Err("Failed to fetch exchange info".to_string())
        }
    }
}
