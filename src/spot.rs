// ==========================================
// MODULE: 币安现货交易客户端 (Spot Execution Client)
// 现货走 api.binance.com, 与合约的 fapi 是两套独立端点/权限。
// 只包含网格所需的最小接口: 限价单/市价卖/撤单/查单/查余额/交易规则。
// 现货无杠杆无强平; 资金安全边界 = 下单时校验不超过预算。
// ==========================================
use std::time::{SystemTime, UNIX_EPOCH};
use hmac::{Hmac, Mac, KeyInit};
use sha2::Sha256;

pub struct SpotClient {
    client: reqwest::Client,
    api_key: String,
    api_secret: String,
    base_url: String,
}

#[derive(Debug, Clone)]
pub struct SpotSymbolRules {
    pub tick_size: f64,     // 价格最小步进
    pub step_size: f64,     // 数量最小步进
    pub min_notional: f64,  // 单笔最小名义(USDT)
}

fn round_step(value: f64, step: f64) -> f64 {
    if step <= 0.0 {
        return value;
    }
    (value / step).floor() * step
}

impl SpotClient {
    pub fn from_env() -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: std::env::var("BINANCE_API_KEY").unwrap_or_default(),
            api_secret: std::env::var("BINANCE_API_SECRET").unwrap_or_default(),
            base_url: std::env::var("BINANCE_SPOT_URL").unwrap_or_else(|_| "https://api.binance.com".to_string()),
        }
    }

    fn sign(&self, payload: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.api_secret.as_bytes()).expect("HMAC key");
        mac.update(payload.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    async fn signed_request(&self, method: reqwest::Method, endpoint: &str, params: &str) -> Result<serde_json::Value, String> {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
        let payload = if params.is_empty() {
            format!("recvWindow=10000&timestamp={}", ts)
        } else {
            format!("{}&recvWindow=10000&timestamp={}", params, ts)
        };
        let sig = self.sign(&payload);
        let url = format!("{}{}?{}&signature={}", self.base_url, endpoint, payload, sig);
        let res = self.client.request(method, &url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await.map_err(|e| e.to_string())?;
        let status = res.status();
        let text = res.text().await.map_err(|e| e.to_string())?;
        if status.is_success() {
            serde_json::from_str(&text).map_err(|e| format!("解析响应失败: {} ({})", e, text))
        } else {
            Err(format!("HTTP {}: {}", status, text))
        }
    }

    // 交易规则: tick/step/minNotional (公开接口, 无需签名)
    pub async fn symbol_rules(&self, symbol: &str) -> Result<SpotSymbolRules, String> {
        let url = format!("{}/api/v3/exchangeInfo?symbol={}", self.base_url, symbol);
        let v: serde_json::Value = self.client.get(&url).send().await.map_err(|e| e.to_string())?
            .json().await.map_err(|e| e.to_string())?;
        if v.get("code").and_then(|c| c.as_i64()) == Some(-1121) {
            return Err(format!("该币种在币安现货市场不存在或已下架 ({})", symbol));
        }
        let filters = v["symbols"][0]["filters"].as_array().ok_or(format!("无 filters 字段: {}", v.to_string()))?;
        let mut rules = SpotSymbolRules { tick_size: 0.0, step_size: 0.0, min_notional: 5.0 };
        for f in filters {
            match f["filterType"].as_str().unwrap_or("") {
                "PRICE_FILTER" => rules.tick_size = f["tickSize"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                "LOT_SIZE" => rules.step_size = f["stepSize"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                "NOTIONAL" | "MIN_NOTIONAL" => {
                    if let Some(mn) = f["minNotional"].as_str().and_then(|s| s.parse().ok()) {
                        rules.min_notional = mn;
                    }
                }
                _ => {}
            }
        }
        if rules.tick_size <= 0.0 || rules.step_size <= 0.0 {
            return Err(format!("{} 交易规则不完整: {:?}", symbol, rules));
        }
        Ok(rules)
    }

    pub async fn last_price(&self, symbol: &str) -> Result<f64, String> {
        let url = format!("{}/api/v3/ticker/price?symbol={}", self.base_url, symbol);
        let v: serde_json::Value = self.client.get(&url).send().await.map_err(|e| e.to_string())?
            .json().await.map_err(|e| e.to_string())?;
        v["price"].as_str().and_then(|s| s.parse().ok()).ok_or_else(|| "无价格字段".to_string())
    }

    // 限价单 (GTC)。返回 orderId。数量/价格按交易规则取整后再发。
    pub async fn place_limit(&self, symbol: &str, side: &str, qty: f64, price: f64, rules: &SpotSymbolRules) -> Result<u64, String> {
        let q = round_step(qty, rules.step_size);
        let p = round_step(price, rules.tick_size);
        if q * p < rules.min_notional {
            return Err(format!("名义 {:.2} 低于最小限制 {:.2}", q * p, rules.min_notional));
        }
        // 用足够的小数位格式化, 再去掉尾零, 避免科学计数法被拒
        let qs = format!("{:.12}", q); let qs = qs.trim_end_matches('0').trim_end_matches('.');
        let ps = format!("{:.12}", p); let ps = ps.trim_end_matches('0').trim_end_matches('.');
        let params = format!("symbol={}&side={}&type=LIMIT&timeInForce=GTC&quantity={}&price={}", symbol, side, qs, ps);
        let v = self.signed_request(reqwest::Method::POST, "/api/v3/order", &params).await?;
        v["orderId"].as_u64().ok_or_else(|| format!("下单成功但无 orderId: {}", v))
    }

    // 市价卖出 (清算库存用)
    pub async fn market_sell(&self, symbol: &str, qty: f64, rules: &SpotSymbolRules) -> Result<f64, String> {
        let q = round_step(qty, rules.step_size);
        if q <= 0.0 {
            return Err("数量取整后为 0".to_string());
        }
        let qs = format!("{:.12}", q); let qs = qs.trim_end_matches('0').trim_end_matches('.');
        let params = format!("symbol={}&side=SELL&type=MARKET&quantity={}", symbol, qs);
        let v = self.signed_request(reqwest::Method::POST, "/api/v3/order", &params).await?;
        // 返回实际成交的报价资产总额
        Ok(v["cummulativeQuoteQty"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0))
    }

    pub async fn cancel_order(&self, symbol: &str, order_id: u64) -> Result<(), String> {
        let params = format!("symbol={}&orderId={}", symbol, order_id);
        self.signed_request(reqwest::Method::DELETE, "/api/v3/order", &params).await.map(|_| ())
    }

    // 查单个订单状态: 返回 (status, executedQty, cummulativeQuoteQty)
    pub async fn get_order(&self, symbol: &str, order_id: u64) -> Result<(String, f64, f64), String> {
        let params = format!("symbol={}&orderId={}", symbol, order_id);
        let v = self.signed_request(reqwest::Method::GET, "/api/v3/order", &params).await?;
        Ok((
            v["status"].as_str().unwrap_or("").to_string(),
            v["executedQty"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
            v["cummulativeQuoteQty"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
        ))
    }

    // 该交易对当前的在场挂单 orderId 集合
    pub async fn open_order_ids(&self, symbol: &str) -> Result<Vec<u64>, String> {
        let params = format!("symbol={}", symbol);
        let v = self.signed_request(reqwest::Method::GET, "/api/v3/openOrders", &params).await?;
        Ok(v.as_array().map(|a| a.iter().filter_map(|o| o["orderId"].as_u64()).collect()).unwrap_or_default())
    }

    // 查某资产可用余额
    pub async fn free_balance(&self, asset: &str) -> Result<f64, String> {
        let v = self.signed_request(reqwest::Method::GET, "/api/v3/account", "").await?;
        let balances = v["balances"].as_array().ok_or("无 balances 字段")?;
        for b in balances {
            if b["asset"].as_str() == Some(asset) {
                return Ok(b["free"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0));
            }
        }
        Ok(0.0)
    }
}
