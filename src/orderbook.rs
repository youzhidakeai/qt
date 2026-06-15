use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::sync::RwLock;
use std::str::FromStr;

#[derive(Debug, Default)]
pub struct OrderBook {
    // Normal BTreeMap iterates from lowest to highest.
    // Asks: Lowest price is best, so .iter().next() is Best Ask.
    // Bids: Highest price is best, so .iter().next_back() is Best Bid.
    pub bids: BTreeMap<Decimal, Decimal>,
    pub asks: BTreeMap<Decimal, Decimal>,
}

pub struct OrderBookManager {
    pub symbol: String,
    pub book: RwLock<OrderBook>,
}

impl OrderBookManager {
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            book: RwLock::new(OrderBook::default()),
        }
    }

    pub fn apply_update(&self, bids: &[[String; 2]], asks: &[[String; 2]]) {
        let mut book = self.book.write().expect("RwLock poisoned");

        for b in bids {
            let price = Decimal::from_str(&b[0]).unwrap_or_default();
            let qty = Decimal::from_str(&b[1]).unwrap_or_default();
            if qty.is_zero() {
                book.bids.remove(&price);
            } else {
                book.bids.insert(price, qty);
            }
        }

        for a in asks {
            let price = Decimal::from_str(&a[0]).unwrap_or_default();
            let qty = Decimal::from_str(&a[1]).unwrap_or_default();
            if qty.is_zero() {
                book.asks.remove(&price);
            } else {
                book.asks.insert(price, qty);
            }
        }
    }

    pub fn print_top_of_book(&self) {
        let book = self.book.read().expect("RwLock poisoned");
        
        let best_bid = book.bids.iter().next_back();
        let best_ask = book.asks.iter().next();

        match (best_bid, best_ask) {
            (Some((bid_p, bid_q)), Some((ask_p, ask_q))) => {
                tracing::info!(
                    "[{}] LOB -> Bid: {} ({}) | Ask: {} ({})",
                    self.symbol, bid_p, bid_q, ask_p, ask_q
                );
            }
            _ => {
                // Ignore until we have both sides
            }
        }
    }
}
