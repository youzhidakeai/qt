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
        book.bids.clear();
        book.asks.clear();

        for b in bids {
            let price = Decimal::from_str(&b[0]).unwrap_or_default();
            let qty = Decimal::from_str(&b[1]).unwrap_or_default();
            if !qty.is_zero() {
                book.bids.insert(price, qty);
            }
        }

        for a in asks {
            let price = Decimal::from_str(&a[0]).unwrap_or_default();
            let qty = Decimal::from_str(&a[1]).unwrap_or_default();
            if !qty.is_zero() {
                book.asks.insert(price, qty);
            }
        }
    }

    pub fn get_thickest_wall(&self, is_bid: bool, levels: usize) -> Option<Decimal> {
        let book = self.book.read().expect("RwLock poisoned");
        let mut best_price = None;
        let mut max_qty = Decimal::ZERO;
        
        if is_bid {
            for (price, qty) in book.bids.iter().rev().take(levels) {
                if *qty > max_qty {
                    max_qty = *qty;
                    best_price = Some(*price);
                }
            }
        } else {
            for (price, qty) in book.asks.iter().take(levels) {
                if *qty > max_qty {
                    max_qty = *qty;
                    best_price = Some(*price);
                }
            }
        }
        best_price
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
