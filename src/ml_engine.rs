use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use arc_swap::ArcSwap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeuralNetwork {
    pub weights_input_hidden: [[f32; 4]; 5],
    pub bias_hidden: [f32; 4],
    pub weights_hidden_output: [f32; 4],
    pub bias_output: f32,
}

lazy_static::lazy_static! {
    pub static ref GLOBAL_NN: ArcSwap<NeuralNetwork> = ArcSwap::from_pointee(NeuralNetwork::default_weights());
}

impl NeuralNetwork {
    pub fn default_weights() -> Self {
        Self {
            weights_input_hidden: [
                [ 0.85, -0.42,  0.11, -0.99],
                [ 1.12,  0.77, -0.55,  0.23],
                [-2.30,  1.45,  0.88, -1.12],
                [ 0.44, -0.22,  1.75, -0.66],
                [-0.15,  0.68, -0.33,  1.05],
            ],
            bias_hidden: [0.10, -0.25, 0.05, -0.15],
            weights_hidden_output: [1.25, -0.85, 0.65, -1.05],
            bias_output: -0.20,
        }
    }

    #[inline(always)]
    fn relu(x: f32) -> f32 {
        if x > 0.0 { x } else { 0.0 }
    }

    #[inline(always)]
    fn fast_sigmoid(x: f32) -> f32 {
        0.5 * (x / (1.0 + x.abs())) + 0.5
    }

    pub fn forward(&self, features: &[f32; 5]) -> f32 {
        let mut hidden = [0.0f32; 4];
        
        for i in 0..4 {
            let mut sum = self.bias_hidden[i];
            for j in 0..5 {
                sum += features[j] * self.weights_input_hidden[j][i];
            }
            hidden[i] = Self::relu(sum);
        }

        let mut output = self.bias_output;
        for i in 0..4 {
            output += hidden[i] * self.weights_hidden_output[i];
        }

        Self::fast_sigmoid(output)
    }
}

pub struct MLEngine;

impl MLEngine {
    pub fn predict_win_rate(
        obi: Decimal, 
        taker_buy: Decimal, 
        taker_sell: Decimal, 
        funding_rate: Decimal, 
        side: &str,
        mid_price_history: &std::collections::VecDeque<Decimal>
    ) -> Decimal {
        
        let obi_f32 = obi.to_f32().unwrap_or(0.0);
        let f_obi = if side == "BUY" { obi_f32 } else { -obi_f32 };

        let buy_f32 = taker_buy.to_f32().unwrap_or(0.0);
        let sell_f32 = taker_sell.to_f32().unwrap_or(0.0);
        let flow_ratio = if side == "BUY" {
            if sell_f32 > 0.0 { (buy_f32 / sell_f32).ln() } else { 2.0 }
        } else {
            if buy_f32 > 0.0 { (sell_f32 / buy_f32).ln() } else { 2.0 }
        };
        let f_flow = flow_ratio.clamp(-3.0, 3.0);

        let rate_f32 = funding_rate.to_f32().unwrap_or(0.0);
        let f_funding = if side == "BUY" { -rate_f32 * 1000.0 } else { rate_f32 * 1000.0 };

        let f_volatility = if mid_price_history.len() > 10 {
            let first = mid_price_history.front().unwrap().to_f32().unwrap_or(0.0);
            let last = mid_price_history.back().unwrap().to_f32().unwrap_or(0.0);
            ((last - first) / first * 10000.0).abs()
        } else {
            0.0
        };

        let f_momentum = if side == "BUY" { f_volatility * 0.5 } else { -f_volatility * 0.5 };

        let features = [
            f_obi,
            f_flow,
            f_funding.clamp(-2.0, 2.0),
            f_volatility.clamp(0.0, 5.0),
            f_momentum.clamp(-3.0, 3.0),
        ];

        // 极速无锁读取 (Lock-free Read) 提取当前权重进行前向传播
        let nn_guard = GLOBAL_NN.load();
        let prob = nn_guard.forward(&features);

        Decimal::from_f32_retain(prob).unwrap_or(dec!(0.5))
    }
}
