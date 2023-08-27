use serde::Serialize;
use std::fmt;
use tokio_postgres::Row;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum VolumeType {
    Base,
    Quote,
}
impl fmt::Display for VolumeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VolumeType::Base => write!(f, "Base"),
            VolumeType::Quote => write!(f, "Quote"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Trader {
    pub pubkey: String,
    pub volume: f64,
}
impl Trader {
    pub fn from_row(row: Row) -> Self {
        Trader {
            pubkey: row.get(0),
            volume: row.get(1),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TraderResponse {
    pub start_time: u64,
    pub end_time: u64,
    pub volume_type: String,
    pub traders: Vec<Trader>,
}
