use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum ControlIn {
    Subscribe { topics: Vec<String> },
    Unsubscribe { topics: Vec<String> },
    Replay { topic: String, last_n: usize },
}

#[derive(Debug, Serialize)]
pub struct ReplayOut {
    pub op: &'static str,
    pub topic: String,
    pub rows: Vec<crate::stream::row::IlpRow>,
}
