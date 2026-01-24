use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct Request {
    pub id: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Serialize)]
pub struct ResponseOk<T: Serialize> {
    pub id: String,
    pub result: T,
}

#[derive(Debug, Serialize)]
pub struct ResponseErr {
    pub id: String,
    pub error: String,
}


