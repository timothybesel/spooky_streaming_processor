use cbor4ii::core as cbor;
use serde::Deserialize;
//use surrealdb::types::RecordId;

#[derive(Deserialize, Debug)]
pub struct IngestRequest {
    pub table: String,
    pub op: String,
    pub id: String,
    pub record: cbor::Value,
}

pub fn ingest_handler(body: &[u8]) {
    let payload: IngestRequest = match cbor4ii::serde::from_slice(body) {
        Ok(p) => p,
        Err(_) => panic!("error beim Deserialize of cbor bytes"),
    };
}
