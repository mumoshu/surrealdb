use crate::kvs;
use futures::lock::Mutex;
use std::sync::Arc;

pub type Transaction<'a> = Arc<Mutex<kvs::Transaction<'a>>>;
