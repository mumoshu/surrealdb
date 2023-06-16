use derive::Key;
use serde::{Deserialize, Serialize};

// Tt stands for "T"able "t"imestamp
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Tt {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_d: u8,
	_e: u8,
	_f: u8,
}

#[allow(unused)]
pub fn new(ns: &str, db: &str) -> Tt {
	 Tt::new(ns.to_string(), db.to_string())
}

impl Tt {
	pub fn new(ns: String, db: String) -> Tt {
		Tt {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_d: b'!',
			_e: b't',
			_f: b't', 
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Tt::new(
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Tt::encode(&val).unwrap();
		let dec = Tt::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
