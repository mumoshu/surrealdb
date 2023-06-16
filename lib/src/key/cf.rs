use derive::Key;
use serde::{Deserialize, Serialize};

use std::str;

pub type Versionstamp = [u8; 10];

// u64_to_versionstamp converts a u64 to a 10-byte versionstamp
// assuming big-endian and the the last two bytes are zero.
pub fn u64_to_versionstamp(v: u64) -> [u8; 10] {
	let mut buf = [0; 10];
	buf[0] = (v >> 56) as u8;
	buf[1] = (v >> 48) as u8;
	buf[2] = (v >> 40) as u8;
	buf[3] = (v >> 32) as u8;
	buf[4] = (v >> 24) as u8;
	buf[5] = (v >> 16) as u8;
	buf[6] = (v >> 8) as u8;
	buf[7] = v as u8;
	buf
}

// u128_to_versionstamp converts a u128 to a 10-byte versionstamp
// assuming big-endian.
#[allow(unused)]
pub fn u128_to_versionstamp(v: u128) -> [u8; 10] {
	let mut buf = [0; 10];
	buf[0] = (v >> 72) as u8;
	buf[1] = (v >> 64) as u8;
	buf[2] = (v >> 56) as u8;
	buf[3] = (v >> 48) as u8;
	buf[4] = (v >> 40) as u8;
	buf[5] = (v >> 32) as u8;
	buf[6] = (v >> 24) as u8;
	buf[7] = (v >> 16) as u8;
	buf[8] = (v >> 8) as u8;
	buf[9] = v as u8;
	buf
}

// to_u128_be converts a 10-byte versionstamp to a u128 assuming big-endian.
// This is handy for human comparing versionstamps.
pub fn to_u128_be(vs: [u8; 10]) -> u128 {
	let mut buf = [0; 16];
	let mut i = 0;
	while i < 10 {
		buf[i + 6] = vs[i];
		i += 1;
	}
	u128::from_be_bytes(buf)
}

// to_u64_be converts a 10-byte versionstamp to a u64 assuming big-endian.
// Only the first 8 bytes are used.
pub fn to_u64_be(vs: [u8; 10]) -> u64 {
	let mut buf = [0; 8];
	let mut i = 0;
	while i < 8 {
		buf[i] = vs[i];
		i += 1;
	}
	u64::from_be_bytes(buf)
}

// to_u128_le converts a 10-byte versionstamp to a u128 assuming little-endian.
// This is handy for producing human-readable versions of versionstamps.
#[allow(unused)]
pub fn to_u128_le(vs: [u8; 10]) -> u128 {
	let mut buf = [0; 16];
	let mut i = 0;
	while i < 10 {
		buf[i] = vs[i];
		i += 1;
	}
	u128::from_be_bytes(buf)
}

// Cf stands for change feeds
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Cf {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_d: u8,
	_e: u8,
	_f: u8,
	pub ts: [u8; 10],
	_c: u8,
	pub tb: String,
}

#[allow(unused)]
pub fn new(ns: &str, db: &str, ts: u64, tb: &str) -> Cf {
	 Cf::new(ns.to_string(), db.to_string(), u64_to_versionstamp(ts), tb.to_string())
}

#[allow(unused)]
pub fn versionstamped_key_prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'c', b'f']);
	k
}

#[allow(unused)]
pub fn versionstamped_key_suffix(tb: &str) -> Vec<u8> {
	let mut k: Vec<u8> = vec![];
	k.extend_from_slice(&[b'*']);
	k.extend_from_slice(tb.as_bytes());
	// Without this, decoding fails with UnexpectedEOF errors
	k.extend_from_slice(&[0x00]);
	k
}


#[allow(unused)]
pub fn ts_prefix(ns: &str, db: &str, ts: Versionstamp) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'c', b'f']);
	k.extend_from_slice(&ts);
	k
}

#[allow(unused)]
pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'c', b'f', 0x00]);
	k
}

#[allow(unused)]
pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'c', b'f', 0xff]);
	k
}

impl Cf {
	pub fn new(ns: String, db: String, ts: [u8; 10], tb: String) -> Cf {
		Cf {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_d: b'!',
			_e: b'c',
			_f: b'f',
			ts: ts,
			_c: b'*',
			tb,
		}
	}
}

#[cfg(test)]
mod tests {
	use std::ascii::escape_default;

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Cf::new(
			"test".to_string(),
			"test".to_string(),
			super::u128_to_versionstamp(12345),
			"test".to_string(),
		);
		let enc = Cf::encode(&val).unwrap();
		println!("enc={}", show(&enc));
		let dec = Cf::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn versionstamp_conversions() {
		use super::*;

		let a = u64_to_versionstamp(12345);
		let b = to_u64_be(a);
		assert_eq!(12345, b);

		let a = u128_to_versionstamp(12345);
		let b = to_u128_be(a);
		assert_eq!(12345, b);
	}

	fn show(bs: &[u8]) -> String {
		let mut visible = String::new();
		for &b in bs {
			let part: Vec<u8> = escape_default(b).collect();
			visible.push_str(std::str::from_utf8(&part).unwrap());
		}
		visible
	}
}
