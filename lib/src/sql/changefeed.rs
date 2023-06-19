use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use crate::sql::array::Array;
use crate::sql::object::Object;
use crate::vs::to_u128_be;
use nom::bytes::complete::tag_no_case;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::str;
use std::time;

use derive::Store;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct ChangeFeed {
    pub enabled: bool,
	pub expiry: time::Duration,
}

impl ChangeFeed {
	pub fn none() -> Self {
		ChangeFeed {
			enabled: false,
			expiry: time::Duration::from_secs(0),
		}
	}

	pub fn enabled(d: time::Duration) -> Self {
		ChangeFeed {
			enabled: true,
			expiry: d,
		}
	}
}

impl Display for ChangeFeed {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		if !self.enabled {
			return Ok(());
		};

		write!(f, "CHANGEFEED {}", crate::sql::duration::Duration(self.expiry))?;
		Ok(())
	}
}

pub fn changefeed(i: &str) -> IResult<&str, ChangeFeed> {
	let (i, _) = tag_no_case("CHANGEFEED")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = crate::sql::duration::duration(i)?;
	Ok((
		i,
		ChangeFeed {
			enabled: true,
			expiry: v.0,
		},
	))
}

impl Default for ChangeFeed {
	fn default() -> Self {
		Self::none()
	}
}

// Mutation is a single mutation to a table.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
pub enum TableMutation {
	// Althouhgh the Value is supposed to contain a field "id" of Thing,
	// we do include it in the first field for convenience.
	Set(Thing, Value),
	Del(Thing),
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
pub struct TableMutations(pub String, pub Vec<TableMutation>);

impl TableMutations {
	pub fn new(tb: String) -> Self {
		Self(tb, Vec::new())
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
pub struct DatabaseMutation(pub Vec<TableMutations>);

impl DatabaseMutation {
	pub fn new() -> Self {
		Self(Vec::new())
	}
}

// Change is a set of mutations made to a table at the specific timestamp.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
pub struct ChangeSet(pub [u8; 10], pub DatabaseMutation);

impl TableMutation {
	pub fn to_value(&self) -> Value {
		let (k, v) = match self {
			TableMutation::Set(_t, v) => {
				("update".to_string(), v.clone())
			}
			TableMutation::Del(t) => {
				let mut h = BTreeMap::<String, Value>::new();
				h.insert("id".to_string(), Value::Thing(t.clone()));
				let o = Object::from(h);
				("delete".to_string(), Value::Object(o))
			}
		};

		let mut h = BTreeMap::<String, Value>::new();
		h.insert(k, v);
		let o = crate::sql::object::Object::from(h);
		Value::Object(o)
	}
}

impl DatabaseMutation {
	pub fn to_value(self) -> Value {
		let mut changes = Vec::<Value>::new();
		for tbs in self.0 {
			for tb in tbs.1 {
				changes.push(tb.to_value());
			}
		}
		Value::Array(Array::from(changes))
	}
}

impl ChangeSet {
	pub fn to_value(self) -> Value {
		let mut m = BTreeMap::<String, Value>::new();
		let vs = to_u128_be(self.0);
		m.insert("versionstamp".to_string(), Value::from(vs));
		m.insert("changes".to_string(), self.1.to_value());
		let so: Object = m.into();
		Value::Object(so)
	}
}

impl Display for TableMutation {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			TableMutation::Set(id, v) => write!(f, "SET {} {}", id, v),
			TableMutation::Del(id) => write!(f, "DEL {}", id),
		}
	}
}

impl Display for TableMutations {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		let tb = &self.0;
		let muts = &self.1;
		write!(f, "{}", tb)?;
		muts.iter().try_for_each(|v| write!(f, "{}", v))
	}
}


impl Display for DatabaseMutation {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		let x = &self.0;

		x.iter().try_for_each(|v| write!(f, "{}", v))
	}
}

impl Display for ChangeSet {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		let x = &self.1;

		write!(f, "{}", x)
	}
}

// WriteMutationSet is a set of mutations to be to a table at the specific timestamp.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
pub struct WriteMutationSet(pub Vec<TableMutations>);

impl WriteMutationSet {
	pub fn new() -> Self {
		Self(Vec::new())
	}
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn changefeed_none() {
		let sql: &str = "";
		let res = changefeed(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("", format!("{}", out));
		assert_eq!(out, ChangeFeed::none());
	}

	#[test]
	fn changefeed_enabled() {
		let sql = "CHANGEFEED 1h";
		let res = changefeed(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CHANGEFEED 1h", format!("{}", out));
		assert_eq!(out, ChangeFeed::enabled(time::Duration::from_secs(3600)));
	}
}
