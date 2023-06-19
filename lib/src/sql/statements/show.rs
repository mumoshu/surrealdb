use crate::ctx::Context;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::table::{table, Table};
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::combinator::opt;
use nom::combinator::recognize;
use nom::sequence::preceded;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub struct ShowStatement {
	pub table: Option<String>,
	pub since: Option<u64>,
	pub limit: Option<u32>,
}

impl ShowStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Selected DB?
		opt.needs(Level::Db)?;
		// Allowed to run?
		opt.check(Level::Db)?;
		// Allowed to run?
		opt.check(Level::No)?;
		// Clone transaction
		let run = txn.clone();
		// Claim transaction
		let mut run = run.lock().await;
		// Process the show query
		let tb = self.table.as_ref().map(|x| x.as_str());
		let r = crate::cf::read(&mut run, opt.ns(), opt.db(), tb, self.since, self.limit).await?;
		// Return the changes
		let mut a = Vec::<Value>::new();
		for r in r.iter() {
			let v: Value = r.clone().to_value();
			a.push(v);
		}
		let v: Value = Value::Array(crate::sql::array::Array(a));
		Ok(v)
	}
}

impl fmt::Display for ShowStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "SHOW CHANGES FOR")?;
		match self.table {
			Some(ref v) => write!(f, " TABLE {}", v)?,
			None => write!(f, " DATABASE")?,
		}
		match self.since {
			Some(ref v) => write!(f, " SINCE {}", v)?,
			None => (),
		}
		match self.limit {
			Some(ref v) => write!(f, " LIMIT {}", v)?,
			None => (),
		}
		Ok(())
	}
}

pub fn table_or_database(i: &str) -> IResult<&str, Option<String>> {
	let (i, v) = alt((
		map(preceded(tag_no_case("table"), preceded(shouldbespace, table)), |v: Table| Some(v.0)),
		map(tag_no_case("database"), |_| None),
	))(i)?;
	Ok((i, v))
}

pub fn int_str(i: &str) -> IResult<&str, &str> {
	use nom::{
		character::complete::{char, one_of},
		multi::{many0, many1},
		sequence::terminated,
	};

	recognize(many1(terminated(one_of("0123456789"), many0(char('_')))))(i)
}

pub fn since(i: &str) -> IResult<&str, u64> {
	use std::num::ParseIntError;

	let (i, _) = tag_no_case("SINCE")(i)?;
	let (i, _) = shouldbespace(i)?;

	let r = map(int_str, |v| {
		let r: Result<u64, ParseIntError> = v.parse();
		let r = r.unwrap();
		r
	})(i);

	r
}

pub fn limit(i: &str) -> IResult<&str, u32> {
	use std::num::ParseIntError;

	let (i, _) = tag_no_case("LIMIT")(i)?;
	let (i, _) = shouldbespace(i)?;

	let r = map(int_str, |v| {
		let r: Result<u32, ParseIntError> = v.parse();
		let r = r.unwrap();
		r
	})(i);

	r
}

pub fn show(i: &str) -> IResult<&str, ShowStatement> {
	let (i, _) = tag_no_case("SHOW CHANGES")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FOR")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, table) = table_or_database(i)?;
	let (i, since) = opt(preceded(shouldbespace, since))(i)?;
	let (i, limit) = opt(preceded(shouldbespace, limit))(i)?;
	Ok((
		i,
		ShowStatement {
			table,
			since,
			limit,
		},
	))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn tb() {
		let sql = "TABLE person";
		let res = table_or_database(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1.unwrap();
		assert_eq!("person", format!("{}", out))
	}

	#[test]
	fn db() {
		let sql = "DATABASE";
		let res = table_or_database(sql);
		assert!(res.is_ok());
		assert!(res.unwrap().1.is_none())
	}

	#[test]
	fn show_table_changes() {
		let sql = "SHOW CHANGES FOR TABLE person";
		let res = show(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_table_changes_since() {
		let sql = "SHOW CHANGES FOR TABLE person SINCE 0";
		let res = show(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_table_changes_since_limit() {
		let sql = "SHOW CHANGES FOR TABLE person SINCE 0 LIMIT 10";
		let res = show(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_database_changes() {
		let sql = "SHOW CHANGES FOR DATABASE";
		let res = show(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_database_changes_since() {
		let sql = "SHOW CHANGES FOR DATABASE SINCE 0";
		let res = show(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_database_changes_since_limit() {
		let sql = "SHOW CHANGES FOR DATABASE SINCE 0 LIMIT 10";
		let res = show(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}
}
