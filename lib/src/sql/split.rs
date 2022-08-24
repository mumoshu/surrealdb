use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::idiom::{basic, Idiom};
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::multi::separated_list1;
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Splits<'a>(pub Vec<Split<'a>>);

impl <'a>Deref for Splits<'a> {
	type Target = Vec<Split<'a>>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl <'a>IntoIterator for Splits<'a> {
	type Item = Split<'a>;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl <'a>fmt::Display for Splits<'a> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"SPLIT ON {}",
			self.0.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", ")
		)
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Split<'a>(pub Idiom<'a>);

impl <'a>Deref for Split<'a> {
	type Target = Idiom<'a>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl <'a>fmt::Display for Split<'a> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

pub fn split(i: &str) -> IResult<&str, Splits> {
	let (i, _) = tag_no_case("SPLIT")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("ON"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = separated_list1(commas, split_raw)(i)?;
	Ok((i, Splits(v)))
}

fn split_raw(i: &str) -> IResult<&str, Split> {
	let (i, v) = basic(i)?;
	Ok((i, Split(v)))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::test::Parse;

	#[test]
	fn split_statement() {
		let sql = "SPLIT field";
		let res = split(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Splits(vec![Split(Idiom::parse("field"))]),);
		assert_eq!("SPLIT ON field", format!("{}", out));
	}

	#[test]
	fn split_statement_on() {
		let sql = "SPLIT ON field";
		let res = split(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Splits(vec![Split(Idiom::parse("field"))]),);
		assert_eq!("SPLIT ON field", format!("{}", out));
	}

	#[test]
	fn split_statement_multiple() {
		let sql = "SPLIT field, other.field";
		let res = split(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Splits(vec![Split(Idiom::parse("field")), Split(Idiom::parse("other.field")),])
		);
		assert_eq!("SPLIT ON field, other.field", format!("{}", out));
	}
}
