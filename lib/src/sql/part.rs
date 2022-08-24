use crate::sql::comment::shouldbespace;
use crate::sql::ending::ident as ending;
use crate::sql::error::IResult;
use crate::sql::graph::{graph as graph_raw, Graph};
use crate::sql::ident::{ident, Ident};
use crate::sql::idiom::Idiom;
use crate::sql::number::{number, Number};
use crate::sql::thing::{thing as thing_raw, Thing};
use crate::sql::value::{value, Value};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Part<'a> {
	Any,
	All,
	Last,
	First,
	Field(Ident),
	Index(Number),
	Where(Value<'a>),
	Thing(Thing),
	Graph(Graph<'a>),
}

impl <'a>From<i32> for Part<'a> {
	fn from(v: i32) -> Self {
		Part::Index(v.into())
	}
}

impl <'a>From<isize> for Part<'a> {
	fn from(v: isize) -> Self {
		Part::Index(v.into())
	}
}

impl <'a>From<usize> for Part<'a> {
	fn from(v: usize) -> Self {
		Part::Index(v.into())
	}
}

impl <'a>From<Number> for Part<'a> {
	fn from(v: Number) -> Self {
		Part::Index(v)
	}
}

impl <'a>From<Ident> for Part<'a> {
	fn from(v: Ident) -> Self {
		Part::Field(v)
	}
}

impl <'a>From<Value<'a>> for Part<'a> {
	fn from(v: Value) -> Self {
		Part::Where(v)
	}
}

impl <'a>From<Thing> for Part<'a> {
	fn from(v: Thing) -> Self {
		Part::Thing(v)
	}
}

impl <'a>From<Graph<'a>> for Part<'a> {
	fn from(v: Graph) -> Self {
		Part::Graph(v)
	}
}

impl <'a>From<String> for Part<'a> {
	fn from(v: String) -> Self {
		Part::Field(Ident(v))
	}
}

impl <'a>From<&str> for Part<'a> {
	fn from(v: &str) -> Self {
		match v.parse::<isize>() {
			Ok(v) => Part::from(v),
			_ => Part::from(v.to_owned()),
		}
	}
}

impl <'a>Part<'a> {
	// Returns a yield if an alias is specified
	pub(crate) fn alias(&self) -> Option<&Idiom> {
		match self {
			Part::Graph(v) => v.alias.as_ref(),
			_ => None,
		}
	}
}

impl <'a>fmt::Display for Part<'a> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Part::Any => write!(f, ".."),
			Part::All => write!(f, "[*]"),
			Part::Last => write!(f, "[$]"),
			Part::First => write!(f, "[0]"),
			Part::Field(v) => write!(f, ".{}", v),
			Part::Index(v) => write!(f, "[{}]", v),
			Part::Where(v) => write!(f, "[WHERE {}]", v),
			Part::Thing(v) => write!(f, "{}", v),
			Part::Graph(v) => write!(f, "{}", v),
		}
	}
}

// ------------------------------

pub trait Next<'a> {
	fn next(&'a self) -> &[Part];
}

impl<'a> Next<'a> for &'a [Part<'a>] {
	fn next(&'a self) -> &'a [Part] {
		match self.len() {
			0 => &[],
			_ => &self[1..],
		}
	}
}

// ------------------------------

pub fn part(i: &str) -> IResult<&str, Part> {
	alt((all, last, index, field, graph, filter))(i)
}

pub fn first(i: &str) -> IResult<&str, Part> {
	let (i, v) = ident(i)?;
	let (i, _) = ending(i)?;
	Ok((i, Part::Field(v)))
}

pub fn all(i: &str) -> IResult<&str, Part> {
	let (i, _) = alt((
		|i| {
			let (i, _) = char('.')(i)?;
			let (i, _) = char('*')(i)?;
			Ok((i, ()))
		},
		|i| {
			let (i, _) = char('[')(i)?;
			let (i, _) = char('*')(i)?;
			let (i, _) = char(']')(i)?;
			Ok((i, ()))
		},
	))(i)?;
	Ok((i, Part::All))
}

pub fn last(i: &str) -> IResult<&str, Part> {
	let (i, _) = char('[')(i)?;
	let (i, _) = char('$')(i)?;
	let (i, _) = char(']')(i)?;
	Ok((i, Part::Last))
}

pub fn index(i: &str) -> IResult<&str, Part> {
	let (i, _) = char('[')(i)?;
	let (i, v) = number(i)?;
	let (i, _) = char(']')(i)?;
	Ok((i, Part::Index(v)))
}

pub fn field(i: &str) -> IResult<&str, Part> {
	let (i, _) = char('.')(i)?;
	let (i, v) = ident(i)?;
	let (i, _) = ending(i)?;
	Ok((i, Part::Field(v)))
}

pub fn filter(i: &str) -> IResult<&str, Part> {
	let (i, _) = char('[')(i)?;
	let (i, _) = alt((tag_no_case("WHERE"), tag("?")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = value(i)?;
	let (i, _) = char(']')(i)?;
	Ok((i, Part::Where(v)))
}

pub fn thing(i: &str) -> IResult<&str, Part> {
	let (i, v) = thing_raw(i)?;
	Ok((i, Part::Thing(v)))
}

pub fn graph(i: &str) -> IResult<&str, Part> {
	let (i, v) = graph_raw(i)?;
	Ok((i, Part::Graph(v)))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::expression::Expression;
	use crate::sql::test::Parse;

	#[test]
	fn part_all() {
		let sql = "[*]";
		let res = part(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[*]", format!("{}", out));
		assert_eq!(out, Part::All);
	}

	#[test]
	fn part_last() {
		let sql = "[$]";
		let res = part(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[$]", format!("{}", out));
		assert_eq!(out, Part::Last);
	}

	#[test]
	fn part_number() {
		let sql = "[0]";
		let res = part(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[0]", format!("{}", out));
		assert_eq!(out, Part::Index(Number::from("0")));
	}

	#[test]
	fn part_expression_question() {
		let sql = "[? test = true]";
		let res = part(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[WHERE test = true]", format!("{}", out));
		assert_eq!(out, Part::Where(Value::from(Expression::parse("test = true"))));
	}

	#[test]
	fn part_expression_condition() {
		let sql = "[WHERE test = true]";
		let res = part(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[WHERE test = true]", format!("{}", out));
		assert_eq!(out, Part::Where(Value::from(Expression::parse("test = true"))));
	}
}
