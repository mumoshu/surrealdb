use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::number::Number;
use crate::sql::operation::Operation;
use crate::sql::serde::is_internal_serialization;
use crate::sql::strand::Strand;
use crate::sql::value::{value, Value};
use nom::character::complete::char;
use nom::combinator::opt;
use nom::multi::separated_list0;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops;
use std::ops::Deref;
use std::ops::DerefMut;
use std::borrow::Cow;

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Deserialize)]
pub struct Array<'a>(pub CowValueVec<'a>);


#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Deserialize)]
pub struct CowValueVec<'a>(pub Vec<Cow<'a, Value<'a>>>);

fn newArray<'a>(v: Vec<Value>) -> Array<'a> {
	Array(CowValueVec(v.iter().map(|x| Cow::Borrowed(x))))
}

impl <'a>From<Value<'a>> for Array<'a> {
	fn from(v: Value) -> Self {
		Array(vec![v])
	}
}

impl <'a>From<Vec<Value<'a>>> for Array<'a> {
	fn from(v: Vec<Value>) -> Self {
		Array(v)
	}
}

impl <'a>From<Vec<i32>> for Array<'a> {
	fn from(v: Vec<i32>) -> Self {
		Array(v.into_iter().map(Value::from).collect())
	}
}

impl <'a>From<Vec<&str>> for Array<'a> {
	fn from(v: Vec<&str>) -> Self {
		Array(v.into_iter().map(Value::from).collect())
	}
}

impl <'a>From<Vec<Operation>> for Array<'a> {
	fn from(v: Vec<Operation>) -> Self {
		Array(v.into_iter().map(Value::from).collect())
	}
}

impl <'a>Deref for Array<'a> {
	type Target = Vec<Value<'a>>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl <'a>DerefMut for Array<'a> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl <'a>IntoIterator for Array<'a> {
	type Item = Value<'a>;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl <'a>Array<'a> {
	pub fn new() -> Self {
		Array(Vec::default())
	}

	pub fn with_capacity(len: usize) -> Self {
		Array(Vec::with_capacity(len))
	}

	pub fn as_ints(self) -> Vec<i64> {
		self.0.into_iter().map(|v| v.as_int()).collect()
	}

	pub fn as_floats(self) -> Vec<f64> {
		self.0.into_iter().map(|v| v.as_float()).collect()
	}

	pub fn as_numbers(self) -> Vec<Number> {
		self.0.into_iter().map(|v| v.as_number()).collect()
	}

	pub fn as_strands(self) -> Vec<Strand> {
		self.0.into_iter().map(|v| v.as_strand()).collect()
	}

	pub fn as_point(mut self) -> [f64; 2] {
		match self.len() {
			0 => [0.0, 0.0],
			1 => [self.0.remove(0).as_float(), 0.0],
			_ => [self.0.remove(0).as_float(), self.0.remove(0).as_float()],
		}
	}
}

impl <'a>Array<'a> {
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value<'a>>,
	) -> Result<Value, Error> {
		let mut x = Vec::new();
		for v in self.iter() {
			match v.compute(ctx, opt, txn, doc).await {
				Ok(v) => x.push(v),
				Err(e) => return Err(e),
			};
		}
		Ok(Value::Array(&Array(x)))
	}
}

impl <'a>fmt::Display for Array<'a> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "[{}]", self.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", "))
	}
}

impl <'a>Serialize for Array<'a> {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			serializer.serialize_newtype_struct("Array", &self.0)
		} else {
			serializer.serialize_some(&self.0)
		}
	}
}

// ------------------------------

impl <'a>ops::Add<Value<'a>> for Array<'a> {
	type Output = Self;
	fn add(mut self, other: Value) -> Self {
		if !self.0.iter().any(|x| *x == other) {
			self.0.push(other)
		}
		self
	}
}

impl <'a>ops::Add for Array<'a> {
	type Output = Self;
	fn add(mut self, other: Self) -> Self {
		for v in other.0 {
			if !self.0.iter().any(|x| *x == v) {
				self.0.push(v)
			}
		}
		self
	}
}

// ------------------------------

impl <'a>ops::Sub<Value<'a>> for Array<'a> {
	type Output = Self;
	fn sub(mut self, other: Value<'a>) -> Self {
		if let Some(p) = self.0.iter().position(|x| *x == other) {
			self.0.remove(p);
		}
		self
	}
}

impl <'a>ops::Sub for Array<'a> {
	type Output = Self;
	fn sub(mut self, other: Self) -> Self {
		for v in other.0 {
			if let Some(p) = self.0.iter().position(|x| *x == v) {
				self.0.remove(p);
			}
		}
		self
	}
}

// ------------------------------

pub trait Abolish<T> {
	fn abolish<F>(&mut self, f: F)
	where
		F: FnMut(usize) -> bool;
}

impl<T> Abolish<T> for Vec<T> {
	fn abolish<F>(&mut self, mut f: F)
	where
		F: FnMut(usize) -> bool,
	{
		let len = self.len();
		let mut del = 0;
		{
			let v = &mut **self;

			for i in 0..len {
				if f(i) {
					del += 1;
				} else if del > 0 {
					v.swap(i - del, i);
				}
			}
		}
		if del > 0 {
			self.truncate(len - del);
		}
	}
}

// ------------------------------

pub trait Combine<T> {
	fn combine(self, other: T) -> T;
}

impl <'a>Combine<Array<'a>> for Array<'a> {
	fn combine(self, other: Array) -> Array {
		let mut out = Array::new();
		for a in self.iter() {
			for b in other.iter() {
				out.push(vec![a.clone(), b.clone()].into());
			}
		}
		out
	}
}

// ------------------------------

pub trait Concat<T> {
	fn concat(self, other: T) -> T;
}

impl <'a>Concat<Array<'a>> for Array<'a> {
	fn concat(mut self, mut other: Array) -> Array {
		self.append(&mut other);
		self
	}
}

// ------------------------------

pub trait Difference<T> {
	fn difference(self, other: T) -> T;
}

impl <'a>Difference<Array<'a>> for Array<'a> {
	fn difference(self, other: Array) -> Array {
		let mut out = Array::new();
		let mut other: Vec<_> = other.into_iter().collect();
		for a in self.into_iter() {
			if let Some(pos) = other.iter().position(|b| a == *b) {
				other.remove(pos);
			} else {
				out.push(a);
			}
		}
		out.append(&mut other);
		out
	}
}

// ------------------------------

pub trait Intersect<T> {
	fn intersect(self, other: T) -> T;
}

impl <'a>Intersect<Array<'a>> for Array<'a> {
	fn intersect(self, other: Array) -> Array {
		let mut out = Array::new();
		let mut other: Vec<_> = other.into_iter().collect();
		for a in self.0.into_iter() {
			if let Some(pos) = other.iter().position(|b| a == *b) {
				out.push(a);
				other.remove(pos);
			}
		}
		out
	}
}

// ------------------------------

pub trait Union<T> {
	fn union(self, other: T) -> T;
}

impl <'a>Union<Array<'a>> for Array<'a> {
	fn union(mut self, mut other: Array) -> Array {
		self.append(&mut other);
		self.uniq()
	}
}

// ------------------------------

pub trait Uniq<T> {
	fn uniq(self) -> T;
}

impl <'a>Uniq<Array<'a>> for Array<'a> {
	fn uniq(mut self) -> Array<'a> {
		for x in (0..self.len()).rev() {
			for y in (x + 1..self.len()).rev() {
				if self[x] == self[y] {
					self.remove(y);
				}
			}
		}
		self
	}
}

// ------------------------------

pub fn array(i: &str) -> IResult<&str, Array> {
	let (i, _) = char('[')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list0(commas, item)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = opt(char(','))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(']')(i)?;
	Ok((i, Array(v)))
}

fn item(i: &str) -> IResult<&str, Value> {
	let (i, v) = value(i)?;
	Ok((i, v))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn array_normal() {
		let sql = "[1,2,3]";
		let res = array(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3]", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn array_commas() {
		let sql = "[1,2,3,]";
		let res = array(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3]", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn array_expression() {
		let sql = "[1,2,3+1]";
		let res = array(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3 + 1]", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}
}
