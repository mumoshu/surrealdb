use crate::err::Error;
use crate::sql::value::Value;
use std::ops::Add;
use std::ops::Div;
use std::ops::Mul;
use std::ops::Sub;

pub fn or<'a>(a: Value<'a>, b: Value<'a>) -> Result<Value<'a>, Error<'a>> {
	match a.is_truthy() {
		true => Ok(a),
		false => Ok(b),
	}
}

pub fn and<'a>(a: Value<'a>, b: Value<'a>) -> Result<Value<'a>, Error<'a>> {
	match a.is_truthy() {
		true => Ok(b),
		false => Ok(a),
	}
}

pub fn add<'a>(a: Value, b: Value) -> Result<Value<'a>, Error<'a>> {
	Ok(a.add(b))
}

pub fn sub<'a>(a: Value, b: Value) -> Result<Value<'a>, Error<'a>> {
	Ok(a.sub(b))
}

pub fn mul<'a>(a: Value, b: Value) -> Result<Value<'a>, Error<'a>> {
	Ok(a.mul(b))
}

pub fn div<'a>(a: Value, b: Value) -> Result<Value<'a>, Error<'a>> {
	Ok(a.div(b))
}

pub fn exact<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	Ok(Value::from(a == b))
}

pub fn equal<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.equal(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn not_equal<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.equal(b) {
		true => Ok(Value::False),
		false => Ok(Value::True),
	}
}

pub fn all_equal<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.all_equal(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn any_equal<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.any_equal(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn like<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.fuzzy(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn not_like<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.fuzzy(b) {
		true => Ok(Value::False),
		false => Ok(Value::True),
	}
}

pub fn all_like<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.all_fuzzy(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn any_like<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.any_fuzzy(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn less_than<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.lt(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn less_than_or_equal<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.le(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn more_than<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.gt(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn more_than_or_equal<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.ge(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn contain<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.contains(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn not_contain<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.contains(b) {
		true => Ok(Value::False),
		false => Ok(Value::True),
	}
}

pub fn contain_all<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.contains_all(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn contain_any<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.contains_any(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn contain_none<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.contains_any(b) {
		true => Ok(Value::False),
		false => Ok(Value::True),
	}
}

pub fn inside<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match b.contains(a) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn not_inside<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match b.contains(a) {
		true => Ok(Value::False),
		false => Ok(Value::True),
	}
}

pub fn inside_all<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match b.contains_all(a) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn inside_any<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match b.contains_any(a) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn inside_none<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match b.contains_any(a) {
		true => Ok(Value::False),
		false => Ok(Value::True),
	}
}

pub fn outside<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.intersects(b) {
		true => Ok(Value::False),
		false => Ok(Value::True),
	}
}

pub fn intersects<'a>(a: &Value, b: &Value) -> Result<Value<'a>, Error<'a>> {
	match a.intersects(b) {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn or_true() {
		let one = Value::from(1);
		let two = Value::from(2);
		let res = or(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn or_false_one() {
		let one = Value::from(0);
		let two = Value::from(1);
		let res = or(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn or_false_two() {
		let one = Value::from(1);
		let two = Value::from(0);
		let res = or(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn and_true() {
		let one = Value::from(1);
		let two = Value::from(2);
		let res = and(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("2", format!("{}", out));
	}

	#[test]
	fn and_false_one() {
		let one = Value::from(0);
		let two = Value::from(1);
		let res = and(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("0", format!("{}", out));
	}

	#[test]
	fn and_false_two() {
		let one = Value::from(1);
		let two = Value::from(0);
		let res = and(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("0", format!("{}", out));
	}

	#[test]
	fn add_basic() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = add(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("9", format!("{}", out));
	}

	#[test]
	fn sub_basic() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = sub(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn mul_basic() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = mul(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("20", format!("{}", out));
	}

	#[test]
	fn div_basic() {
		let one = Value::from(5);
		let two = Value::from(4);
		let res = div(one, two);
		assert!(res.is_ok());
		let out = res.unwrap();
		assert_eq!("1.25", format!("{}", out));
	}
}
