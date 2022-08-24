use crate::ctx::Context;
use crate::err::Error;
use crate::sql::value::Value;

pub fn run<'a>(_: &Context, expr: Value<'a>) -> Result<Value<'a>, Error<'a>> {
	Ok(expr)
}
