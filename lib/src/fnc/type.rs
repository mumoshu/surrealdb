use crate::ctx::Context;
use crate::err::Error;
use crate::sql::geometry::Geometry;
use crate::sql::number::Number;
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::Value;

pub fn bool<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0).is_truthy() {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn datetime<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let val = args.remove(0);
	match val {
		Value::Datetime(_) => Ok(val),
		_ => Ok(Value::Datetime(val.as_datetime())),
	}
}

pub fn decimal<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let val = args.remove(0);
	match val {
		Value::Number(Number::Decimal(_)) => Ok(val),
		_ => Ok(Value::Number(Number::Decimal(val.as_decimal()))),
	}
}

pub fn duration<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let val = args.remove(0);
	match val {
		Value::Duration(_) => Ok(val),
		_ => Ok(Value::Duration(val.as_duration())),
	}
}

pub fn float<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let val = args.remove(0);
	match val {
		Value::Number(Number::Float(_)) => Ok(val),
		_ => Ok(Value::Number(Number::Float(val.as_float()))),
	}
}

pub fn int<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let val = args.remove(0);
	match val {
		Value::Number(Number::Int(_)) => Ok(val),
		_ => Ok(Value::Number(Number::Int(val.as_int()))),
	}
}

pub fn number<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let val = args.remove(0);
	match val {
		Value::Number(_) => Ok(val),
		_ => Ok(Value::Number(val.as_number())),
	}
}

pub fn point<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.len() {
		2 => {
			let x = args.remove(0);
			let y = args.remove(0);
			Ok((x.as_float(), y.as_float()).into())
		}
		1 => match args.remove(0) {
			Value::Array(v) if v.len() == 2 => Ok(v.as_point().into()),
			Value::Geometry(Geometry::Point(v)) => Ok(v.into()),
			_ => Ok(Value::None),
		},
		_ => unreachable!(),
	}
}

pub fn regex<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Strand(v) => Ok(Value::Regex(v.as_str().into())),
		_ => Ok(Value::None),
	}
}

pub fn string<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let val = args.remove(0);
	match val {
		Value::Strand(_) => Ok(val),
		_ => Ok(Value::Strand(val.as_strand())),
	}
}

pub fn table<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	Ok(Value::Table(Table(args.remove(0).as_string())))
}

pub fn thing<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.len() {
		2 => {
			let tb = args.remove(0);
			match args.remove(0) {
				Value::Thing(id) => Ok(Value::Thing(Thing {
					tb: tb.as_string(),
					id: id.id,
				})),
				id => Ok(Value::Thing(Thing {
					tb: tb.as_string(),
					id: id.as_string().into(),
				})),
			}
		}
		1 => match args.remove(0) {
			Value::Thing(v) => Ok(v.into()),
			_ => Ok(Value::None),
		},
		_ => unreachable!(),
	}
}
