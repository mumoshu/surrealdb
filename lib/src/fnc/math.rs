use crate::ctx::Context;
use crate::err::Error;
use crate::fnc::util::math::bottom::Bottom;
use crate::fnc::util::math::deviation::Deviation;
use crate::fnc::util::math::interquartile::Interquartile;
use crate::fnc::util::math::mean::Mean;
use crate::fnc::util::math::median::Median;
use crate::fnc::util::math::midhinge::Midhinge;
use crate::fnc::util::math::mode::Mode;
use crate::fnc::util::math::nearestrank::Nearestrank;
use crate::fnc::util::math::percentile::Percentile;
use crate::fnc::util::math::spread::Spread;
use crate::fnc::util::math::top::Top;
use crate::fnc::util::math::trimean::Trimean;
use crate::fnc::util::math::variance::Variance;
use crate::sql::number::Number;
use crate::sql::value::Value;

pub fn abs<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	Ok(args.remove(0).as_number().abs().into())
}

pub fn bottom<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => {
			let c = args.remove(0).as_int();
			Ok(v.as_numbers().bottom(c).into())
		}
		_ => Ok(Value::None),
	}
}

pub fn ceil<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	Ok(args.remove(0).as_number().ceil().into())
}

pub fn fixed<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let v = args.remove(0);
	match args.remove(0).as_int() {
		p if p > 0 => Ok(v.as_number().fixed(p as usize).into()),
		_ => Err(Error::InvalidArguments {
			name: String::from("math::fixed"),
			message: String::from("The second argument must be an integer greater than 0."),
		}),
	}
}

pub fn floor<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	Ok(args.remove(0).as_number().floor().into())
}

pub fn interquartile<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().interquartile().into()),
		_ => Ok(Value::None),
	}
}

pub fn max<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => match v.as_numbers().into_iter().max() {
			Some(v) => Ok(v.into()),
			None => Ok(Value::None),
		},
		v => Ok(v),
	}
}

pub fn mean<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().mean().into()),
		_ => Ok(Value::None),
	}
}

pub fn median<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().median().into()),
		_ => Ok(Value::None),
	}
}

pub fn midhinge<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().midhinge().into()),
		_ => Ok(Value::None),
	}
}

pub fn min<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => match v.as_numbers().into_iter().min() {
			Some(v) => Ok(v.into()),
			None => Ok(Value::None),
		},
		v => Ok(v),
	}
}

pub fn mode<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().mode().into()),
		_ => Ok(Value::None),
	}
}

pub fn nearestrank<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().nearestrank(args.remove(0).as_number()).into()),
		_ => Ok(Value::None),
	}
}

pub fn percentile<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().percentile(args.remove(0).as_number()).into()),
		_ => Ok(Value::None),
	}
}

pub fn product<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().into_iter().product::<Number>().into()),
		_ => Ok(Value::None),
	}
}

pub fn round<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	Ok(args.remove(0).as_number().round().into())
}

pub fn spread<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().spread().into()),
		_ => Ok(Value::None),
	}
}

pub fn sqrt<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	Ok(args.remove(0).as_number().sqrt().into())
}

pub fn stddev<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().deviation().into()),
		_ => Ok(Value::None),
	}
}

pub fn sum<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().into_iter().sum::<Number>().into()),
		_ => Ok(Value::None),
	}
}

pub fn top<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => {
			let c = args.remove(0).as_int();
			Ok(v.as_numbers().top(c).into())
		}
		_ => Ok(Value::None),
	}
}

pub fn trimean<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().trimean().into()),
		_ => Ok(Value::None),
	}
}

pub fn variance<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.as_numbers().variance().into()),
		_ => Ok(Value::None),
	}
}
