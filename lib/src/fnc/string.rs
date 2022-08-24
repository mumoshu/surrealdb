use crate::ctx::Context;
use crate::err::Error;
use crate::fnc::util::string;
use crate::sql::value::Value;

pub fn concat<'a>(_: &Context, args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	Ok(args.into_iter().map(|x| x.as_string()).collect::<Vec<_>>().concat().into())
}

pub fn ends_with<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let val = args.remove(0).as_string();
	let chr = args.remove(0).as_string();
	Ok(val.ends_with(&chr).into())
}

pub fn join<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let chr = args.remove(0).as_string();
	let val = args.into_iter().map(|x| x.as_string());
	let val = val.collect::<Vec<_>>().join(&chr);
	Ok(val.into())
}

pub fn length<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let val = args.remove(0).as_string();
	let num = val.chars().count() as i64;
	Ok(num.into())
}

pub fn lowercase<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	Ok(args.remove(0).as_string().to_lowercase().into())
}

pub fn repeat<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let val = args.remove(0).as_string();
	let num = args.remove(0).as_int() as usize;
	Ok(val.repeat(num).into())
}

pub fn replace<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let val = args.remove(0).as_string();
	let old = args.remove(0).as_string();
	let new = args.remove(0).as_string();
	Ok(val.replace(&old, &new).into())
}

pub fn reverse<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	Ok(args.remove(0).as_string().chars().rev().collect::<String>().into())
}

pub fn slice<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let val = args.remove(0).as_string();
	let beg = args.remove(0).as_int() as usize;
	let lim = args.remove(0).as_int() as usize;
	let val = val.chars().skip(beg).take(lim).collect::<String>();
	Ok(val.into())
}

pub fn slug<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	Ok(string::slug(&args.remove(0).as_string()).into())
}

pub fn split<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let val = args.remove(0).as_string();
	let chr = args.remove(0).as_string();
	let val = val.split(&chr).collect::<Vec<&str>>();
	Ok(val.into())
}

pub fn starts_with<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let val = args.remove(0).as_string();
	let chr = args.remove(0).as_string();
	Ok(val.starts_with(&chr).into())
}

pub fn trim<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	Ok(args.remove(0).as_string().trim().into())
}

pub fn uppercase<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	Ok(args.remove(0).as_string().to_uppercase().into())
}

pub fn words<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	Ok(args.remove(0).as_string().split(' ').collect::<Vec<&str>>().into())
}
