use crate::ctx::Context;
use crate::err::Error;
use crate::sql::paths::DB;
use crate::sql::paths::ID;
use crate::sql::paths::IP;
use crate::sql::paths::NS;
use crate::sql::paths::OR;
use crate::sql::paths::SC;
use crate::sql::value::Value;

pub fn db<'a>(ctx: &Context, _: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	ctx.value("session").unwrap_or(&Value::None).pick(DB.as_ref()).ok()
}

pub fn id<'a>(ctx: &Context, _: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	ctx.value("session").unwrap_or(&Value::None).pick(ID.as_ref()).ok()
}

pub fn ip<'a>(ctx: &Context, _: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	ctx.value("session").unwrap_or(&Value::None).pick(IP.as_ref()).ok()
}

pub fn ns<'a>(ctx: &Context, _: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	ctx.value("session").unwrap_or(&Value::None).pick(NS.as_ref()).ok()
}

pub fn origin<'a>(ctx: &Context, _: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	ctx.value("session").unwrap_or(&Value::None).pick(OR.as_ref()).ok()
}

pub fn sc<'a>(ctx: &Context, _: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	ctx.value("session").unwrap_or(&Value::None).pick(SC.as_ref()).ok()
}
