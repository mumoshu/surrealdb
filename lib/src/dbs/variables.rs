use crate::ctx::Context;
use crate::sql::value::Value;
use std::collections::BTreeMap;

pub type Variables<'a> = Option<BTreeMap<String, Value<'a>>>;

pub(crate) trait Attach {
	fn attach(self, ctx: Context) -> Context;
}

impl Attach for Variables<'_> {
	fn attach(self, mut ctx: Context) -> Context {
		match self {
			Some(m) => {
				for (key, val) in m {
					ctx.add_value(key, val);
				}
				ctx
			}
			None => ctx,
		}
	}
}
