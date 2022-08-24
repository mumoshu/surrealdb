use crate::sql::value::Value;

impl <'a>Value<'a> {
	pub fn single(&self) -> &Self {
		match self {
			Value::Array(v) => match v.first() {
				None => &Value::None,
				Some(v) => v,
			},
			v => v,
		}
	}
}
