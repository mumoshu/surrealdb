use crate::sql::part::Part;
use crate::sql::value::Value;

impl <'a>Value<'a> {
	pub fn first(&self) -> Self {
		self.pick(&[Part::First])
	}
}
