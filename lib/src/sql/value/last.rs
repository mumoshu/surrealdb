use crate::sql::part::Part;
use crate::sql::value::Value;

impl <'a>Value<'a> {
	pub fn last(&self) -> Self {
		self.pick(&[Part::Last])
	}
}
