use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value<'_> {
	pub fn all(&self) -> Self {
		self.pick(&[Part::All])
	}
}
