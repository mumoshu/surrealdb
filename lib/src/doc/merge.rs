use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::dbs::Workable;
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn merge(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction<'_>,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Set default field values
		self.current.to_mut().def(ctx, opt, txn, rid).await?;
		// This is an INSERT statement
		if let Workable::Insert(v) = &self.extras {
			let v = v.compute(ctx, opt, txn, Some(&self.current)).await?;
			self.current.to_mut().merge(ctx, opt, txn, v).await?;
		}
		// Set default field values
		self.current.to_mut().def(ctx, opt, txn, rid).await?;
		// Carry on
		Ok(())
	}
}
