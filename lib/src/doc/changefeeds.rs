use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn changefeeds(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Get the record id
		let _ = self.id.as_ref().unwrap();
		let tb = self.tb(opt, txn).await?;
		let tb = tb.as_ref();
		if tb.changefeed.enabled {
			// Clone transaction
			let run = txn.clone();
			// Claim transaction
			let mut run = run.lock().await;

			let id = self.id.as_ref().unwrap().clone();
			// Create the changefeed entry
			run.record_change(tb, id, self.current.to_owned());
		}
		// Carry on
		Ok(())
	}
}
