use crate::sql::changefeed::{ChangeSet, DatabaseMutation, TableMutations};
use crate::err::Error;
use crate::kvs::Transaction;
use crate::key::cf;
use crate::key::dv;

use std::ascii::escape_default;
use std::str;

fn show(bs: &[u8]) -> String {
    let mut visible = String::new();
    for &b in bs {
        let part: Vec<u8> = escape_default(b).collect();
        visible.push_str(str::from_utf8(&part).unwrap());
    }
    visible
}

// put_change writes all the mutations buffered for this transaction
// onto the key composed of the specified prefix + the current timestamp.
pub async fn read(tx: &mut Transaction, ns: &str, db: &str, tb: Option<&str>, start: Option<u64>, limit: Option<u32>) -> Result<Vec<ChangeSet>, Error>
{
    // Get the current timestamp
    let seq = dv::new(ns, db);

    let beg = match start {
        Some(x) => cf::ts_prefix(ns, db, cf::u64_to_versionstamp(x)),
        None => {
            let ts = tx.get_timestamp(seq, false).await?;
            cf::ts_prefix(ns, db, ts)
        },
        // None => dc::prefix(ns, db),
    };
    let end = cf::suffix(ns, db);

    let limit = match limit {
        Some(x) => x,
        None => 100,
    };
    
    let _x = tx.scan(beg..end, limit).await?;
    
    let mut vs: Option<[u8; 10]> = None;
    let mut buf: Vec<TableMutations> = Vec::new();

    let mut r = Vec::<ChangeSet>::new();
    // iterate over _x and put decoded elements to r
    for (k,v) in _x {
		println!("read: k={}", show(k.as_slice()));

        let dec = crate::key::cf::Cf::decode(&k).unwrap();

        if let Some(tb) = tb {
            if dec.tb != tb {
                continue;
            }
        }

        let _tb = dec.tb;
        let ts = dec.vs;

        // Decode the byte array into a vector of operations
        let tb_muts: TableMutations = v.into();
        
        match vs {
            Some(x) => {
                if ts != x {
                    let db_mut = DatabaseMutation(buf);
                    r.push(ChangeSet(x, db_mut));
                    buf = Vec::new();
                    vs = Some(ts)
                }
            },
            None => {
                vs = Some(ts);
            },
        }
        buf.push(tb_muts);
    }

    if buf.len() > 0 {
        let db_mut = DatabaseMutation(buf);
        r.push(ChangeSet(vs.unwrap(), db_mut));
    }

    Ok(r)
}
