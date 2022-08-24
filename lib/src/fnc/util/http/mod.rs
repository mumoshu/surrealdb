use crate::err::Error;
use crate::sql::json;
use crate::sql::object::Object;
use crate::sql::strand::Strand;
use crate::sql::value::Value;
use surf::Client;
use surf::Config;

pub async fn head<'a>(uri: Strand, opts: impl Into<Object<'a>>) -> Result<Value<'a>, Error<'a>> {
	// Set a default client with no timeout
	let cli: Client = Config::new().set_timeout(None).try_into().unwrap();
	// Start a new HEAD request
	let mut req = cli.head(uri.as_str());
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Send the request and wait
	let res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => Ok(Value::None),
		s => Err(Error::Http(s.canonical_reason().to_owned())),
	}
}

pub async fn get<'a>(uri: Strand, opts: impl Into<Object<'a>>) -> Result<Value<'a>, Error<'a>> {
	// Set a default client with no timeout
	let cli: Client = Config::new().set_timeout(None).try_into().unwrap();
	// Start a new GET request
	let mut req = cli.get(uri.as_str());
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Send the request and wait
	let mut res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => match res.content_type() {
			Some(mime) if mime.essence() == "application/json" => {
				let txt = res.body_string().await?;
				let val = json(&txt)?;
				Ok(val)
			}
			_ => {
				let txt = res.body_string().await?;
				Ok(txt.into())
			}
		},
		s => Err(Error::Http(s.canonical_reason().to_owned())),
	}
}

pub async fn put<'a>(uri: Strand, body: Value<'a>, opts: impl Into<Object<'a>>) -> Result<Value<'a>, Error> {
	// Set a default client with no timeout
	let cli: Client = Config::new().set_timeout(None).try_into().unwrap();
	// Start a new GET request
	let mut req = cli.put(uri.as_str());
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Submit the request body
	req = req.body_json(&body)?;
	// Send the request and wait
	let mut res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => match res.content_type() {
			Some(mime) if mime.essence() == "application/json" => {
				let txt = res.body_string().await?;
				let val = json(&txt)?;
				Ok(val)
			}
			_ => {
				let txt = res.body_string().await?;
				Ok(txt.into())
			}
		},
		s => Err(Error::Http(s.canonical_reason().to_owned())),
	}
}

pub async fn post<'a>(uri: Strand, body: Value<'a>, opts: impl Into<Object<'a>>) -> Result<Value<'a>, Error> {
	// Set a default client with no timeout
	let cli: Client = Config::new().set_timeout(None).try_into().unwrap();
	// Start a new GET request
	let mut req = cli.post(uri.as_str());
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Submit the request body
	req = req.body_json(&body)?;
	// Send the request and wait
	let mut res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => match res.content_type() {
			Some(mime) if mime.essence() == "application/json" => {
				let txt = res.body_string().await?;
				let val = json(&txt)?;
				Ok(val)
			}
			_ => {
				let txt = res.body_string().await?;
				Ok(txt.into())
			}
		},
		s => Err(Error::Http(s.canonical_reason().to_owned())),
	}
}

pub async fn patch<'a>(uri: Strand, body: Value<'a>, opts: impl Into<Object<'a>>) -> Result<Value<'a>, Error> {
	// Set a default client with no timeout
	let cli: Client = Config::new().set_timeout(None).try_into().unwrap();
	// Start a new GET request
	let mut req = cli.patch(uri.as_str());
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Submit the request body
	req = req.body_json(&body)?;
	// Send the request and wait
	let mut res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => match res.content_type() {
			Some(mime) if mime.essence() == "application/json" => {
				let txt = res.body_string().await?;
				let val = json(&txt)?;
				Ok(val)
			}
			_ => {
				let txt = res.body_string().await?;
				Ok(txt.into())
			}
		},
		s => Err(Error::Http(s.canonical_reason().to_owned())),
	}
}

pub async fn delete<'a>(uri: Strand, opts: impl Into<Object<'a>>) -> Result<Value<'a>, Error<'a>> {
	// Set a default client with no timeout
	let cli: Client = Config::new().set_timeout(None).try_into().unwrap();
	// Start a new GET request
	let mut req = cli.delete(uri.as_str());
	// Add the User-Agent header
	if cfg!(not(target_arch = "wasm32")) {
		req = req.header("User-Agent", "SurrealDB");
	}
	// Add specified header values
	for (k, v) in opts.into().iter() {
		req = req.header(k.as_str(), v.to_strand().as_str());
	}
	// Send the request and wait
	let mut res = req.send().await?;
	// Check the response status
	match res.status() {
		s if s.is_success() => match res.content_type() {
			Some(mime) if mime.essence() == "application/json" => {
				let txt = res.body_string().await?;
				let val = json(&txt)?;
				Ok(val)
			}
			_ => {
				let txt = res.body_string().await?;
				Ok(txt.into())
			}
		},
		s => Err(Error::Http(s.canonical_reason().to_owned())),
	}
}
