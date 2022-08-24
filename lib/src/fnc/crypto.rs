use crate::ctx::Context;
use crate::err::Error;
use crate::sql::value::Value;
use md5::Digest;
use md5::Md5;
use sha1::Sha1;
use sha2::Sha256;
use sha2::Sha512;

pub fn md5<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let mut hasher = Md5::new();
	hasher.update(args.remove(0).as_string().as_str());
	let val = hasher.finalize();
	let val = format!("{:x}", val);
	Ok(val.into())
}

pub fn sha1<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let mut hasher = Sha1::new();
	hasher.update(args.remove(0).as_string().as_str());
	let val = hasher.finalize();
	let val = format!("{:x}", val);
	Ok(val.into())
}

pub fn sha256<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let mut hasher = Sha256::new();
	hasher.update(args.remove(0).as_string().as_str());
	let val = hasher.finalize();
	let val = format!("{:x}", val);
	Ok(val.into())
}

pub fn sha512<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
	let mut hasher = Sha512::new();
	hasher.update(args.remove(0).as_string().as_str());
	let val = hasher.finalize();
	let val = format!("{:x}", val);
	Ok(val.into())
}

pub mod argon2 {

	use crate::ctx::Context;
	use crate::err::Error;
	use crate::sql::value::Value;
	use argon2::{
		password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
		Argon2,
	};
	use rand::rngs::OsRng;

	pub fn cmp<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
		let algo = Argon2::default();
		let hash = args.remove(0).as_string();
		let pass = args.remove(0).as_string();
		let test = PasswordHash::new(&hash).unwrap();
		Ok(algo.verify_password(pass.as_ref(), &test).is_ok().into())
	}

	pub fn gen<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
		let algo = Argon2::default();
		let pass = args.remove(0).as_string();
		let salt = SaltString::generate(&mut OsRng);
		let hash = algo.hash_password(pass.as_ref(), salt.as_ref()).unwrap().to_string();
		Ok(hash.into())
	}
}

pub mod pbkdf2 {

	use crate::ctx::Context;
	use crate::err::Error;
	use crate::sql::value::Value;
	use pbkdf2::{
		password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
		Pbkdf2,
	};
	use rand::rngs::OsRng;

	pub fn cmp<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
		let hash = args.remove(0).as_string();
		let pass = args.remove(0).as_string();
		let test = PasswordHash::new(&hash).unwrap();
		Ok(Pbkdf2.verify_password(pass.as_ref(), &test).is_ok().into())
	}

	pub fn gen<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
		let pass = args.remove(0).as_string();
		let salt = SaltString::generate(&mut OsRng);
		let hash = Pbkdf2.hash_password(pass.as_ref(), salt.as_ref()).unwrap().to_string();
		Ok(hash.into())
	}
}

pub mod scrypt {

	use crate::ctx::Context;
	use crate::err::Error;
	use crate::sql::value::Value;
	use rand::rngs::OsRng;
	use scrypt::{
		password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
		Scrypt,
	};

	pub fn cmp<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
		let hash = args.remove(0).as_string();
		let pass = args.remove(0).as_string();
		let test = PasswordHash::new(&hash).unwrap();
		Ok(Scrypt.verify_password(pass.as_ref(), &test).is_ok().into())
	}

	pub fn gen<'a>(_: &Context, mut args: Vec<Value<'a>>) -> Result<Value<'a>, Error<'a>> {
		let pass = args.remove(0).as_string();
		let salt = SaltString::generate(&mut OsRng);
		let hash = Scrypt.hash_password(pass.as_ref(), salt.as_ref()).unwrap().to_string();
		Ok(hash.into())
	}
}
