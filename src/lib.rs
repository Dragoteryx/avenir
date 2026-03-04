#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(nightly, feature(doc_cfg))]
#![forbid(unsafe_code)]

#[cfg(feature = "std")]
mod thread;
#[cfg(feature = "std")]
pub use thread::*;

mod executor;
pub use executor::*;

mod util;
pub use util::*;
