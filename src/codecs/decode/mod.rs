//! Proto → SDK model decoders (Go `codecs/decode` parity).

mod auth;
mod enums;
mod money;
mod orders;

pub use auth::*;
pub use enums::*;
pub use money::*;
pub use orders::*;
