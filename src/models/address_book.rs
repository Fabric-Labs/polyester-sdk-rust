//! Address book models (Go `models/address_book.go` thin parity).

use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct AddressBooksList {
    pub books: Vec<Value>,
}
