//! Address book decoders.

use crate::models::AddressBooksList;
use crate::proto::auth::v1::ListAddressBooksResponse;

pub fn list_books_from_proto(msg: &ListAddressBooksResponse) -> AddressBooksList {
    AddressBooksList {
        books: msg
            .books
            .iter()
            .filter_map(|book| serde_json::to_value(book).ok())
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::auth::v1::AddressBook;

    #[test]
    fn list_books_serializes_rows() {
        let msg = ListAddressBooksResponse {
            books: vec![AddressBook {
                label: "main".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = list_books_from_proto(&msg);
        assert_eq!(result.books.len(), 1);
        assert_eq!(result.books[0]["label"], "main");
    }
}
