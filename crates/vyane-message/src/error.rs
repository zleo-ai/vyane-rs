use thiserror::Error;

use crate::DeliveryStatus;

pub type Result<T> = std::result::Result<T, MessageStoreError>;

#[derive(Debug, Error)]
pub enum MessageStoreError {
    #[error("message resource was not found in the authorized owner scope")]
    NotFound,

    #[error("idempotency key was reused with different message content")]
    IdempotencyConflict,

    #[error("message publication is already in a conflicting terminal state")]
    PublicationConflict,

    #[error("delivery `{delivery_id}` already has a different external transport receipt")]
    TransportReceiptConflict { delivery_id: String },

    #[error("delivery `{delivery_id}` receipt operation id was reused with different arguments")]
    ReceiptOperationConflict { delivery_id: String },

    #[error("invalid message input: {0}")]
    InvalidInput(String),

    #[error("delivery `{delivery_id}` cannot {operation} while it is {state}")]
    InvalidState {
        delivery_id: String,
        operation: &'static str,
        state: DeliveryStatus,
    },

    #[error("delivery receipt for `{delivery_id}` is stale or invalid")]
    InvalidReceipt { delivery_id: String },

    #[error("delivery lease for `{delivery_id}` has expired")]
    LeaseExpired { delivery_id: String },

    #[error("outbox event is absent, already projected, or belongs to another owner")]
    ProjectionConflict,

    #[error("message database schema {found} is newer than supported schema {supported}")]
    UnsupportedSchema { found: u32, supported: u32 },

    #[error("message database contains invalid data: {0}")]
    CorruptData(String),

    #[error(transparent)]
    Sqlite(rusqlite::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl MessageStoreError {
    /// Stable snake_case *kind* token; delivery ids/operations/payloads stay out.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::PublicationConflict => "publication_conflict",
            Self::TransportReceiptConflict { .. } => "transport_receipt_conflict",
            Self::ReceiptOperationConflict { .. } => "receipt_operation_conflict",
            Self::InvalidInput(_) => "invalid_input",
            Self::InvalidState { .. } => "invalid_state",
            Self::InvalidReceipt { .. } => "invalid_receipt",
            Self::LeaseExpired { .. } => "lease_expired",
            Self::ProjectionConflict => "projection_conflict",
            Self::UnsupportedSchema { .. } => "unsupported_schema",
            Self::CorruptData(_) => "corrupt_data",
            Self::Sqlite(_) => "sqlite",
            Self::Io(_) => "io",
        }
    }
}

impl From<rusqlite::Error> for MessageStoreError {
    fn from(error: rusqlite::Error) -> Self {
        match error {
            rusqlite::Error::FromSqlConversionFailure(index, value_type, source) => {
                match source.downcast::<Self>() {
                    Ok(message_error) => *message_error,
                    Err(_) => Self::CorruptData(format!(
                        "column {index} contains invalid {value_type:?} data"
                    )),
                }
            }
            rusqlite::Error::IntegralValueOutOfRange(index, _) => {
                Self::CorruptData(format!("column {index} contains an out-of-range integer"))
            }
            rusqlite::Error::Utf8Error(_) => {
                Self::CorruptData("database contains invalid UTF-8".into())
            }
            rusqlite::Error::InvalidColumnType(index, name, value_type) => Self::CorruptData(
                format!("column {index} (`{name}`) contains incompatible {value_type:?} data"),
            ),
            other => Self::Sqlite(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MessageStoreError;
    use crate::DeliveryStatus;

    #[test]
    fn message_store_error_kind_tokens_are_snake_case_without_payload() {
        assert_eq!(MessageStoreError::NotFound.as_str(), "not_found");
        assert_eq!(
            MessageStoreError::IdempotencyConflict.as_str(),
            "idempotency_conflict"
        );
        assert_eq!(
            MessageStoreError::PublicationConflict.as_str(),
            "publication_conflict"
        );
        assert_eq!(
            MessageStoreError::TransportReceiptConflict {
                delivery_id: "secret-delivery".into()
            }
            .as_str(),
            "transport_receipt_conflict"
        );
        assert_eq!(
            MessageStoreError::ReceiptOperationConflict {
                delivery_id: "secret-delivery".into()
            }
            .as_str(),
            "receipt_operation_conflict"
        );
        assert_eq!(
            MessageStoreError::InvalidInput("secret-meta".into()).as_str(),
            "invalid_input"
        );
        assert_eq!(
            MessageStoreError::InvalidState {
                delivery_id: "secret-delivery".into(),
                operation: "ack",
                state: DeliveryStatus::Pending,
            }
            .as_str(),
            "invalid_state"
        );
        assert_eq!(
            MessageStoreError::InvalidReceipt {
                delivery_id: "secret-delivery".into()
            }
            .as_str(),
            "invalid_receipt"
        );
        assert_eq!(
            MessageStoreError::LeaseExpired {
                delivery_id: "secret-delivery".into()
            }
            .as_str(),
            "lease_expired"
        );
        assert_eq!(
            MessageStoreError::ProjectionConflict.as_str(),
            "projection_conflict"
        );
        assert_eq!(
            MessageStoreError::UnsupportedSchema {
                found: 9,
                supported: 3
            }
            .as_str(),
            "unsupported_schema"
        );
        assert_eq!(
            MessageStoreError::CorruptData("secret-blob".into()).as_str(),
            "corrupt_data"
        );
        assert!(
            !MessageStoreError::CorruptData("secret-blob".into())
                .as_str()
                .contains("secret")
        );
        assert_eq!(
            MessageStoreError::Io(std::io::Error::other("secret-io")).as_str(),
            "io"
        );
    }
}
