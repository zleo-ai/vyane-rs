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
