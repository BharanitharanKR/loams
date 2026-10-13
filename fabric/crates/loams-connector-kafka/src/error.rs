//! librdkafka errors as SDK error classes.
//!
//! The rule: a broker or network condition that clears by itself is
//! `Retryable` (or `Throttled` for a quota), a record the cluster refuses as
//! such is `Poison` (DLQ), a refused credential or ACL is `Auth`, a lost
//! position is `PositionLost`, and anything else, including a fenced
//! transactional producer, is `Fatal`.

use std::time::Duration;

use loams_connector_sdk::{ConnectorError, ErrorClass};
use rdkafka::error::{KafkaError, RDKafkaErrorCode as C};

/// The class of a librdkafka error code.
pub fn class_of_code(code: C) -> ErrorClass {
    match code {
        C::MessageSizeTooLarge
        | C::InvalidMessage
        | C::InvalidMessageSize
        | C::InvalidRecord
        | C::MessageBatchTooLarge
        | C::BadMessage
        | C::KeyDeserialization
        | C::ValueDeserialization => ErrorClass::Poison,
        C::TopicAuthorizationFailed
        | C::GroupAuthorizationFailed
        | C::ClusterAuthorizationFailed
        | C::TransactionalIdAuthorizationFailed
        | C::SaslAuthenticationFailed
        | C::Authentication
        | C::IllegalSASLState
        | C::UnsupportedSASLMechanism
        | C::DelegationTokenAuthorizationFailed => ErrorClass::Auth,
        C::AutoOffsetReset | C::OffsetOutOfRange => ErrorClass::PositionLost,
        C::ThrottlingQuotaExceeded => ErrorClass::Throttled {
            after: Duration::from_secs(1),
        },
        C::QueueFull
        | C::MessageTimedOut
        | C::RequestTimedOut
        | C::OperationTimedOut
        | C::TimedOutQueue
        | C::NotEnoughReplicas
        | C::NotEnoughReplicasAfterAppend
        | C::LeaderNotAvailable
        | C::NotLeaderForPartition
        | C::NetworkException
        | C::BrokerNotAvailable
        | C::AllBrokersDown
        | C::BrokerTransportFailure
        | C::Resolve
        | C::UnknownTopicOrPartition
        | C::UnknownPartition
        | C::UnknownTopic
        | C::CoordinatorLoadInProgress
        | C::CoordinatorNotAvailable
        | C::NotCoordinator
        | C::ConcurrentTransactions
        | C::KafkaStorageError
        | C::OutOfOrderSequenceNumber
        | C::UnknownLeaderEpoch
        | C::FencedLeaderEpoch
        | C::RebalanceInProgress
        | C::Retry
        | C::State
        | C::BrokerDestroy => ErrorClass::Retryable { after: None },
        _ => ErrorClass::Fatal,
    }
}

/// The class of a [`KafkaError`].
pub fn class_of(e: &KafkaError) -> ErrorClass {
    match e {
        KafkaError::Transaction(rd) => {
            if rd.is_fatal() {
                ErrorClass::Fatal
            } else if rd.is_retriable() || rd.txn_requires_abort() {
                ErrorClass::Retryable { after: None }
            } else {
                class_of_code(rd.code())
            }
        }
        KafkaError::MessageConsumptionFatal(_) => ErrorClass::Fatal,
        KafkaError::ClientConfig(..) | KafkaError::ClientCreation(_) => ErrorClass::Fatal,
        KafkaError::Canceled => ErrorClass::Retryable { after: None },
        other => other
            .rdkafka_error_code()
            .map_or(ErrorClass::Fatal, class_of_code),
    }
}

/// A [`KafkaError`] as a [`ConnectorError`], with what was being done.
pub fn kafka_error(what: &str, e: &KafkaError) -> ConnectorError {
    ConnectorError::new(class_of(e), format!("{what}: {e}"))
}

/// A blocking call that panicked or was cancelled.
pub fn join_error(what: &str, e: tokio::task::JoinError) -> ConnectorError {
    ConnectorError::fatal(format!("{what}: the blocking call failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes() {
        let poison = KafkaError::MessageProduction(C::MessageSizeTooLarge);
        assert_eq!(class_of(&poison), ErrorClass::Poison);
        let auth = KafkaError::MessageProduction(C::TopicAuthorizationFailed);
        assert_eq!(class_of(&auth), ErrorClass::Auth);
        let down = KafkaError::MetadataFetch(C::AllBrokersDown);
        assert_eq!(class_of(&down), ErrorClass::Retryable { after: None });
        let lost = KafkaError::MessageConsumption(C::AutoOffsetReset);
        assert_eq!(class_of(&lost), ErrorClass::PositionLost);
        let fenced = KafkaError::MessageProduction(C::ProducerFenced);
        assert_eq!(class_of(&fenced), ErrorClass::Fatal);
        assert!(
            kafka_error("produce", &poison)
                .message
                .starts_with("produce: ")
        );
    }
}
