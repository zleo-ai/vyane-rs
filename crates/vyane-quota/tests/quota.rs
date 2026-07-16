#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use reqwest::header::HeaderMap;
use vyane_quota::{
    QuotaBalance, QuotaCard, QuotaConnector, QuotaConnectorError, QuotaConnectorErrorCode,
    QuotaHttpReader, QuotaReadPolicy, QuotaRunnerError, QuotaSnapshotRunner, QuotaSnapshotStatus,
    QuotaStatus, QuotaTransportError, QuotaUnit, QuotaValidationError, QuotaWindow,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[derive(Clone)]
enum FakeResult {
    Card(QuotaCard),
    Error(QuotaConnectorErrorCode),
}

struct FakeConnector {
    id: String,
    provider: String,
    delay: Duration,
    result: FakeResult,
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
}

#[async_trait]
impl QuotaConnector for FakeConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn provider(&self) -> &str {
        &self.provider
    }

    async fn snapshot(&self, _policy: QuotaReadPolicy) -> Result<QuotaCard, QuotaConnectorError> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        match &self.result {
            FakeResult::Card(card) => Ok(card.clone()),
            FakeResult::Error(code) => Err(QuotaConnectorError::new(*code)),
        }
    }
}

fn card(id: &str, provider: &str) -> QuotaCard {
    QuotaCard {
        connector_id: id.into(),
        provider: provider.into(),
        status: QuotaStatus::Available,
        checked_at: Utc::now(),
        windows: vec![],
        balance: None,
    }
}

fn connector(
    id: &str,
    delay: Duration,
    result: FakeResult,
    active: &Arc<AtomicUsize>,
    maximum: &Arc<AtomicUsize>,
) -> Arc<dyn QuotaConnector> {
    Arc::new(FakeConnector {
        id: id.into(),
        provider: "provider-a".into(),
        delay,
        result,
        active: Arc::clone(active),
        maximum: Arc::clone(maximum),
    })
}

#[tokio::test]
async fn runner_sorts_results_and_isolates_connector_failures() {
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let connectors = vec![
        connector(
            "z-last",
            Duration::from_millis(1),
            FakeResult::Card(card("z-last", "provider-a")),
            &active,
            &maximum,
        ),
        connector(
            "a-first",
            Duration::ZERO,
            FakeResult::Error(QuotaConnectorErrorCode::Authentication),
            &active,
            &maximum,
        ),
    ];
    let runner = QuotaSnapshotRunner::new(connectors, 2, QuotaReadPolicy::default()).unwrap();

    let snapshots = runner.snapshot().await;

    assert_eq!(snapshots.len(), 2);
    assert_eq!(snapshots[0].connector_id, "a-first");
    assert_eq!(snapshots[0].status, QuotaSnapshotStatus::Error);
    assert_eq!(
        snapshots[0].error,
        Some(QuotaConnectorErrorCode::Authentication)
    );
    assert_eq!(snapshots[1].connector_id, "z-last");
    assert_eq!(snapshots[1].status, QuotaSnapshotStatus::Ok);
    assert_eq!(snapshots[1].card.as_ref().unwrap().connector_id, "z-last");
}

#[tokio::test]
async fn runner_enforces_concurrency() {
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let connectors = (0..5)
        .map(|index| {
            let id = format!("connector-{index}");
            connector(
                &id,
                Duration::from_millis(20),
                FakeResult::Card(card(&id, "provider-a")),
                &active,
                &maximum,
            )
        })
        .collect();
    let runner = QuotaSnapshotRunner::new(
        connectors,
        2,
        QuotaReadPolicy {
            timeout: Duration::from_millis(100),
            ..QuotaReadPolicy::default()
        },
    )
    .unwrap();

    let snapshots = runner.snapshot().await;

    assert_eq!(maximum.load(Ordering::SeqCst), 2);
    assert!(
        snapshots
            .iter()
            .all(|snapshot| snapshot.status == QuotaSnapshotStatus::Ok)
    );
}

#[tokio::test]
async fn runner_times_out_one_connector_without_losing_other_results() {
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let connectors = vec![
        connector(
            "slow",
            Duration::from_millis(30),
            FakeResult::Card(card("slow", "provider-a")),
            &active,
            &maximum,
        ),
        connector(
            "fast",
            Duration::ZERO,
            FakeResult::Card(card("fast", "provider-a")),
            &active,
            &maximum,
        ),
    ];
    let runner = QuotaSnapshotRunner::new(
        connectors,
        2,
        QuotaReadPolicy {
            timeout: Duration::from_millis(5),
            ..QuotaReadPolicy::default()
        },
    )
    .unwrap();

    let snapshots = runner.snapshot().await;

    assert_eq!(snapshots[0].connector_id, "fast");
    assert_eq!(snapshots[0].status, QuotaSnapshotStatus::Ok);
    assert_eq!(snapshots[1].connector_id, "slow");
    assert_eq!(snapshots[1].status, QuotaSnapshotStatus::Timeout);
}

#[tokio::test]
async fn runner_rejects_duplicate_ids_and_invalid_card_identity() {
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let duplicate = vec![
        connector(
            "same",
            Duration::ZERO,
            FakeResult::Card(card("same", "provider-a")),
            &active,
            &maximum,
        ),
        connector(
            "same",
            Duration::ZERO,
            FakeResult::Card(card("same", "provider-a")),
            &active,
            &maximum,
        ),
    ];
    assert!(matches!(
        QuotaSnapshotRunner::new(duplicate, 1, QuotaReadPolicy::default()),
        Err(QuotaRunnerError::DuplicateConnector)
    ));

    let mismatch = vec![connector(
        "expected",
        Duration::ZERO,
        FakeResult::Card(card("different", "provider-a")),
        &active,
        &maximum,
    )];
    let snapshots = QuotaSnapshotRunner::new(mismatch, 1, QuotaReadPolicy::default())
        .unwrap()
        .snapshot()
        .await;
    assert_eq!(snapshots[0].status, QuotaSnapshotStatus::Error);
    assert_eq!(
        snapshots[0].error,
        Some(QuotaConnectorErrorCode::InvalidResponse)
    );
    assert!(snapshots[0].card.is_none());
}

#[test]
fn quota_card_validation_rejects_invalid_windows_and_balances() {
    let mut invalid_window = card("connector", "provider-a");
    invalid_window.windows.push(QuotaWindow {
        id: "window".into(),
        used_basis_points: 10_001,
        resets_at: None,
    });
    assert_eq!(
        invalid_window.validate(),
        Err(QuotaValidationError::InvalidWindowUsage)
    );

    let mut invalid_balance = card("connector", "provider-a");
    invalid_balance.balance = Some(QuotaBalance {
        unit: QuotaUnit::Tokens,
        remaining: 11,
        limit: Some(10),
    });
    assert_eq!(
        invalid_balance.validate(),
        Err(QuotaValidationError::InvalidBalance)
    );

    let mut contradiction = card("connector", "provider-a");
    contradiction.status = QuotaStatus::Exhausted;
    contradiction.balance = Some(QuotaBalance {
        unit: QuotaUnit::Requests,
        remaining: 1,
        limit: Some(10),
    });
    assert_eq!(
        contradiction.validate(),
        Err(QuotaValidationError::StatusContradictsBalance)
    );
}

#[tokio::test]
async fn http_reader_rejects_redirects_and_oversized_bodies() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/redirect"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", "/target"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/large"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b'x'; 9]))
        .mount(&server)
        .await;
    let reader = QuotaHttpReader::new().unwrap();

    let redirect = reader
        .get(
            &format!("{}/redirect", server.uri()),
            HeaderMap::new(),
            QuotaReadPolicy::default(),
        )
        .await;
    assert_eq!(redirect, Err(QuotaTransportError::RedirectRejected));

    let oversized = reader
        .get(
            &format!("{}/large", server.uri()),
            HeaderMap::new(),
            QuotaReadPolicy {
                max_body_bytes: 8,
                ..QuotaReadPolicy::default()
            },
        )
        .await;
    assert_eq!(oversized, Err(QuotaTransportError::BodyTooLarge));
}

#[tokio::test]
async fn http_reader_times_out_without_using_real_network() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/slow"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(50)))
        .mount(&server)
        .await;
    let reader = QuotaHttpReader::new().unwrap();

    let result = reader
        .get(
            &format!("{}/slow", server.uri()),
            HeaderMap::new(),
            QuotaReadPolicy {
                timeout: Duration::from_millis(5),
                ..QuotaReadPolicy::default()
            },
        )
        .await;

    assert_eq!(result, Err(QuotaTransportError::Timeout));
}
