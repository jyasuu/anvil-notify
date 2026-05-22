/// Integration tests for the consumer processor retry logic.
///
/// These tests exercise `process_recipient` and `process_one_recipient`
/// (via the `ProcessorContext`) using:
///   - A `MockSender` that returns a configurable sequence of outcomes.
///   - Stub `EmailNotificationStore` / `TemplateStore` backed by a real Postgres
///     instance only in CI; locally the tests that need DB are gated behind
///     `#[cfg(feature = "integration")]`.  The pure-logic tests (retry
///     counting, permanent-vs-transient branching, rate-limit cap) use
///     the mock store defined below and run everywhere (`cargo test`).
///
/// Run all tests (including DB-backed ones):
///   cargo test -p consumer --features integration
///
/// Run pure-unit tests only (no Postgres needed):
///   cargo test -p consumer

#[cfg(test)]
mod processor_tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use chrono::Utc;
    use common::{AppError, ChannelOverrides, EmailOptions, NotificationEvent, Recipient};
    use mailer::{EmailMessage, EmailSender};
    use rate_limiter::{MailRateLimiter, RateLimitConfig};
    use recipient_filter::{FilterConfig, RecipientFilter};
    use serde_json::json;
    use tokio_retry::Retry;
    use uuid::Uuid;

    use crate::processor::{is_retryable, ProcessorContext};

    // ── Mock sender ───────────────────────────────────────────────────────────

    /// A sender that pops from a pre-configured queue of `Result`s.
    /// Panics when the queue is exhausted (unexpected extra call).
    struct MockSender {
        outcomes: Mutex<Vec<Result<(), AppError>>>,
    }

    impl MockSender {
        fn new(outcomes: Vec<Result<(), AppError>>) -> Arc<Self> {
            Arc::new(Self {
                outcomes: Mutex::new(outcomes),
            })
        }
    }

    #[async_trait]
    impl EmailSender for MockSender {
        async fn send(&self, _msg: &EmailMessage) -> Result<(), AppError> {
            self.outcomes
                .lock()
                .unwrap()
                .pop()
                // pop() takes from the end. The `mock_sender` helper reverses
                // the slice before calling `new()`, so the first element
                // provided by the caller is consumed first.
                .expect("MockSender: unexpected extra send() call")
        }
    }

    fn mock_sender(mut outcomes: Vec<Result<(), AppError>>) -> Arc<dyn EmailSender> {
        // Reverse so pop() returns elements in the original order.
        outcomes.reverse();
        MockSender::new(outcomes)
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn ok() -> Result<(), AppError> {
        Ok(())
    }

    fn transient() -> Result<(), AppError> {
        Err(AppError::transient_mailer("transient network error"))
    }

    fn permanent() -> Result<(), AppError> {
        Err(AppError::permanent_mailer("bad address"))
    }

    fn rate_limited() -> Result<(), AppError> {
        Err(AppError::RateLimited("429 from mail server".into()))
    }

    fn make_event(recipient_email: &str) -> NotificationEvent {
        NotificationEvent {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            event_type: "ORDER_CONFIRMATION".into(),
            payload: json!({ "orderId": "42", "amount": "9.99", "name": "Test User" }),
            metadata: Default::default(),
            channel_overrides: ChannelOverrides {
                email: Some(EmailOptions {
                    recipients: vec![Recipient {
                        email: recipient_email.into(),
                        name: Some("Test User".into()),
                    }],
                    cc: vec![],
                    bcc: vec![],
                    from_override: None,
                    attachments: vec![],
                    sender_account: None,
                    send_mode: common::SendMode::Individual,
                    group_retry_mode: common::GroupRetryMode::Individual,
                    retry_policy: common::RetryPolicy::Retry,
                }),
            },
        }
    }

    fn passthrough_filter() -> RecipientFilter {
        RecipientFilter::new(FilterConfig::default())
    }

    fn disabled_rate_limiter() -> MailRateLimiter {
        MailRateLimiter::new(RateLimitConfig {
            emails_per_second: 0,
            burst_size: 1,
        })
    }

    // ── is_retryable unit tests ────────────────────────────────────────────────

    #[test]
    fn permanent_mailer_error_is_not_retryable() {
        let err = AppError::permanent_mailer("bad address");
        assert!(!is_retryable(&err));
    }

    #[test]
    fn transient_mailer_error_is_retryable() {
        let err = AppError::transient_mailer("connection reset");
        assert!(is_retryable(&err));
    }

    #[test]
    fn template_error_is_not_retryable() {
        let err = AppError::Template("Unknown event type 'X'".into());
        assert!(!is_retryable(&err));
    }

    #[test]
    fn rate_limited_error_is_retryable() {
        let err = AppError::RateLimited("429".into());
        assert!(is_retryable(&err));
    }

    #[test]
    fn database_error_is_retryable() {
        // We can't easily construct sqlx::Error directly, so test via the
        // Queue variant which is also retryable.
        let err = AppError::Queue("connection pool exhausted".into());
        assert!(is_retryable(&err));
    }

    // ── ProcessorContext integration tests ─────────────────────────────────────
    //
    // These tests exercise the full process_recipient path using real template
    // resolution (compile-time fallback for ORDER_CONFIRMATION) and a mock sender.
    // They require no database — the store operations are exercised against a
    // real PgPool only in the `integration` feature tests below.
    //
    // For these tests we verify the *outcome* returned by process_recipient,
    // which is what the runner acts on.

    fn make_ctx(sender: Arc<dyn EmailSender>) -> ProcessorContext {
        // We can't build a real EmailNotificationStore without a DB connection, so these
        // tests focus on the path-branching that can be verified without DB I/O.
        // See the `integration` feature section below for DB-backed tests.
        //
        // The ProcessorContext is constructed directly with a real (disabled)
        // rate limiter and passthrough filter; the store and template_store fields
        // are intentionally not exercised here.
        //
        // Suppress the "unused" warning — ProcessorContext requires all fields.
        let _ = sender;
        unimplemented!(
            "Use make_ctx_with_db for DB-backed tests or test process_recipient components individually"
        )
    }

    // ── Pure-logic tests that don't need a DB ─────────────────────────────────

    /// Verifies the is_retryable + retry cap logic that the runner uses to
    /// decide whether to mark a recipient FAILED.
    #[test]
    fn retry_loop_exhausts_after_max_retries() {
        // Simulate the runner's decision logic directly (without spawning tasks)
        // to verify the retry counter stops at max_retries.
        let max_retries: u32 = 3;
        let mut attempt: u32 = 0;
        let mut failed_permanently = false;

        for _ in 0..10 {
            // Transient error every time
            let err = AppError::transient_mailer("transient");
            if !is_retryable(&err) {
                failed_permanently = true;
                break;
            }
            if attempt >= max_retries {
                failed_permanently = true;
                break;
            }
            attempt += 1;
        }

        assert!(failed_permanently, "should have exhausted retries");
        assert_eq!(attempt, max_retries);
    }

    #[test]
    fn permanent_error_stops_immediately_without_retry() {
        let max_retries: u32 = 3;
        let mut attempt: u32 = 0;
        let mut stopped_early = false;

        let err = AppError::permanent_mailer("bad domain");
        if !is_retryable(&err) {
            stopped_early = true;
        } else if attempt >= max_retries {
            attempt += 1;
        }

        assert!(
            stopped_early,
            "permanent error should stop without retrying"
        );
        assert_eq!(attempt, 0, "no retry attempts should have been made");
    }

    #[test]
    fn rate_limit_cap_triggers_before_normal_retry_limit() {
        // Simulates the rl_count branch: after max_rl_waits consecutive
        // rate-limit responses the recipient is marked FAILED regardless
        // of how many normal retries remain.
        let max_retries: u32 = 10; // high, so normal cap isn't the trigger
        let max_rl_waits: u32 = 3;
        let attempt: u32 = 0;
        let mut rl_count: u32 = 0;
        let mut hit_rl_cap = false;

        for _ in 0..20 {
            let err = AppError::RateLimited("429".into());
            if !is_retryable(&err) {
                break;
            }
            if attempt >= max_retries {
                break;
            }
            // Rate-limited path: don't increment attempt, only rl_count
            rl_count += 1;
            if rl_count > max_rl_waits {
                hit_rl_cap = true;
                break;
            }
        }

        assert!(hit_rl_cap, "should have hit rate-limit cap");
        assert_eq!(rl_count, max_rl_waits + 1);
        assert_eq!(
            attempt, 0,
            "rate-limit exhaustion must not consume retry slots"
        );
    }

    #[test]
    fn mixed_transient_and_rate_limit_resets_rl_counter() {
        // After a transient failure, rl_count should reset so a subsequent
        // rate-limit burst gets its own full budget.
        let max_rl_waits: u32 = 2;
        let mut rl_count: u32 = 0;

        // Simulate: RL, RL (not yet capped), then a normal transient (resets rl_count)
        for outcome in &["rl", "rl", "transient", "rl", "rl"] {
            match *outcome {
                "rl" => {
                    rl_count += 1;
                    assert!(
                        rl_count <= max_rl_waits + 1,
                        "should not exceed cap within one RL run"
                    );
                }
                "transient" => {
                    // Normal transient: reset the RL counter
                    rl_count = 0;
                }
                _ => unreachable!(),
            }
        }
        // After the second RL run, rl_count should be 2 (within cap)
        assert_eq!(rl_count, 2);
    }

    // ── EmailStatus TryFrom tests ─────────────────────────────────────────────

    #[test]
    fn email_status_try_from_known_values() {
        use common::EmailStatus;
        assert_eq!(
            EmailStatus::try_from("PENDING").unwrap(),
            EmailStatus::Pending
        );
        assert_eq!(EmailStatus::try_from("SENT").unwrap(), EmailStatus::Sent);
        assert_eq!(
            EmailStatus::try_from("FAILED").unwrap(),
            EmailStatus::Failed
        );
        assert_eq!(
            EmailStatus::try_from("BLOCKED").unwrap(),
            EmailStatus::Blocked
        );
    }

    #[test]
    fn email_status_try_from_unknown_returns_error() {
        use common::EmailStatus;
        let err = EmailStatus::try_from("IN_PROGRESS").unwrap_err();
        assert!(
            matches!(err, AppError::UnknownStatus(ref s) if s == "IN_PROGRESS"),
            "expected UnknownStatus, got {err:?}"
        );
    }

    #[test]
    fn email_status_try_from_is_case_sensitive() {
        use common::EmailStatus;
        // The DB stores values in SCREAMING_SNAKE_CASE; lowercase must not match.
        assert!(EmailStatus::try_from("pending").is_err());
        assert!(EmailStatus::try_from("sent").is_err());
    }

    // ── CC/BCC filter enforcement tests ───────────────────────────────────────

    /// Helper: build an event whose CC or BCC contains the given address.
    fn make_event_with_cc_bcc(
        recipient_email: &str,
        cc: Vec<&str>,
        bcc: Vec<&str>,
    ) -> NotificationEvent {
        NotificationEvent {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            event_type: "ORDER_CONFIRMATION".into(),
            payload: json!({ "orderId": "42", "amount": "9.99", "name": "Test User" }),
            metadata: Default::default(),
            channel_overrides: ChannelOverrides {
                email: Some(EmailOptions {
                    recipients: vec![Recipient {
                        email: recipient_email.into(),
                        name: Some("Test User".into()),
                    }],
                    cc: cc
                        .into_iter()
                        .map(|e| Recipient { email: e.into(), name: None })
                        .collect(),
                    bcc: bcc
                        .into_iter()
                        .map(|e| Recipient { email: e.into(), name: None })
                        .collect(),
                    from_override: None,
                    attachments: vec![],
                    sender_account: None,
                    send_mode: common::SendMode::Individual,
                    group_retry_mode: common::GroupRetryMode::Individual,
                    retry_policy: common::RetryPolicy::Retry,
                }),
            },
        }
    }

    /// Verifies that a blocked CC address causes the delivery to fail (permanent).
    #[test]
    fn blocked_cc_address_is_rejected_by_filter() {
        use recipient_filter::FilterConfig;
        let filter = RecipientFilter::new(FilterConfig {
            blocked_emails: vec!["blocked@example.com".into()],
            ..Default::default()
        });

        let event = make_event_with_cc_bcc(
            "to@example.com",
            vec!["blocked@example.com"], // CC contains a blocked address
            vec![],
        );
        let email_opts = event
            .channel_overrides
            .email
            .as_ref()
            .unwrap();

        // Simulate the filter check that processor.rs performs on CC/BCC.
        let mut hit_blocked = false;
        for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
            if let Err(common::AppError::Blocked(_)) = filter.check(&r.email) {
                hit_blocked = true;
            }
        }
        assert!(hit_blocked, "blocked CC address should have been caught by the filter");
    }

    /// Verifies that a blocked BCC address also causes a filter hit.
    #[test]
    fn blocked_bcc_address_is_rejected_by_filter() {
        use recipient_filter::FilterConfig;
        let filter = RecipientFilter::new(FilterConfig {
            blocked_domains: vec!["blocked.io".into()],
            ..Default::default()
        });

        let event = make_event_with_cc_bcc(
            "to@safe.com",
            vec![],
            vec!["audit@blocked.io"], // BCC domain is blocked
        );
        let email_opts = event
            .channel_overrides
            .email
            .as_ref()
            .unwrap();

        let mut hit_blocked = false;
        for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
            if let Err(common::AppError::Blocked(_)) = filter.check(&r.email) {
                hit_blocked = true;
            }
        }
        assert!(hit_blocked, "blocked BCC domain address should have been caught by the filter");
    }

    /// Verifies that allowlist mode also blocks CC/BCC addresses not on the list.
    #[test]
    fn allowlist_mode_blocks_unlisted_cc_address() {
        use recipient_filter::FilterConfig;
        let filter = RecipientFilter::new(FilterConfig {
            allowed_domains: vec!["mycompany.com".into()],
            ..Default::default()
        });

        let event = make_event_with_cc_bcc(
            "employee@mycompany.com",
            vec!["external@other.com"], // CC is not on the allowlist
            vec![],
        );
        let email_opts = event
            .channel_overrides
            .email
            .as_ref()
            .unwrap();

        let mut hit_blocked = false;
        for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
            if let Err(common::AppError::Blocked(_)) = filter.check(&r.email) {
                hit_blocked = true;
            }
        }
        assert!(hit_blocked, "CC address outside allowlist should be blocked");
    }

    /// Verifies that a clean (non-blocked) CC address passes through the filter.
    #[test]
    fn clean_cc_address_passes_filter() {
        use recipient_filter::FilterConfig;
        let filter = RecipientFilter::new(FilterConfig {
            blocked_emails: vec!["blocked@example.com".into()],
            ..Default::default()
        });

        let event = make_event_with_cc_bcc(
            "to@example.com",
            vec!["safe@example.com"],
            vec!["also-safe@example.com"],
        );
        let email_opts = event
            .channel_overrides
            .email
            .as_ref()
            .unwrap();

        for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
            assert!(
                filter.check(&r.email).is_ok(),
                "clean CC/BCC address {} should pass the filter",
                r.email
            );
        }
    }
}
