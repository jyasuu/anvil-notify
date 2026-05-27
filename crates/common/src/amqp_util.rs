/// Redact credentials from an AMQP URL before storing or logging.
///
/// Replaces `user:password@` with `[redacted]@` so broker host / vhost remain
/// visible in logs while credentials never appear in plaintext.
///
/// Uses `rfind('@')` so a password containing a literal `@` (which should be
/// percent-encoded but may not be in a misconfigured URL) does not leak.
pub fn scrub_amqp_url(url: &str) -> String {
    if let Some(at_pos) = url.rfind('@') {
        if let Some(scheme_end) = url.find("://") {
            let scheme = &url[..scheme_end + 3];
            let after_at = &url[at_pos + 1..];
            return format!("{scheme}[redacted]@{after_at}");
        }
        return "[redacted — unrecognised URL format containing '@']".to_owned();
    }
    url.to_owned()
}

#[cfg(test)]
mod tests {
    use super::scrub_amqp_url;

    #[test]
    fn redacts_standard_credentials() {
        assert_eq!(
            scrub_amqp_url("amqp://user:secret@broker.example.com:5672"),
            "amqp://[redacted]@broker.example.com:5672"
        );
    }

    #[test]
    fn passthrough_when_no_at_sign() {
        let url = "amqps://broker.example.com:5671";
        assert_eq!(scrub_amqp_url(url), url);
    }

    #[test]
    fn redacts_password_containing_at_sign() {
        let result = scrub_amqp_url("amqp://user:p@ss@broker.example.com:5672");
        assert!(!result.contains("p@ss"), "password leaked: {result}");
        assert!(result.contains("broker.example.com"));
    }
}
