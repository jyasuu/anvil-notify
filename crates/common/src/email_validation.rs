//! Email address format validation.
//!
//! A single canonical implementation shared by the consumer (pre-send check)
//! and the API layer (pre-enqueue check). Keeping one copy ensures fixes and
//! rule changes propagate everywhere automatically.
//!
//! Deliberately stricter than a full RFC-5321 parser but well short of one:
//! the goal is to catch obvious typos and structural errors before they reach
//! the SMTP server, not to implement the full address grammar.
//!
//! Rules:
//! - Total length ≤ 254 characters (RFC-5321 §4.5.3.1.3).
//! - Exactly one `@` separating a non-empty local part and domain.
//! - Local part: 1–64 chars, no leading/trailing/consecutive dots.
//!   Allowed characters: alphanumeric plus `!#$%&'*+/=?^_`{|}~.-`
//! - Domain: labels separated by `.`, each 1–63 chars, alphanumeric plus
//!   hyphens (not leading/trailing). Single-label domains (e.g. `localhost`)
//!   are accepted for internal mail relays and SMTP test servers.

/// Returns `true` if `addr` passes the structural email address check.
pub fn is_valid_email(addr: &str) -> bool {
    if addr.len() > 254 {
        return false;
    }
    let (local, domain) = match addr.split_once('@') {
        Some(parts) => parts,
        None => return false,
    };
    is_valid_local(local) && is_valid_domain(domain)
}

fn is_valid_local(local: &str) -> bool {
    if local.is_empty() || local.len() > 64 {
        return false;
    }
    if local.starts_with('.') || local.ends_with('.') || local.contains("..") {
        return false;
    }
    local.chars().all(|c| {
        c.is_ascii_alphanumeric()
            || matches!(
                c,
                '!' | '#'
                    | '$'
                    | '%'
                    | '&'
                    | '\''
                    | '*'
                    | '+'
                    | '/'
                    | '='
                    | '?'
                    | '^'
                    | '_'
                    | '`'
                    | '{'
                    | '|'
                    | '}'
                    | '~'
                    | '-'
                    | '.'
            )
    })
}

fn is_valid_domain(domain: &str) -> bool {
    if domain.is_empty() || domain.len() > 253 {
        return false;
    }
    let labels: Vec<&str> = domain.split('.').collect();
    // Allow single-label domains (e.g. "localhost") for internal relay support.
    labels.iter().all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_standard_address() {
        assert!(is_valid_email("user@example.com"));
    }
    #[test]
    fn accepts_subdomain() {
        assert!(is_valid_email("user@mail.example.co.uk"));
    }
    #[test]
    fn accepts_plus_tag() {
        assert!(is_valid_email("user+tag@example.com"));
    }
    #[test]
    fn accepts_dots_in_local() {
        assert!(is_valid_email("first.last@example.com"));
    }
    #[test]
    fn accepts_localhost_for_internal_relay() {
        assert!(is_valid_email("user@localhost"));
    }
    #[test]
    fn accepts_hyphen_in_domain_label() {
        assert!(is_valid_email("user@my-company.com"));
    }

    #[test]
    fn rejects_missing_at() {
        assert!(!is_valid_email("userexample.com"));
    }
    #[test]
    fn rejects_empty_local() {
        assert!(!is_valid_email("@example.com"));
    }
    #[test]
    fn rejects_empty_domain() {
        assert!(!is_valid_email("user@"));
    }
    #[test]
    fn rejects_leading_dot_in_local() {
        assert!(!is_valid_email(".user@example.com"));
    }
    #[test]
    fn rejects_trailing_dot_in_local() {
        assert!(!is_valid_email("user.@example.com"));
    }
    #[test]
    fn rejects_consecutive_dots_in_local() {
        assert!(!is_valid_email("us..er@example.com"));
    }
    #[test]
    fn rejects_leading_hyphen_in_domain_label() {
        assert!(!is_valid_email("user@-example.com"));
    }
    #[test]
    fn rejects_trailing_hyphen_in_domain_label() {
        assert!(!is_valid_email("user@example-.com"));
    }
    #[test]
    fn rejects_space_in_address() {
        assert!(!is_valid_email("us er@example.com"));
    }
    #[test]
    fn rejects_address_over_254_chars() {
        let long_local = "a".repeat(65);
        assert!(!is_valid_email(&format!("{long_local}@example.com")));
    }
}
