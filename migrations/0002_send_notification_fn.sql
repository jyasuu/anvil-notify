-- migrations/0002_send_notification_fn.sql
--
-- Installs notify_send_email() and its validator helpers.
--
-- This is a convenience wrapper that business services can call inside their
-- own transactions to enqueue an email event in the outbox table without
-- constructing the JSONB payload by hand.
--
-- This file consolidates migrations 0012, 0021, and 0022 (the incremental
-- additions of CC/BCC support and the send_mode / sender_account parameters).
-- The installed function is the final version from 0022.
--
-- ── Usage ─────────────────────────────────────────────────────────────────────
--
-- Simple order confirmation (single recipient):
--
--   BEGIN;
--     INSERT INTO orders (...) VALUES (...) RETURNING id INTO v_order_id;
--
--     PERFORM notify_send_email(
--       p_event_type => 'ORDER_CONFIRMATION',
--       p_recipient  => jsonb_build_object('email', v_email, 'name', v_name),
--       p_payload    => jsonb_build_object('orderId', v_order_id, 'amount', v_amount),
--       p_event_id   => v_order_id   -- use order UUID as idempotency key
--     );
--   COMMIT;
--
-- Group send — all recipients see each other in the To: header:
--
--   SELECT notify_send_email(
--     p_event_type => 'TEAM_ALERT',
--     p_recipients => '[
--       {"email":"alice@acme.com","name":"Alice"},
--       {"email":"bob@acme.com","name":"Bob"}
--     ]'::jsonb,
--     p_send_mode  => 'group',
--     p_payload    => '{"alertTitle":"Disk usage critical","threshold":"90%"}'::jsonb
--   );
--
-- With CC/BCC, custom From, named SMTP account, and attachment:
--
--   SELECT notify_send_email(
--     p_event_type     => 'INVOICE_READY',
--     p_recipient      => '{"email":"alice@example.com"}'::jsonb,
--     p_payload        => '{"invoiceId":"INV-42"}'::jsonb,
--     p_cc             => '[{"email":"manager@acme.com","name":"Manager"}]'::jsonb,
--     p_bcc            => '[{"email":"audit@acme.com"}]'::jsonb,
--     p_from_override  => '{"email":"billing@acme.com","name":"Acme Billing"}'::jsonb,
--     p_sender_account => 'billing',
--     p_attachments    => '[{
--       "url":          "https://storage.example.com/inv-42.pdf?token=xyz",
--       "filename":     "invoice-42.pdf",
--       "content_type": "application/pdf",
--       "max_age_secs": 300
--     }]'::jsonb
--   );
--
-- ── Parameters ────────────────────────────────────────────────────────────────
--
--   p_event_type      — Required. Matched against notification_template.type.
--
--   p_recipient       — Single TO recipient: {"email":"...","name":"..."}.
--   p_recipients      — Array of TO recipients. Supply exactly one of these two.
--
--   p_send_mode       — 'individual' (default): each recipient gets a separate
--                       email with its own tracking row and retry loop.
--                       'group': all recipients share one email; all addresses
--                       appear in the To: header together.
--
--   p_cc / p_bcc      — Optional arrays of CC/BCC recipients. Same shape as
--                       p_recipients. Neither creates independent tracking rows.
--
--   p_payload         — Template variables forwarded to the Jinja2 renderer.
--
--   p_from_override   — Optional From address override for this event only.
--                       {"email":"orders@acme.com","name":"Acme Orders"}
--
--   p_attachments     — Optional URL-based attachment references.
--                       [{"url":"...","filename":"...","content_type":"...","max_age_secs":N}]
--
--   p_sender_account  — Optional named SMTP account from [sender_accounts] config.
--                       NULL → use global [mailer] default.
--
--   p_metadata        — Optional arbitrary metadata forwarded to the consumer.
--
--   p_event_id        — Optional stable idempotency key. When NULL a random UUID
--                       is generated. Pass a business-entity UUID (e.g. order_id)
--                       to guarantee at-most-once insertion on transaction retry.
--
-- Returns the event_id UUID that was inserted into the outbox.

-- ── Helper: validate a single recipient object ────────────────────────────────

CREATE OR REPLACE FUNCTION _notify_validate_recipient(p_r jsonb)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_email text;
    v_local text;
    v_domain text;
BEGIN
    IF p_r IS NULL OR jsonb_typeof(p_r) <> 'object' THEN
        RAISE EXCEPTION 'notify_send_email: each recipient must be a JSON object, got: %', p_r
            USING ERRCODE = 'P0001';
    END IF;

    v_email := p_r->>'email';

    IF v_email IS NULL OR v_email = '' THEN
        RAISE EXCEPTION 'notify_send_email: recipient missing required "email" field'
            USING ERRCODE = 'P0001';
    END IF;

    -- Basic structural check mirrors common::is_valid_email:
    --   total ≤ 254 chars, exactly one @, non-empty local and domain parts.
    IF length(v_email) > 254 THEN
        RAISE EXCEPTION 'notify_send_email: recipient email too long (> 254 chars): %', v_email
            USING ERRCODE = 'P0001';
    END IF;

    IF (length(v_email) - length(replace(v_email, '@', ''))) <> 1 THEN
        RAISE EXCEPTION 'notify_send_email: recipient email must contain exactly one "@": %', v_email
            USING ERRCODE = 'P0001';
    END IF;

    v_local  := split_part(v_email, '@', 1);
    v_domain := split_part(v_email, '@', 2);

    IF v_local  = '' THEN
        RAISE EXCEPTION 'notify_send_email: recipient email has empty local part: %', v_email
            USING ERRCODE = 'P0001';
    END IF;
    IF v_domain = '' THEN
        RAISE EXCEPTION 'notify_send_email: recipient email has empty domain part: %', v_email
            USING ERRCODE = 'P0001';
    END IF;
END;
$$;

-- ── Helper: validate an attachments array ─────────────────────────────────────

CREATE OR REPLACE FUNCTION _notify_validate_attachments(p_atts jsonb)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_att  jsonb;
    v_idx  int := 0;
    v_url  text;
    v_fn   text;
    v_ct   text;
BEGIN
    IF p_atts IS NULL THEN
        RETURN;
    END IF;

    IF jsonb_typeof(p_atts) <> 'array' THEN
        RAISE EXCEPTION 'notify_send_email: p_attachments must be a JSON array'
            USING ERRCODE = 'P0001';
    END IF;

    FOR v_att IN SELECT jsonb_array_elements(p_atts) LOOP
        v_idx := v_idx + 1;

        v_url := v_att->>'url';
        v_fn  := v_att->>'filename';
        v_ct  := v_att->>'content_type';

        IF v_url IS NULL OR v_url = '' THEN
            RAISE EXCEPTION 'notify_send_email: attachment[%] missing required "url"', v_idx
                USING ERRCODE = 'P0001';
        END IF;
        IF v_url NOT LIKE 'http://%' AND v_url NOT LIKE 'https://%' THEN
            RAISE EXCEPTION 'notify_send_email: attachment[%] url must start with http:// or https://, got: %', v_idx, v_url
                USING ERRCODE = 'P0001';
        END IF;

        IF v_fn IS NULL OR v_fn = '' THEN
            RAISE EXCEPTION 'notify_send_email: attachment[%] missing required "filename"', v_idx
                USING ERRCODE = 'P0001';
        END IF;
        IF v_fn LIKE '%/%' OR v_fn LIKE '%\%' THEN
            RAISE EXCEPTION 'notify_send_email: attachment[%] filename must not contain path separators, got: %', v_idx, v_fn
                USING ERRCODE = 'P0001';
        END IF;

        IF v_ct IS NULL OR v_ct = '' THEN
            RAISE EXCEPTION 'notify_send_email: attachment[%] missing required "content_type"', v_idx
                USING ERRCODE = 'P0001';
        END IF;
        IF v_ct NOT LIKE '%/%' THEN
            RAISE EXCEPTION 'notify_send_email: attachment[%] content_type must be a valid MIME type (e.g. "application/pdf"), got: %', v_idx, v_ct
                USING ERRCODE = 'P0001';
        END IF;
    END LOOP;
END;
$$;

-- ── Helper: validate a recipient array (for cc / bcc) ─────────────────────────

CREATE OR REPLACE FUNCTION _notify_validate_recipient_list(p_label text, p_list jsonb)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_r   jsonb;
    v_idx int := 0;
BEGIN
    IF p_list IS NULL THEN
        RETURN;
    END IF;

    IF jsonb_typeof(p_list) <> 'array' THEN
        RAISE EXCEPTION 'notify_send_email: % must be a JSON array, got: %', p_label, jsonb_typeof(p_list)
            USING ERRCODE = 'P0001';
    END IF;

    FOR v_r IN SELECT jsonb_array_elements(p_list) LOOP
        v_idx := v_idx + 1;
        IF jsonb_typeof(v_r) <> 'object' THEN
            RAISE EXCEPTION 'notify_send_email: %[%] must be a JSON object, got: %', p_label, v_idx, v_r
                USING ERRCODE = 'P0001';
        END IF;
        PERFORM _notify_validate_recipient(v_r);
    END LOOP;
END;
$$;

-- ── Main function ─────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION notify_send_email(
    -- Required
    p_event_type      text,

    -- TO recipient(s): supply exactly one of p_recipient or p_recipients.
    --   p_recipient  — single TO recipient: {"email":"...","name":"..."}
    --   p_recipients — array  of TO recipients
    p_recipient       jsonb    DEFAULT NULL,
    p_recipients      jsonb    DEFAULT NULL,

    -- Delivery mode for multiple recipients.
    --   'individual' (default) — each recipient gets a separate email with
    --                            its own tracking row and retry loop.
    --   'group'                — all recipients share one email; all addresses
    --                            appear in the To: header together.
    p_send_mode       text     DEFAULT 'individual',

    -- Optional CC / BCC recipient arrays.
    --   Each element: {"email":"addr@example.com","name":"Optional Name"}
    --   Omit or pass NULL for no CC / BCC.
    p_cc              jsonb    DEFAULT NULL,
    p_bcc             jsonb    DEFAULT NULL,

    -- Required: template variables forwarded to the renderer
    p_payload         jsonb    DEFAULT '{}',

    -- Optional: override the From address for this event only
    --   {"email":"orders@acme.com","name":"Acme Orders"}
    p_from_override   jsonb    DEFAULT NULL,

    -- Optional: URL-based attachment references
    p_attachments     jsonb    DEFAULT NULL,

    -- Optional: named SMTP sender account (must match a key under
    -- [sender_accounts] in the notification-service config).
    -- When NULL the service uses its global [mailer] default.
    p_sender_account  text     DEFAULT NULL,

    -- Optional: arbitrary metadata forwarded verbatim to the consumer
    --   {"source":"order-service"}
    p_metadata        jsonb    DEFAULT NULL,

    -- Optional: stable idempotency key. When NULL a random UUID is generated.
    p_event_id        uuid     DEFAULT NULL
)
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
    v_event_id    uuid;
    v_recipients  jsonb;
    v_payload_env jsonb;
    v_r           jsonb;
BEGIN
    -- ── 1. Validate event_type ─────────────────────────────────────────────
    IF p_event_type IS NULL OR p_event_type = '' THEN
        RAISE EXCEPTION 'notify_send_email: p_event_type must not be empty'
            USING ERRCODE = 'P0001';
    END IF;

    -- ── 2. Validate send_mode ──────────────────────────────────────────────
    IF p_send_mode IS NULL OR p_send_mode NOT IN ('individual', 'group') THEN
        RAISE EXCEPTION 'notify_send_email: p_send_mode must be ''individual'' or ''group'', got: %',
            COALESCE(p_send_mode, 'NULL')
            USING ERRCODE = 'P0001';
    END IF;

    -- ── 3. Resolve TO recipients ───────────────────────────────────────────
    IF p_recipient IS NOT NULL AND p_recipients IS NOT NULL THEN
        RAISE EXCEPTION 'notify_send_email: supply p_recipient OR p_recipients, not both'
            USING ERRCODE = 'P0001';
    END IF;

    IF p_recipient IS NOT NULL THEN
        PERFORM _notify_validate_recipient(p_recipient);
        v_recipients := jsonb_build_array(p_recipient);

    ELSIF p_recipients IS NOT NULL THEN
        IF jsonb_typeof(p_recipients) <> 'array' THEN
            RAISE EXCEPTION 'notify_send_email: p_recipients must be a JSON array'
                USING ERRCODE = 'P0001';
        END IF;
        IF jsonb_array_length(p_recipients) = 0 THEN
            RAISE EXCEPTION 'notify_send_email: p_recipients must not be empty'
                USING ERRCODE = 'P0001';
        END IF;
        FOR v_r IN SELECT jsonb_array_elements(p_recipients) LOOP
            PERFORM _notify_validate_recipient(v_r);
        END LOOP;
        v_recipients := p_recipients;

    ELSE
        RAISE EXCEPTION 'notify_send_email: one of p_recipient or p_recipients is required'
            USING ERRCODE = 'P0001';
    END IF;

    -- group mode with a single recipient is pointless but harmless; allow it.

    -- ── 4. Validate CC / BCC lists ─────────────────────────────────────────
    PERFORM _notify_validate_recipient_list('p_cc',  p_cc);
    PERFORM _notify_validate_recipient_list('p_bcc', p_bcc);

    -- ── 5. Validate from_override ──────────────────────────────────────────
    IF p_from_override IS NOT NULL THEN
        IF jsonb_typeof(p_from_override) <> 'object' THEN
            RAISE EXCEPTION 'notify_send_email: p_from_override must be a JSON object'
                USING ERRCODE = 'P0001';
        END IF;
        IF p_from_override->>'email' IS NULL OR p_from_override->>'email' = '' THEN
            RAISE EXCEPTION 'notify_send_email: p_from_override must contain a non-empty "email" field'
                USING ERRCODE = 'P0001';
        END IF;
        PERFORM _notify_validate_recipient(jsonb_build_object('email', p_from_override->>'email'));
    END IF;

    -- ── 6. Validate attachments ────────────────────────────────────────────
    PERFORM _notify_validate_attachments(p_attachments);

    -- ── 7. Build payload envelope ──────────────────────────────────────────
    v_event_id := COALESCE(p_event_id, gen_random_uuid());

    -- jsonb_strip_nulls omits NULL-valued keys so the worker's .get() returns
    -- None for absent optional fields, which it already treats as the default.
    v_payload_env := jsonb_strip_nulls(jsonb_build_object(
        'recipients',     v_recipients,
        'send_mode',      p_send_mode,
        'payload',        COALESCE(p_payload, '{}'),
        'from_override',  p_from_override,
        'attachments',    COALESCE(p_attachments, '[]'::jsonb),
        'cc',             p_cc,
        'bcc',            p_bcc,
        'sender_account', p_sender_account,
        'metadata',       COALESCE(p_metadata, '{}'::jsonb)
    ));

    -- ── 8. Insert into outbox (idempotent on p_event_id) ──────────────────
    -- ON CONFLICT DO NOTHING means callers can safely retry on transaction
    -- failure without risk of duplicate sends.
    INSERT INTO outbox (event_id, event_type, payload)
    VALUES             (v_event_id, p_event_type, v_payload_env)
    ON CONFLICT (event_id) DO NOTHING;

    RETURN v_event_id;
END;
$$;
