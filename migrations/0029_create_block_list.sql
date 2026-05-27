-- migrations/0029_create_block_list.sql
--
-- Moves the recipient block/allow-list from static config files into the
-- database so operators can add or remove entries at runtime via the HTTP API
-- without restarting the service.
--
-- Design notes:
--   • `kind` discriminates the entry type:
--       'blocked_email'   — exact email address that must never receive mail.
--       'blocked_domain'  — entire domain blocked (e.g. 'competitor.com').
--       'allowed_email'   — allowlist mode: only this address may receive mail.
--       'allowed_domain'  — allowlist mode: only this domain may receive mail.
--   • `value` is stored lowercase; the application normalises on write.
--   • `active` allows soft-delete without losing audit history.
--   • `reason` is a free-text operator note (who added it, why).
--   • The cache TTL means changes propagate within seconds (default 30 s);
--     call DELETE /admin/blocklist/cache to force immediate reload.

CREATE TABLE IF NOT EXISTS block_list (
    id         BIGSERIAL   PRIMARY KEY,
    kind       TEXT        NOT NULL
                           CHECK (kind IN (
                               'blocked_email', 'blocked_domain',
                               'allowed_email',  'allowed_domain'
                           )),
    value      TEXT        NOT NULL,
    reason     TEXT,
    active     BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Unique on (kind, value) so the same entry cannot be added twice.
CREATE UNIQUE INDEX IF NOT EXISTS block_list_kind_value_idx
    ON block_list (kind, lower(value));

COMMENT ON TABLE  block_list       IS 'Runtime recipient block/allow-list managed via the HTTP API.';
COMMENT ON COLUMN block_list.kind  IS 'Entry type: blocked_email | blocked_domain | allowed_email | allowed_domain';
COMMENT ON COLUMN block_list.value IS 'Lowercase email address or domain. Normalised to lowercase on insert.';
COMMENT ON COLUMN block_list.reason IS 'Operator note explaining why this entry was added.';
COMMENT ON COLUMN block_list.active IS 'Soft-delete flag. Set to FALSE to disable without losing history.';
