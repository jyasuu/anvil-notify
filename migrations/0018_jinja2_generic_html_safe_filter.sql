-- migrations/0018_jinja2_generic_html_safe_filter.sql
--
-- Migrates all built-in templates from the old {{key}} hand-rolled syntax
-- to Jinja2 (minijinja) syntax: {{ key }}.
--
-- The critical fix is GENERIC_HTML's body_html column: the placeholder is
-- changed from {{ body_html }} (which was being HTML-escaped, destroying the
-- caller's HTML) to {{ body_html | safe }}, which inserts the caller-supplied
-- HTML verbatim.  The caller is responsible for the safety of that content.
--
-- All other templates are functionally unchanged — minijinja's {{ variable }}
-- syntax is identical to the old {{variable}} for simple interpolation.

UPDATE email_template
SET
    subject  = '{{ subject }}',
    body_html = concat(
        '<!DOCTYPE html><html>',
        '<head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width,initial-scale=1"></head>',
        '<body style="margin:0;padding:24px;font-family:sans-serif;color:#111">',
        '{{ body_html | safe }}',
        '</body></html>'
    ),
    body_text = '{{ body_text }}',
    version   = version + 1,
    updated_at = now()
WHERE type = 'GENERIC_HTML';

-- GENERIC_TEXT: body is plain text, no | safe needed.
UPDATE email_template
SET
    subject   = '{{ subject }}',
    body_html = '<div style="font-family:sans-serif;white-space:pre-wrap">{{ body }}</div>',
    body_text = '{{ body }}',
    version   = version + 1,
    updated_at = now()
WHERE type = 'GENERIC_TEXT';

-- Remaining built-ins: variable names unchanged, just spacing normalised.
UPDATE email_template
SET
    subject   = 'Order {{ orderId }} confirmed',
    body_html = '<h1>Hi {{ name }},</h1><p>Your order <strong>{{ orderId }}</strong> of ${{ amount }} has been confirmed.</p>',
    body_text = 'Hi {{ name }}, Your order {{ orderId }} of ${{ amount }} has been confirmed.',
    version   = version + 1,
    updated_at = now()
WHERE type = 'ORDER_CONFIRMATION';

UPDATE email_template
SET
    subject   = 'Reset your password',
    body_html = '<p>Click <a href="{{ resetLink }}">here</a> to reset your password.</p>',
    body_text = 'Visit this link to reset your password: {{ resetLink }}',
    version   = version + 1,
    updated_at = now()
WHERE type = 'PASSWORD_RESET';

UPDATE email_template
SET
    subject   = 'Welcome to {{ appName }}!',
    body_html = '<h1>Welcome, {{ name }}!</h1><p>Thanks for joining {{ appName }}.</p>',
    body_text = 'Welcome, {{ name }}! Thanks for joining {{ appName }}.',
    version   = version + 1,
    updated_at = now()
WHERE type = 'WELCOME';