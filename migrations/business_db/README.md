# Business DB Migrations

These SQL files apply to the **business service database** — the one your
application writes orders, users, etc. into. They are **not** run by the
notification service's `sqlx::migrate!()` call (which targets the
notification DB at `database.url`).

Run them manually, or embed them in your business service's own migration
pipeline:

```bash
# Example — psql
```

| File | Purpose |
| ---- | ------- |
