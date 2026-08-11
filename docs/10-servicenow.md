# 10: ServiceNow integration

Pull incidents from ServiceNow, generate an RCA, write it back as a work note.

**If you only want the setup steps, read the next section and stop.** Everything
after it is how it works and why.

---

## Setup, start to finish

You need a ServiceNow instance. A free personal developer instance is enough.

### 1. Get an instance

Sign up at **developer.servicenow.com**, then **Request Instance**. You get:

```
Instance URL   https://devXXXXX.service-now.com
Username       admin
Password       shown on the "Manage my instance" page
```

The password sits behind an **eye icon**. Click it to reveal, then copy. Copying
the masked dots is the single most common way this goes wrong.

Personal instances hibernate after about 10 days idle. Wake them from the same
page. If the developer portal itself is slow or stuck loading, that is its
problem, not yours; try later.

### 2. Put the password in `.env`

```bash
cd ~/AIRS
read -rs SN_PASS
```

The terminal goes blank. That is correct. Paste the password, press Enter.

```bash
echo "SERVICENOW_PASSWORD=$SN_PASS" >> .env
```

`.env` is gitignored, so this is never committed.

### 3. Check the credentials work

```bash
curl -s -u "admin:$SN_PASS" \
  'https://devXXXXX.service-now.com/api/now/table/incident?sysparm_limit=1&sysparm_fields=number'
```

Good: `{"result":[{"number":"INC0000001"}]}`

Bad: `{"error":{"message":"User is not authenticated"...`

If it is bad, in order of likelihood: you copied the masked dots rather than the
revealed password; or the account is locked after repeated failed attempts, in
which case reset it from **Actions** on the developer portal page.

### 4. Point AIRS at it

In [`config/airs.yaml`](../config/airs.yaml):

```yaml
connectors:
  servicenow:
    enabled: true                                    # was false
    instance_url: https://devXXXXX.service-now.com   # your instance
    dry_run: true                                    # leave this alone for now
```

### 5. Run it in dry run and read what it would write

```bash
docker compose up -d
docker compose logs -f connector-service
```

With `dry_run: true` the sink logs the exact work note it *would* post and
writes nothing. Create or update an incident in ServiceNow, wait for the poll
interval, and watch it come through.

### 6. Only then, go live

Once you have read a few notes and are happy with them:

```yaml
    dry_run: false
```

```bash
docker compose up -d connector-service
```

Work notes now appear on real tickets, visible to anyone looking at them.

---

## Optional: a dedicated user

Running as `admin` is fine on a throwaway developer instance. For anything you
care about, make a user with only the access this needs:

1. In the instance, go to **System Security > Users and Groups > Users**, **New**
2. User ID `airs_integration`, tick **Web service access only**
3. Save, then use the **Set Password** related link. The password field is
   hidden on the form by default, which is why setting it is a separate step
4. Make sure **Password needs reset** is unticked, or API logins fail
5. On the **Roles** tab, add `itil`

Then set `username: airs_integration` in the config and put that password in
`.env` instead. `itil` is read and write on incidents and nothing else, so
revoking one role cuts AIRS off completely.

---

## What it actually does

```
ServiceNow                 connector-service              AIRS pipeline
    │                             │                            │
    │  poll incidents changed     │                            │
    │  since last watermark       │                            │
    ├────────────────────────────▶│                            │
    │                             │  map to Incident           │
    │                             ├───────────────────────────▶│  incidents-topic
    │                             │                            │
    │                             │                     ai-service generates RCA
    │                             │                            │
    │                             │◀───────────────────────────┤  enriched-incidents-topic
    │  PATCH work_notes           │                            │
    │◀────────────────────────────┤                            │
```

### It enters at `incidents-topic`, not `logs-topic`

A ServiceNow incident is already an incident. Somebody, or some rule, already
decided that these symptoms are one thing. Running it through AIRS detection
and correlation would produce a second, competing opinion about the same event.

So the source declares `kind = SourceKind.incidents` and connector-service
routes on that. Detection and correlation are skipped entirely.

### Field mapping

| ServiceNow | AIRS | Note |
| --- | --- | --- |
| `sys_id` | `external_id` | how the sink finds the ticket again |
| `number` | in `summary` | INC0010042 |
| `priority` | `severity` | 1 and 2 to critical, 3 to warning, 4 and 5 to info |
| `cmdb_ci` | `service` | falls back to assignment group, then category |
| `short_description`, `description` | `timeline` | |
| `sys_updated_on` | poll cursor | the watermark for the next fetch |

`service` is the weakest of these. A ServiceNow incident often has no
configuration item set, and then AIRS is grouping by assignment group, which is
who owns the problem rather than what broke. Set `cmdb_ci` on your incidents if
you want this to be good.

---

## The loop, and why it does not happen

This is the failure mode worth understanding, because it is silent and it
escalates.

Writing a work note updates the ticket. Updating the ticket bumps
`sys_updated_on`. The poller fetches everything changed since its watermark. So
without protection:

> AIRS writes a note → ticket looks changed → poller re-fetches it → new RCA →
> another note → forever, at the poll interval, on a real person's ticket.

The guard is one clause in the query:

```
sys_updated_by!=<the account AIRS uses>
```

AIRS never sees its own writes. This is tested end to end against a fake
instance that honours the exclusion, not just asserted on the query string.

**If you change `username` in the config, the guard follows it automatically**,
because it is built from the configured account rather than hardcoded.

---

## Writing twice, and why that does not happen either

A retry after a lost response would otherwise post the same work note twice.
Every outbound call goes through `OutboundAdapter` with an idempotency key
derived from the tenant, the incident id and the root cause text.

The root cause is deliberately part of the key. A *regenerated* RCA that reached
a different conclusion is genuinely new information and should be posted. The
same RCA arriving twice is not.

---

## When ServiceNow is down

Personal instances hibernate, and real ones have maintenance windows. Nothing
about that is allowed to affect AIRS:

- A failed poll is logged, counted in `airs_connector_polls_total{status="error"}`,
  and retried at the next interval. It does not crash the service.
- A failed delivery returns an unsuccessful result rather than raising. The RCA
  is already durable in OpenSearch before delivery is attempted.
- connector-service reports itself healthy even when ServiceNow is unreachable,
  because a hibernating third party is not this service being broken. Restarting
  it would fix nothing.

**A failed delivery is not currently retried after its attempts are exhausted.**
It is counted in `airs_connector_sink_deliveries_total{status="error"}` and that
is all. Re-delivery would need the DLQ drain that
[01-scope-and-non-goals.md](01-scope-and-non-goals.md) records as missing.

---

## Metrics

```text
airs_connector_records_fetched_total{source}
airs_connector_records_published_total{source, topic}
airs_connector_polls_total{source, status}
airs_connector_poll_duration_seconds{source}
airs_connector_sink_deliveries_total{sink, status}   # status = ok | error | duplicate
airs_connector_source_up{source}
airs_rca_enriched_published_total{service}
```

The one to watch is `airs_connector_sink_deliveries_total{status="error"}`.
Since a delivery failure never surfaces as an incident failure by design, this
counter is the only place it appears.

---

## Adding another system

The seams are generic. Jira, PagerDuty and Opsgenie are all the same shape:

1. Implement `Source` in `services/connector-service/app/`, set `kind`
2. Implement `Sink` for the write-back, wrapped in `OutboundAdapter`
3. Register both in `build_sources()` and `build_sinks()`

Two rules. The source must return an empty list rather than raising when there
is nothing new, because the caller treats an exception as a failed poll. The
sink must never raise, because one unreachable destination must not deny the
others.

And the constraint the whole arrangement exists to protect, which has a test:
**no core pipeline service may mention the external system.**

---

Previous: [09: Postmortem](09-postmortem.md) | Back to [README](../README.md)
