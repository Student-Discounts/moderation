# Moderation and Media

Two independently runnable services in one Gradle repository. **No Catalog implementation.**

- `moderation-service` owns review assignments, decisions, reports and append-only audit. It projects immutable Submission events into its own PostgreSQL database and sends human commands back through Kafka.
- `media-service` owns private S3 objects, upload metadata, image processing and check results, with a separate PostgreSQL database.
- Submission remains the lifecycle/version owner. Auth remains the identity/role/token owner. No cross-service database reads or writes.

Requirements: Java **25**, installed **Gradle 9.7.1**, Docker Compose. Spring Boot **4.1.2** matches the existing Submission runtime and supersedes the Java 21/Boot 3 diagram labels. No wrapper, Maven, Node, Redis or service-discovery dependency.

## Local startup

From this repository:

```bash
# macOS Homebrew only, if the default java points elsewhere:
export JAVA_HOME=/usr/local/opt/openjdk@25/libexec/openjdk.jdk/Contents/Home
docker compose up -d --wait moderation-db media-db kafka s3
gradle build --console=plain

# Two terminals:
gradle :moderation-service:bootRun --args='--spring.profiles.active=local'
gradle :media-service:bootRun --args='--spring.profiles.active=local'
```

Alternatively, after `gradle assemble`, run `docker compose --profile apps up -d --build`.
The runtime-only image runs as UID/GID 10001 and contains the selected service's boot jar, not sources or a build tool.

| Component | Local address |
|---|---|
| Moderation / OpenAPI | `http://localhost:8082` / `/openapi.yaml` |
| Media / OpenAPI | `http://localhost:8083` / `/openapi.yaml` |
| Moderation PostgreSQL | `localhost:5434/moderation` |
| Media PostgreSQL | `localhost:5435/media` |
| Kafka | `localhost:9092` |
| Private S3 API / console | `localhost:9000` / `localhost:9001` |

Compose credentials are **local-only**, and all published ports bind loopback. The `local` profile explicitly supplies local credentials; do not activate it in production. Local S3 runs [Silo](https://github.com/pgsty/silo), a maintained MinIO-compatible distribution. Local credentials: `minio-local` / `minio-local-secret-change-me`. Bucket `media-private` has no anonymous policy or public proof URL.

Auth is operated separately on port 8080. These services neither start it nor issue runtime tokens. Obtain access tokens through its registration/login API. Senior bootstrap and moderator role management belong to Auth. Standard offline JWT checks cannot observe revoked sessions/roles until token expiry (Auth currently issues five-minute access tokens).

### Connect the existing Submission service

Use **one Kafka broker** across all services. Do not start Submission's second Compose Kafka on the same port. Its PostgreSQL and explicit Catalog/Geo fixtures may be started separately:

```bash
cd ../submission-service
docker compose up -d --wait postgres stubs
SD_SUBMISSION_MEDIA_BASE_URL=http://localhost:8083 \
SD_SUBMISSION_MEDIA_SERVICE_TOKEN=local-media-service-token-change-me \
gradle bootRun --args='--spring.profiles.active=local'
```

Submission's small Media-client integration adds a service credential only to Media requests; its existing metadata route and ownership checks are unchanged. Catalog/Geo/organization fixtures are not real implementations and unknown requests fail. The normal workflow needs an active category and, for approval, a resolved location/timezone. Running the new services alone does not supply these external capabilities.

## Contracts and ownership

The implemented wire contracts follow `docs/design/DESIGN.md`, `docs/c4/C4.md`, `docs/data-model/DATA-MODEL.md` and the **actual** versioned schemas in `submission-service/contracts/events/`.

| Boundary | Contract |
|---|---|
| Submission → Moderation | `submission.events.v1`, key `submissionId`; `SubmissionSubmitted`, `SubmissionVersionCreated`, `SubmissionStatusChanged`, immutable snapshots plus `aggregateSequence` |
| Moderation → Submission | `submission.workflow.v1`, exact reviewed `{submissionId,versionId,versionNo,expectedRowVersion,actorId}`; approval also includes the unchanged payload |
| Submission → Media | `GET /media/v1/assets/{id}`, `X-Service-Token`; `{id,ownerId,status,revision}` |
| Media → Submission | `submission.workflow.v1`, key `mediaAssetId`; producer `media`, `MediaAssetChecked`, data `{mediaAssetId,status,revision}` |
| Moderation command failure | Original command on `submission.workflow.v1.DLT`, correlated by event ID; rejection is visible, not treated as acceptance |
| Moderation reports | [`moderation.events.v1`](moderation-service/contracts/events/moderation-events-v1.schema.json), key `reportId`; observations only, not visibility commands |

Events use stable `eventId`, `schemaVersion:1`, `producer`, `occurredAt`, `correlationId` and `data`. Delivery is at least once: committed outbox rows are acknowledged by Kafka before being marked delivered; consumers must deduplicate. `producer` is a contract field, **not authentication**. Production broker ACLs/TLS/SASL must restrict writers/readers and protect private snapshots/DLT content.

Provision the topics in the table and `moderation.consumer.v1.DLT` before nonlocal startup. The moderation DLT must support the partition numbers of its inputs. Only Submission may write its committed events and workflow DLT. All principals with write access to the shared workflow topic must be trusted for workflow commands; its current contract does not authenticate each producer name separately. Do not truncate the submission event stream ahead of Moderation's recoverable projection or bootstrap a fresh database from a partial stream.

### Review workflow

1. Submission validates an immutable version and confirms `PENDING_REVIEW`.
2. Reviewer claims that exact version using `If-Match` and `Idempotency-Key`.
3. Moderation records a pending command. It waits for Submission's committed `IN_REVIEW`; broker delivery alone is not acceptance.
4. Only the assigned nonauthor reviewer may decide. A senior moderator cannot self-approve. Rejection, requested changes/evidence and escalation require reasons.
5. Approval records a decision and sends the unchanged reviewed payload. It does **not** publish a card or invent `PUBLISHED`.
6. A committed Submission status confirms the outcome; rejected/stale commands remain visible. A newer snapshot invalidates old assignments/commands instead of authorizing a stale decision.

Queue/detail/history, claim/decision/escalation commands and report creation/triage are described by the service OpenAPI. Successful idempotent replays return the original command; reused keys with different input conflict. Optimistic local ETags and the authoritative Submission row version are distinct.

Risk scores must come from a genuine assessment, with explanations and a rules version. No synthetic zero-risk result or automatic approval is substituted when Risk is absent. Moderator corrections use Submission's existing immutable-version API; Moderation never rewrites a reviewed snapshot.

Reports do not automatically hide content. Report triage and re-review are Moderation-owned; actual visibility, suspension, archival and publication are **Catalog-owned**. Its existing workflow schema allows only Catalog to emit suspension/archive confirmations. This repository does not forge that producer or add an unconsumed fake Catalog command.

### Media workflow

Authenticated users upload a multipart `file` to `POST /api/v1/media-assets` with an idempotency key. Generated object keys contain no user filename. Quarantined bytes stay private. The durable worker validates PNG/JPEG extension, declared MIME and actual format; checks header dimensions/pixel bounds **before decode**; decodes and re-encodes into a fresh PNG, removing original metadata/EXIF and trailing private bytes.

`QUARANTINED → PROCESSING → CLEAN|REJECTED`. A dependency outage is retryable work, not a clean result or an invalid-file rejection. Only clean derivatives are previewed, with owner/reviewer authorization, `image/png`, `nosniff` and private cache controls. Original proofs are never public. The service-token route exposes only the metadata Submission needs.

**CLEAN means this codec/format pipeline passed, not an antivirus certificate.** No antivirus or automated image-content classifier is fabricated. Resource limits must still be enforced at the gateway as defense in depth.

## Configuration

Required nonlocal settings have no development credential fallback. The checked-in `application-local.yaml` files are the local examples. Standard Spring datasource, Kafka security and JWT settings remain configurable through Spring.

| Service | Required environment |
|---|---|
| Moderation | `SD_MODERATION_DB_URL`, `SD_MODERATION_DB_USER`, `SD_MODERATION_DB_PASSWORD`, `SD_MODERATION_KAFKA_BOOTSTRAP_SERVERS`, `SD_MODERATION_AUTH_ISSUER`, `SD_MODERATION_AUTH_JWK_SET_URI` |
| Media | `SD_MEDIA_DB_URL`, `SD_MEDIA_DB_USER`, `SD_MEDIA_DB_PASSWORD`, `SD_MEDIA_KAFKA_BOOTSTRAP_SERVERS`, `SD_MEDIA_AUTH_ISSUER`, `SD_MEDIA_AUTH_JWK_SET_URI`, `SD_MEDIA_SERVICE_TOKEN` |
| Media S3 | `SD_MEDIA_S3_ENDPOINT`, `SD_MEDIA_S3_ACCESS_KEY`, `SD_MEDIA_S3_SECRET_KEY`; `SD_MEDIA_S3_REGION` defaults to `us-east-1`, `SD_MEDIA_S3_BUCKET` to `media-private` |

`SD_MEDIA_S3_CREATE_BUCKET=true` is for local provisioning only. Production should provision a private bucket and least-privilege application credentials externally. Use TLS outside local development, rotate the service token in both Media and Submission together, and prevent public gateway access to `/media/v1/**`.

Media checks bucket privacy at startup. Its credentials need bucket existence/ACL/policy reads and access to the service-owned `quarantine/` and `clean/` prefixes. Do not use a bucket policy or public ACL for this bucket. Keep the S3 endpoint and internal service route off public ingress; expose only the authenticated `/api/v1/media-assets/**` API.

JWT verification uses Auth's JWKS, RS256, issuer, audience `student-discounts-api`, required expiry, canonical UUID subject and typed roles. An unknown role grants no permissions. No identity headers, fallback signing keys, cookie sessions or Auth database access.

Both services expose redacted health/readiness, protected metrics/Prometheus and correlation IDs. Request body, per-actor rate, worker batch and bounded processing limits prevent unbounded work; see each service's configuration for exact settings.

## Verification

```bash
gradle build --console=plain
docker compose config --quiet
```

Build includes unit tests and real PostgreSQL/S3/Kafka integration tests through Testcontainers. Docker is required; tests do not silently skip containers or substitute H2. GitHub Actions runs the installed Gradle binary with Java 25.

Run `gradle javaFormat` to format both Java modules. `build` checks the same pinned formatter.

`scripts/smoke.py` exercises **running real Auth, Media, Submission and Moderation**, stopping at confirmed `APPROVED`. It never installs mocks, grants roles, signs tokens, publishes Catalog data or resets databases. Provide dedicated existing accounts with `SD_SMOKE_STUDENT_EMAIL/PASSWORD`, `SD_SMOKE_OTHER_EMAIL/PASSWORD`, `SD_SMOKE_MODERATOR_EMAIL/PASSWORD`. The moderator account must already have the reviewer role:

```bash
# Export credentials securely first; do not commit them.
python3 scripts/smoke.py
```

Optional URLs: `SD_SMOKE_AUTH_URL`, `SD_SMOKE_MEDIA_URL`, `SD_SMOKE_SUBMISSION_URL`, `SD_SMOKE_MODERATION_URL`. Python 3's standard library suffices. Smoke logs out its own sessions but leaves its immutable approved submission/media records for audit; run it only against a dedicated environment. Catalog/Geo dependencies of Submission must be provided independently; using explicit development fixtures does not prove those services.

TDD red runs were observed before implementation for self-review/assignment/confirmation/concurrency/reason rules, image sanitization/spoofing/corruption/pixel bounds, and Submission's service-token forwarding/isolation.

Verified with Java 25 and installed Gradle 9.7.1: `gradle :moderation-service:javaFormat build --console=plain` passed **20 Moderation and 19 Media tests**, with zero failures or skips; `docker compose config --quiet` passed. Submission's targeted `BusinessClientsTest` and `SubmissionPropertiesTest` passed (36 tests). These checks do not establish production SLOs.

The packaged services both returned readiness `UP` and served their OpenAPI documents. The real-auth smoke verified cross-owner denial, upload/claim replay, stale ETag and stale immutable version rejection, correction/reclaim and confirmed approval. Submission's existing Catalog/Geo lookup fixtures were used; no Catalog application ran and no card was published.

## Operations and recovery

Back up each PostgreSQL database **and** private S3 bucket/version history. Preserve inbox/outbox/idempotency/audit together. Restore into an empty isolated target and align Kafka offsets/retention before resuming consumers. RPO/RTO and load targets from the design require deployment-specific restore/load exercises; they are not established by this implementation.

```bash
mkdir -p backups
docker compose exec -T moderation-db pg_dump -U moderation -Fc moderation > backups/moderation.dump
docker compose exec -T media-db pg_dump -U media -Fc media > backups/media.dump
```

Keep backup files private and encrypted. Take a coordinated S3 backup of both object prefixes. A failure between an acknowledged object write and metadata commit can leave an unreferenced private object; reconcile it against a consistent metadata snapshot before removal. Never apply a blanket quarantine expiry while live assets still reference those objects.

`docker compose down` preserves named volumes; do not use `down -v` as recovery. Never alter immutable audit/version rows to resolve a conflict. Preserve original event identity during replay. Inspect rejected commands/DLT privately, fix the cause, then retry through the supported workflow. Credentials, JWTs, private snapshots and proof bytes must not enter ordinary logs.

No automatic deletion of retained immutable decisions/proofs is scheduled: deployment must define the legal retention policy, coordinated with Submission references and protected backups, before accepting personal data in production.
