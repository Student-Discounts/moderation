package org.studentdiscounts.media;

import com.fasterxml.uuid.Generators;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

@Service
public class MediaService {
  private static final com.fasterxml.uuid.impl.TimeBasedEpochGenerator IDS =
      Generators.timeBasedEpochGenerator();

  public record Metadata(
      UUID id,
      UUID ownerId,
      String status,
      long revision,
      Instant createdAt,
      Integer width,
      Integer height,
      String rejectionCode) {}

  public record ConsumerMetadata(UUID id, UUID ownerId, String status, long revision) {}

  public record UploadReceipt(
      UUID id, UUID ownerId, String status, long revision, Instant createdAt) {}

  record Asset(
      UUID id,
      UUID ownerId,
      String filename,
      String mime,
      String originalKey,
      String cleanKey,
      String status,
      long revision,
      Instant createdAt,
      Integer width,
      Integer height,
      String rejectionCode,
      String correlationId,
      UUID claimId,
      int attempts,
      String rawSha256,
      int rawBytes) {
    Metadata metadata() {
      return new Metadata(id, ownerId, status, revision, createdAt, width, height, rejectionCode);
    }
  }

  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;
  private final PrivateStorage storage;
  private final MediaProperties properties;
  private final JsonMapper json;

  public MediaService(
      JdbcTemplate jdbc,
      TransactionTemplate tx,
      PrivateStorage storage,
      MediaProperties properties,
      JsonMapper json) {
    this.jdbc = jdbc;
    this.tx = tx;
    this.storage = storage;
    this.properties = properties;
    this.json = json;
  }

  static UUID newId() {
    return IDS.generate();
  }

  static String hash(byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  static String fingerprint(String rawSha256, int rawLength, String filename, String mime) {
    byte[] name = filename.getBytes(StandardCharsets.UTF_8),
        type = mime.getBytes(StandardCharsets.UTF_8);
    byte[] digest = rawSha256.getBytes(StandardCharsets.US_ASCII);
    return hash(
        ByteBuffer.allocate(12 + name.length + type.length + digest.length)
            .putInt(name.length)
            .put(name)
            .putInt(type.length)
            .put(type)
            .putInt(rawLength)
            .put(digest)
            .array());
  }

  public UploadReceipt upload(UUID owner, String key, String filename, String mime, byte[] bytes) {
    if (key == null || !key.matches("[A-Za-z0-9._:-]{1,128}"))
      throw new ApiFailure(HttpStatus.BAD_REQUEST, "IDEMPOTENCY_KEY_REQUIRED");
    if (bytes.length == 0 || bytes.length > properties.limits().maxBytes())
      throw new ApiFailure(HttpStatus.PAYLOAD_TOO_LARGE, "UPLOAD_SIZE_INVALID");
    if (filename == null
        || filename.isBlank()
        || filename.length() > 200
        || filename.contains("/")
        || filename.contains("\\")
        || filename.codePoints().anyMatch(Character::isISOControl))
      throw new ApiFailure(HttpStatus.UNPROCESSABLE_ENTITY, "INVALID_FILENAME");
    if (mime == null || !List.of("image/jpeg", "image/png").contains(mime))
      throw new ApiFailure(HttpStatus.UNSUPPORTED_MEDIA_TYPE, "UNSUPPORTED_IMAGE_TYPE");
    String rawSha256 = hash(bytes);
    String fingerprint = fingerprint(rawSha256, bytes.length, filename, mime);
    return tx.execute(transaction -> {
      // Serialize owner/key before touching S3; parallel replays never create duplicate objects or
      // records.
      jdbc.execute(
          (org.springframework.jdbc.core.ConnectionCallback<Void>)
              connection -> {
                try (var statement =
                    connection.prepareStatement(
                        "select pg_advisory_xact_lock(hashtextextended(?,0))")) {
                  statement.setString(1, owner + ":" + key);
                  statement.execute();
                }
                return null;
              });
      var prior =
          jdbc.query(
              "select fingerprint,asset_id from upload_idempotency where owner_id=? and key=?",
              (rs, n) -> Map.entry(rs.getString(1), rs.getObject(2, UUID.class)),
              owner,
              key);
      if (!prior.isEmpty()) {
        if (!fingerprint.equals(prior.getFirst().getKey()))
          throw new ApiFailure(HttpStatus.CONFLICT, "IDEMPOTENCY_KEY_REUSED");
        Asset asset = find(prior.getFirst().getValue());
        return new UploadReceipt(asset.id(), asset.ownerId(), "QUARANTINED", 0, asset.createdAt());
      }
      UUID id = newId();
      String object = "quarantine/" + id;
      String correlation = MDC.get("correlationId");
      if (correlation == null) correlation = newId().toString();
      // S3 acknowledgement precedes metadata commit. Failed/ambiguous DB commits may leave private
      // orphans,
      // never metadata pointing at an unacknowledged upload. Do not delete on an ambiguous commit
      // outcome.
      storage.put(object, bytes, "application/octet-stream");
      jdbc.update(
          """
          insert into media_asset(id,owner_id,filename,declared_mime,raw_bytes,raw_sha256,original_key,status,correlation_id)
          values(?,?,?,?,?,?,?,'QUARANTINED',?)
          """,
          id,
          owner,
          filename,
          mime,
          bytes.length,
          rawSha256,
          object,
          correlation);
      jdbc.update(
          "insert into upload_idempotency(owner_id,key,fingerprint,asset_id) values(?,?,?,?)",
          owner,
          key,
          fingerprint,
          id);
      Asset asset = find(id);
      auditAndEvent(asset, null, "UPLOADED", owner);
      return new UploadReceipt(id, owner, "QUARANTINED", 0, asset.createdAt());
    });
  }

  Asset find(UUID id) {
    List<Asset> rows = jdbc.query("select * from media_asset where id=?", MediaService::map, id);
    if (rows.isEmpty()) throw ApiFailure.notFound();
    return rows.getFirst();
  }

  private static Asset map(ResultSet rs, int ignored) throws SQLException {
    return new Asset(
        rs.getObject("id", UUID.class),
        rs.getObject("owner_id", UUID.class),
        rs.getString("filename"),
        rs.getString("declared_mime"),
        rs.getString("original_key"),
        rs.getString("clean_key"),
        rs.getString("status"),
        rs.getLong("revision"),
        rs.getTimestamp("created_at").toInstant(),
        (Integer) rs.getObject("width"),
        (Integer) rs.getObject("height"),
        rs.getString("rejection_code"),
        rs.getString("correlation_id"),
        rs.getObject("claim_id", UUID.class),
        rs.getInt("attempts"),
        rs.getString("raw_sha256"),
        rs.getInt("raw_bytes"));
  }

  Asset claim() {
    return tx.execute(transaction -> {
      List<Asset> rows =
          jdbc.query(
              """
              select * from media_asset where next_attempt_at<=now() and
              (status='QUARANTINED' or (status='PROCESSING' and lease_until<=now()))
              order by next_attempt_at,id limit 1 for update skip locked
              """,
              MediaService::map);
      if (rows.isEmpty()) return null;
      Asset old = rows.getFirst();
      UUID claim = newId();
      jdbc.update(
          """
          update media_asset set status='PROCESSING',revision=revision+case when status='QUARANTINED' then 1 else 0 end,
          claim_id=?,lease_until=now()+interval '120 seconds',attempts=attempts+1,updated_at=now() where id=?
          """,
          claim,
          old.id());
      Asset claimed = find(old.id());
      if ("QUARANTINED".equals(old.status()))
        auditAndEvent(claimed, old.status(), "CODEC_STARTED", null);
      return claimed;
    });
  }

  void complete(Asset claim, ImagePipeline.CleanImage clean, String cleanKey, String rejection) {
    tx.executeWithoutResult(transaction -> {
      jdbc.query(
          "select id from media_asset where id=? for update",
          (rs, n) -> rs.getObject(1, UUID.class),
          claim.id());
      Asset current = find(claim.id());
      if (!claim.claimId().equals(current.claimId()) || !"PROCESSING".equals(current.status()))
        return;
      jdbc.update(
          """
          update media_asset set status=?,revision=revision+1,clean_key=?,width=?,height=?,rejection_code=?,
          claim_id=null,lease_until=null,updated_at=now() where id=?
          """,
          rejection == null ? "CLEAN" : "REJECTED",
          cleanKey,
          clean == null ? null : clean.width(),
          clean == null ? null : clean.height(),
          rejection,
          claim.id());
      auditAndEvent(
          find(claim.id()), "PROCESSING", rejection == null ? "SAFE_REENCODED" : rejection, null);
    });
  }

  void retry(Asset claim) {
    // Infrastructure/codec availability failures never turn a file into CLEAN or a permanent
    // rejection.
    int delay = Math.min(300, 5 * (1 << Math.min(claim.attempts(), 6)));
    jdbc.update(
        """
        update media_asset set lease_until=now(),next_attempt_at=now()+(? * interval '1 second'),updated_at=now()
        where id=? and claim_id=? and status='PROCESSING'
        """,
        delay,
        claim.id(),
        claim.claimId());
  }

  private void auditAndEvent(Asset asset, String previous, String reason, UUID actor) {
    jdbc.update(
        """
        insert into media_audit(id,asset_id,actor_id,from_status,to_status,revision,reason,correlation_id)
        values(?,?,?,?,?,?,?,?)
        """,
        newId(),
        asset.id(),
        actor,
        previous,
        asset.status(),
        asset.revision(),
        reason,
        asset.correlationId());
    UUID event = newId();
    String payload =
        json.writeValueAsString(
            Map.of(
                "eventId",
                event,
                "eventType",
                "MediaAssetChecked",
                "schemaVersion",
                1,
                "producer",
                "media",
                "occurredAt",
                Instant.now().toString(),
                "correlationId",
                asset.correlationId(),
                "data",
                Map.of(
                    "mediaAssetId",
                    asset.id(),
                    "status",
                    asset.status(),
                    "revision",
                    asset.revision())));
    jdbc.update(
        "insert into media_outbox(event_id,asset_id,revision,payload) values(?,?,?,?::jsonb)",
        event,
        asset.id(),
        asset.revision(),
        payload);
  }
}
