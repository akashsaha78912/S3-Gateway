package com.mediator.s3gateway.repository;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class MediatorObjectRepository {

    private final JdbcTemplate jdbc;
    private static final Logger log
            = LoggerFactory.getLogger(MediatorObjectRepository.class);

    public MediatorObjectRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

   //head object database call 
public Optional<DatabaseObject> find(
        String category,
        String objectName
) {
    String sql = """
            SELECT
                o.am_objectID,
                o.am_object_name,
                o.am_object_category,
                COALESCE((
                   SELECT SUM(oc.am_objectComponentSize)
                 FROM am_objectcomponents oc
                 WHERE oc.am_objectComponentOID = o.am_objectID
                    ), 0) AS content_length,
                o.am_ObjectChecksum,
                o.am_archiveDate,
                o.am_objectComments,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM am_objectinstances i
                    WHERE i.am_instanceOID = o.am_objectID
                    AND UPPER(TRIM(i.am_instanceType)) IN (
                        'NLD',
                        'NLD_DISK',
                        'GCS',
                        'S3',
                        'ALTO',
                        'AZURE',
                        'VAST'
                    )
                )
                THEN 'STANDARD'

                            ELSE 'DEEP_ARCHIVE'
                END AS storage_class,

                CASE
                    /* If an online instance exists, restore is already complete. */
                    WHEN EXISTS (
                        SELECT 1
                        FROM am_objectinstances i
                        WHERE i.am_instanceOID = o.am_objectID
                        AND UPPER(TRIM(i.am_instanceType)) IN (
                            'NLD',
                            'NLD_DISK',
                            'GCS',
                            'S3',
                            'ALTO',
                            'AZURE',
                            'VAST'
                        )
                    )
                    THEN FALSE

         /* Otherwise check for an active Manager COPY request. */
                    WHEN EXISTS (
                        SELECT 1
                        FROM am_requests r
                        WHERE r.am_req_object_name = o.am_object_name
                        AND r.am_req_category = o.am_object_category
                        AND r.am_req_type = 5
                        AND r.am_req_status IN (110, 12)
                    )
                    THEN TRUE

                    ELSE FALSE
                END AS restore_ongoing

                FROM am_objects o
            WHERE o.am_object_category = ?
              AND o.am_object_name = ?
              AND o.objMarkedDeleted = 0
            LIMIT 1
            """;

    String normalizedKey = objectName.startsWith("/")
            ? objectName.substring(1)
            : objectName;
              int lastSlash = normalizedKey.lastIndexOf('/');
        if (lastSlash >= 0) {
            objectName = normalizedKey.substring(0, lastSlash);
        } else {
            objectName = normalizedKey;
        }

    log.info(
            "DB_CALL FindObject category={} objectName={}",
            category,
            objectName
    );

    long startedAt = System.nanoTime();

    List<DatabaseObject> results = jdbc.query(
            sql,
            (rs, rowNum) -> new DatabaseObject(
                    rs.getLong("am_objectID"),
                    rs.getString("am_object_name"),
                    rs.getString("am_object_category"),
                    rs.getLong("content_length"),
                    rs.getString("am_ObjectChecksum"),
                    rs.getString("am_archiveDate"),
                    rs.getString("am_objectComments"),
                    rs.getString("storage_class"),
                    rs.getBoolean("restore_ongoing")
            ),
            category,
            objectName
    );

    long elapsedMs =
            (System.nanoTime() - startedAt) / 1_000_000;

    log.info(
            "DB_RESULT FindObject category={} objectName={} rows={} elapsedMs={}",
            category,
            objectName,
            results.size(),
            elapsedMs
    );

    return results.stream().findFirst();
}
    //Database Query to get the list of objects in a category
    public List<ListedObject> listPage(
            String category,
            String prefix,
            String afterKey,
            int fetchSize
    ) {
        String normalizedPrefix = prefix == null ? "" : prefix;
        String normalizedAfter = afterKey == null ? "" : afterKey;
        int safeFetchSize = Math.max(1, Math.min(fetchSize, 101));
        String sql = """
            SELECT 
                o.am_objectID,
                o.am_object_name,
                o.am_object_category,
                COALESCE((
            SELECT SUM(oc.am_objectComponentSize)
              FROM am_objectcomponents oc
                WHERE oc.am_objectComponentOID = o.am_objectID
                ), 0) AS content_length,
                o.am_objectChecksum,
                o.am_archiveDate,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM am_objectinstances i
                    WHERE i.am_instanceOID = o.am_objectID
                    AND UPPER(TRIM(i.am_instanceType)) IN (
                        'NLD',
                        'NLD_DISK',
                        'GCS',
                        'S3',
                        'ALTO',
                        'AZURE',
                        'VAST'
                    )
                )
                THEN 'STANDARD'

                ELSE 'DEEP_ARCHIVE'
            END AS storage_class
               FROM am_objects o
               WHERE o.am_object_category=?
                    AND o.objMarkedDeleted=0
                    AND o.am_object_name LIKE ? ESCAPE '\\\\'
                    AND o.am_object_name > ?
                ORDER BY o.am_object_name ASC
                LIMIT ? 
            """;
        log.info(
                "DB_CALL ListObjectsV2 category={} prefix={} afterKey={} fetchSize={}",
                category,
                normalizedPrefix,
                normalizedAfter,
                safeFetchSize
        );

        long startedAt = System.nanoTime();
        List<ListedObject> results = jdbc.query(
                sql,
                (rs, rowNum) -> new ListedObject(
                        rs.getLong("am_objectID"),
                        rs.getString("am_object_name"),
                        rs.getString("am_object_category"),
                        rs.getLong("content_length"),
                        rs.getString("am_objectChecksum"),
                        rs.getTimestamp("am_archiveDate"),
                        rs.getString("storage_class")
                ),
                category,
                escapeLike(normalizedPrefix) + "%",
                normalizedAfter,
                safeFetchSize
        );
        long elapsedMs
                = (System.nanoTime() - startedAt) / 1_000_000;

        log.info(
                "DB_RESULT ListObjectsV2 category={} rows={} elapsedMs={}",
                category,
                results.size(),
                elapsedMs
        );
        return results;
    }

    private static String escapeLike(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_");
    }
//SQL query to update the comments of an object in the database 
public int updateComments(
        String category,
        String objectName,
        String comments
) {
    String normalizedKey = objectName.startsWith("/")
            ? objectName.substring(1)
            : objectName;

    String sql = """
            UPDATE am_objects
            SET am_objectComments = ?
            WHERE am_object_category = ?
              AND am_object_name = ?
              AND objMarkedDeleted = 0
            """;

    return jdbc.update(
            sql,
            comments,
            category,
            normalizedKey
    );
}
    public record DatabaseObject(
            long objectId,
            String objectName,
            String category,
            long contentLength,
            String checksum,
            String archiveDate,
            String comments,
            String storageClass,
            boolean restoreOngoing
            ) {

      
    }

    public record ListedObject(
            long objectId,
            String key,
            String category,
            long size,
            String checksum,
            Timestamp lastModified,
            String storageClass
            ) {

    }

}
