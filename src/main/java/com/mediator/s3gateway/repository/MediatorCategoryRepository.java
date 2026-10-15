package com.mediator.s3gateway.repository;

import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.dao.DuplicateKeyException;

@Repository
public class MediatorCategoryRepository {

    private final JdbcTemplate jdbc;

    public MediatorCategoryRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<String> findAll(String prefix, String afterBucket, int limit) {
        String bucketPrefix = prefix == null ? "" : prefix;
        if (afterBucket == null || afterBucket.isBlank()) {
            return jdbc.queryForList("""
                    SELECT category_name
                    FROM am_categories
                    WHERE category_name LIKE ?
                    ORDER BY category_name
                    LIMIT ?
                    """,
                    String.class,
                    bucketPrefix + "%",
                    limit);
        }
        return jdbc.queryForList("""
                   SELECT category_name
                   FROM am_categories
                   WHERE category_name LIKE ?
                     AND category_name > ?
                   ORDER BY category_name
                   LIMIT ?
                """,
                String.class,
                bucketPrefix + "%",
                afterBucket,
                limit);
    }

    public void ensureExists(String category) {
        try {
            jdbc.update("""
                    INSERT INTO am_categories (category_name)
                    VALUES (?)
                    """, category);
        } catch (DuplicateKeyException ignored) {
            // Category already exists: leave the existing row unchanged.
        }
    }

    public boolean exists(String category) {
        Boolean exists = jdbc.queryForObject("""
                SELECT EXISTS(
                    SELECT 1
                    FROM am_categories
                    WHERE category_name = ?
                )
                """,
                Boolean.class,
                category);

        return Boolean.TRUE.equals(exists);
    }

}