package com.mediator.s3gateway.repository;

import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class MediatorCategoryRepository {

    private final JdbcTemplate jdbc;

    public MediatorCategoryRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<String> findAll() {
        String sql = """
                SELECT DISTINCT am_object_category
                FROM am_objects
                WHERE am_object_category IS NOT NULL
                  AND TRIM(am_object_category) <> ''
                ORDER BY am_object_category
                """;

        return jdbc.queryForList(sql, String.class);
    }
}