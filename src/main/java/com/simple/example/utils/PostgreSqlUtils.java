/*
 * Copyright 2019 Russian Post
 *
 * This source code is Russian Post Confidential Proprietary.
 * This software is protected by copyright. All rights and titles are reserved.
 * You shall not use, copy, distribute, modify, decompile, disassemble or reverse engineer the software.
 * Otherwise this violation would be treated by law and would be subject to legal prosecution.
 * Legal use of the software provides receipt of a license from the right holder only.
 */

package com.simple.example.utils;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Class responsible for initializing a DataSource for a PostgreSQL database.
 *
 * @author Afanasev E.V.
 * @version 1.0 4/15/2021
 */
public final class PostgreSqlUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlUtils.class);

    /**
     * hidden default constructor.
     */
    private PostgreSqlUtils() {
    }

    /**
     * Initializes and returns DataSource for the given database URL and credentials.
     *
     * @param url                 a database url of the form jdbc:subProtocol:subName
     * @param user                the database user on whose behalf the connection is being made
     * @param password            the user's password
     * @param queryTimeoutSeconds will be used as default and validation query timeout, if not NULL and bigger than 0.
     *                            Otherwise default values will be used.
     * @return created DataSource.
     */
    public static BasicDataSource initializeDataSource(
        final String url, final String user, final String password, final Integer queryTimeoutSeconds
    ) {
        LOG.info("Initializing PostgreSQL data source: url = \"{}\", user = \"{}\"", url, user);
        final BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        dataSource.setMaxTotal(20);
        dataSource.setMaxIdle(20);
        if (queryTimeoutSeconds != null && queryTimeoutSeconds > 0) {
            dataSource.setDefaultQueryTimeout(queryTimeoutSeconds);
            dataSource.setValidationQueryTimeout(queryTimeoutSeconds);
        }
        LOG.info("PostgreSQL data source has been successfully initialized.");
        return dataSource;
    }

    /**
     * Attempts to close the database connection and clears the client, so it can be initialized again lately.
     *
     * @param dataSource data source to terminate.
     */
    public static void terminate(BasicDataSource dataSource) {

        if (dataSource != null) {
            try {
                dataSource.close();
                LOG.info("Successfully closed the PostgreSQL data source.");
            } catch (SQLException e) {
                LOG.warn("Failed to close the PostgreSQL data source.", e);
            }
        } else {
            throw new IllegalStateException("Data source has not been initialized yet. There are data source to close.");
        }
    }
}
