/*
 * Copyright 2019 Russian Post
 *
 * This source code is Russian Post Confidential Proprietary.
 * This software is protected by copyright. All rights and titles are reserved.
 * You shall not use, copy, distribute, modify, decompile, disassemble or reverse engineer the software.
 * Otherwise this violation would be treated by law and would be subject to legal prosecution.
 * Legal use of the software provides receipt of a license from the right holder only.
 */

package com.simple.example.dao;


import com.simple.example.messages.ConsumerData;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * The class writes file to the database (in the table FILE)
 *
 * @author Afanasev E.V.
 * @version 1.0 5/3/2021
 */
public class FileDao {

    /**
     * Inserts a record into the table. FILE
     */
    private static final String INSERT_FILE_SQL = "INSERT INTO file " +
        "(source_system_id, source_topic, topic_partition, topic_offset, file_timestamp, file_name, sender_mailbox," +
        " recipient_mailbox, way, country_a2_from, country_a2_to, int_ref, protocol, protocol_version, postal_item_type) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final Logger LOG = LoggerFactory.getLogger(FileDao.class);
    private final BasicDataSource dataSource;

    /**
     * non default constructor.
     *
     * @param dataSource .
     */
    public FileDao(BasicDataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Saves monitoring data in table FILE
     *
     * @param file Monitoring File
     * @return long saved File Id
     * @throws SQLException db saving error
     */
    public long saveFile(ConsumerData.File file) throws SQLException {
        LOG.debug("Storing data");
        long fileId = 0;
        try (
            final Connection connection = dataSource.getConnection();
            final PreparedStatement statement = connection.prepareStatement(
                INSERT_FILE_SQL, Statement.RETURN_GENERATED_KEYS)
        ) {
            statement.setString(1, file.getSourceSystemId());
            statement.setString(2, file.getSourceTopic());
            statement.setInt(3, file.getTopicPartition());
            statement.setLong(4, file.getTopicOffset());
            statement.setLong(5, file.getRecordTimestamp());
            statement.setString(6, file.getFileName());
            statement.setString(7, file.getSenderMailbox());
            statement.setString(8, file.getRecipientMailbox());
            statement.setString(9, file.getWay().toString());
            statement.setString(10, file.getCountryA2From());
            statement.setString(11, file.getCountryA2To());
            statement.setString(12, file.getIntRef());
            statement.setString(13, file.getProtocol());
            statement.setString(14, file.getProtocolVersion());
            statement.setString(15, null);
            statement.execute();
            ResultSet generatedKeys = statement.getGeneratedKeys();
            if (generatedKeys.next()) {
                fileId = generatedKeys.getLong(1);
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage());
            throw e;
        }
        return fileId;
    }
}
