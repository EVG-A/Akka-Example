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
import com.simple.example.messages.MessageWrapper;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The class writes the headers of Message to the database (in the table message)
 *
 * @author Afanasev E.V.
 * @version 1.0 5/4/2021
 */
public class MessageDao {
    /**
     * Inserts a record into the table. message
     */
    private static final String INSERT_MESSAGE_SQL = "INSERT INTO message " +
        "(type, version, release, controlling_agency, assigned_code, mes_ref, file_id) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?)";

    private static final Logger LOG = LoggerFactory.getLogger(MessageDao.class);
    private final BasicDataSource dataSource;

    /**
     * non default constructor.
     *
     * @param dataSource .
     */
    public MessageDao(BasicDataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Saves Message - in table.
     *
     * @param messageWrapper Wrapper for Message with additional data
     * @return long saved Message Id
     * @throws SQLException db saving error
     */
    public long saveMessage(final MessageWrapper messageWrapper) throws SQLException {
        LOG.debug("Storing data");
        long messageId = 0;
        try (
            final Connection connection = dataSource.getConnection();
            final PreparedStatement statement = connection.prepareStatement(
                INSERT_MESSAGE_SQL, Statement.RETURN_GENERATED_KEYS)
        ) {
            ConsumerData.Message message = messageWrapper.getMessage();
            statement.setString(1, message.getType());
            statement.setString(2, message.getVersion());
            statement.setString(3, message.getRelease());
            statement.setString(4, message.getControllingAgency());
            statement.setString(5, message.getAssignedCode());
            statement.setString(6, message.getMesRef());
            statement.setLong(7, messageWrapper.getFileID());
            statement.execute();
            ResultSet generatedKeys = statement.getGeneratedKeys();
            if (generatedKeys.next()) {
                messageId = generatedKeys.getLong(1);
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage());
            throw e;
        }
        return messageId;
    }
}
