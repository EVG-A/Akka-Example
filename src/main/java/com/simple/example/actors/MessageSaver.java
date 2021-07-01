/*
 * Copyright 2019 Russian Post
 *
 * This source code is Russian Post Confidential Proprietary.
 * This software is protected by copyright. All rights and titles are reserved.
 * You shall not use, copy, distribute, modify, decompile, disassemble or reverse engineer the software.
 * Otherwise this violation would be treated by law and would be subject to legal prosecution.
 * Legal use of the software provides receipt of a license from the right holder only.
 */

package com.simple.example.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import com.simple.example.dao.MessageDao;
import com.simple.example.messages.Done;
import com.simple.example.messages.MessageWrapper;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Saves Message to PostgreSQL
 *
 * @author Afanasev E.V.
 * @version 1.0 5/5/2021
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class MessageSaver extends AbstractActor {

    /**
     * Saves в PostgreSQL Message
     */
    private final MessageDao messageDao;

    private static final Logger LOG = LoggerFactory.getLogger(MessageSaver.class);
    /**
     * Factory method for creating information needed for Actor construction.
     *
     * @param messageDao Saves Message to PostgreSQL
     * @return Props object for Actor construction by Akka
     */
    public static Props props(final MessageDao messageDao) {
        return Props.create(MessageSaver.class, () -> new MessageSaver(messageDao));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(MessageWrapper.class, this::processMessage)
            .matchAny(m -> LOG.warn("Unknown message: {}", m))
            .build();
    }

    private void processMessage(final MessageWrapper messageWrapper) {
        try {
            messageDao.saveMessage(messageWrapper);
            sender().tell(new Done(messageWrapper.getBatchId()), self());
        } catch (SQLException e) {
            LOG.error(e.getMessage());
        }
    }

}
