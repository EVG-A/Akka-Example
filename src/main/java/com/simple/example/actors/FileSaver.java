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
import akka.actor.ActorRef;
import akka.actor.Props;
import com.simple.example.dao.FileDao;
import com.simple.example.messages.ConsumerData;
import com.simple.example.messages.Done;
import com.simple.example.messages.FileWrapper;
import com.simple.example.messages.MessageWrapper;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Saves File to PostgreSQL
 *
 * @author Afanasev E.V.
 * @version 1.0 5/3/2021
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class FileSaver extends AbstractActor {

    /**
     * Saves в PostgreSQL Messages
     */
    private final ActorRef messageSaver;

    /**
     * Saves в PostgreSQL File
     */
    private final FileDao fileDao;

    private static final Logger LOG = LoggerFactory.getLogger(FileSaver.class);
    /**
     * sender
     */
    private ActorRef sender;
    private String currentBatchId;
    private String receivedBatchId;
    private int currentBatchSize;
    private int amountDone = 0;

    /**
     * Factory method for creating information needed for Actor construction.
     *
     * @param messageSaver Saves in PostgreSQL Message
     * @param fileDao      Saves in PostgreSQL File
     * @return Props object for Actor construction by Akka
     */
    public static Props props(final ActorRef messageSaver, final FileDao fileDao) {
        return Props.create(FileSaver.class, () -> new FileSaver(messageSaver, fileDao));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(FileWrapper.class, this::processFile)
            .match(Done.class, m -> handleDone(m.getId()))
            .matchAny(m -> LOG.warn("Unknown message: {}", m))
            .build();
    }

    private void processFile(final FileWrapper fileWrapper) {
        sender = sender();
        receivedBatchId = fileWrapper.getBatchId();
        currentBatchId = UUID.randomUUID().toString();
        long fileId = 0;
        ConsumerData.File file = fileWrapper.getFile();
        final List<ConsumerData.Message> messagesList = file.getMessagesList();
        try {
            fileId = fileDao.saveFile(file);
        } catch (SQLException e) {
            LOG.error(e.getMessage());
        }
        if (messagesList.isEmpty()) {
            sender().tell(new Done(fileWrapper.getBatchId()), self());
        } else {
            amountDone = 0;
            currentBatchSize = messagesList.size();
            long finalFileId = fileId;
            messagesList.forEach(message ->
                messageSaver.tell(new MessageWrapper(
                    currentBatchId,
                    finalFileId,
                    message
                ), self())
            );
        }
    }

    public void handleDone(String batchId) {
        if (Objects.equals(batchId, currentBatchId)) {
            amountDone++;
            if (amountDone == currentBatchSize) {
                sender.tell(new Done(receivedBatchId), self());
            }
        }
    }
}
