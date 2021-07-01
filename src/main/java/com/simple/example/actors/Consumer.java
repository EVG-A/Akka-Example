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
import com.google.protobuf.InvalidProtocolBufferException;
import com.simple.example.messages.ConsumerData;
import com.simple.example.messages.Done;
import com.simple.example.messages.FileWrapper;
import com.simple.example.utils.KafkaBatch;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * Receives and processes messages from Kafka
 * after which store data to the PostgreSQL database.
 *
 * @author Afanasev E.V.
 * @version 1.0 5/3/2021
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class Consumer extends AbstractActor {

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
    /**
     * Saves в PostgreSQL files
     */
    private final ActorRef fileSaver;

    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private String currentBatchId;
    private int currentBatchSize;
    private int amountDone = 0;

    /**
     * Factory method for creating information needed for Actor construction.
     *
     * @param fileSaver Saves в PostgreSQL data
     * @return Props object for Actor construction by Akka
     */
    public static Props props(final ActorRef fileSaver) {
        return Props.create(Consumer.class,
            () -> new Consumer(fileSaver));
    }

    @Override
    public void preStart() throws Exception {
        init();
        askForNewData();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(KafkaBatch.class, this::processKafkaBatch)
            .match(Done.class, m -> handleDone(m.getId()))
            .matchAny(m -> LOG.warn("Unknown message: {}", m))
            .build();
    }

    /**
     * Process message from kafka.
     *
     * @param kafkaBatch byte array with message from kafka
     */
    private void processKafkaBatch(final KafkaBatch kafkaBatch) {
        currentBatchId = UUID.randomUUID().toString();
        final List<FileWrapper> records = new LinkedList<>();
        for (ConsumerRecord<byte[], byte[]> data : kafkaBatch.getEvents()) {
            try {
                final ConsumerData.File file =
                    ConsumerData.File.parseFrom(data.value());
                records.add(new FileWrapper(currentBatchId, file));
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Unknown data in Kafka message: {}", data.value());
            }
        }
        if (records.isEmpty()) {
            commitAndAskForNewData();
        } else {
            amountDone = 0;
            currentBatchSize = records.size();
            records.forEach(file -> fileSaver.tell(file, self()));
        }
    }

    public void handleDone(String batchId) {
        if (Objects.equals(batchId, currentBatchId)) {
            amountDone++;
            if (amountDone == currentBatchSize) {
                commitAndAskForNewData();
            }
        }
    }

    /**
     * Asks Kafka for new batch of data
     */
    private void askForNewData() {
        ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(java.time.Duration.of(5, ChronoUnit.SECONDS));
        List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>(consumerRecords.count());
        final Set<TopicPartition> partitions = consumerRecords.partitions();
        for (TopicPartition partition : partitions) {
            final List<ConsumerRecord<byte[], byte[]>> records = consumerRecords.records(partition);
            batch.addAll(records);
        }
        self().tell(new KafkaBatch(batch), self());
    }

    private void commitAndAskForNewData() {
        kafkaConsumer.commitSync();
        askForNewData();
    }

    private void init() {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
            kafkaConsumer = null;
        }
        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "test-group");
        props.put("login", "login");
        props.put("password", "password");
        props.put("poll.timeout", "300");
        props.put("retry.time", "1000");
        props.put("topic", "simple-example-topic");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("bootstrap.servers","localhost:1001");
        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList(props.getProperty("topic")));
    }
}

