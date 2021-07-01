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
import akka.routing.FromConfig;
import com.simple.example.dao.FileDao;
import com.simple.example.dao.MessageDao;
import com.simple.example.utils.PostgreSqlUtils;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Afanasev E.V.
 * @version 1.0 4/15/2021
 */
@Slf4j
public final class ApplicationSupervisor extends AbstractActor {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationSupervisor.class);
    private BasicDataSource dataSource;

    /**
     * Initialize actor.
     *
     * @return Props .
     */
    public static Props props() {
        return Props.create(ApplicationSupervisor.class, ApplicationSupervisor::new);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .matchAny(m -> LOG.warn("Unknown message: {}", m))
            .build();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        final Config config = context().system().settings().config();
        this.dataSource = preparePostgreSqlDataSource(config);
        /* Init actor hierarchy. */
        initActors();
    }

    /**
     * Initializes PostgreSql DB dataSource.
     *
     * @param config System configuration.
     * @return .
     */
    private static BasicDataSource preparePostgreSqlDataSource(Config config) {
        return PostgreSqlUtils.initializeDataSource(
            config.getString("postgresql-connection.jdbc.url"),
            config.getString("postgresql-connection.user"),
            config.getString("postgresql-connection.password"),
            config.getInt("postgresql-connection.query-timeout-seconds")
        );
    }

    private void initActors() {
        final FileDao fileDao =
            new FileDao(this.dataSource);
        final MessageDao messageDao =
            new MessageDao(this.dataSource);

        ActorRef messageSaver = context().actorOf(
            FromConfig.getInstance().props(MessageSaver.props(
                messageDao
            )),
            "messageSaver"
        );
        ActorRef fileSaver = context().actorOf(
            FromConfig.getInstance().props(FileSaver.props(
                messageSaver,
                fileDao
            )),
            "fileSaver"
        );

        context().actorOf(Consumer.props(fileSaver), "consumer");
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        PostgreSqlUtils.terminate(this.dataSource);
        LOG.info("ApplicationSupervisor stopped");
    }
}
