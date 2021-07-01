/*
 * Copyright 2020 Russian Post
 *
 * This source code is Russian Post Confidential Proprietary.
 * This software is protected by copyright. All rights and titles are reserved.
 * You shall not use, copy, distribute, modify, decompile, disassemble or reverse engineer the software.
 * Otherwise this violation would be treated by law and would be subject to legal prosecution.
 * Legal use of the software provides receipt of a license from the right holder only.
 */
package com.simple.example;

import akka.actor.ActorSystem;
import com.simple.example.actors.ApplicationSupervisor;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Afanasev Evgeni
 * @version 1.0 14.04.2021.
 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final String SERVICE_NAME = "simple-example";

    private Main() {
    }

    /**
     * Main entry point to application.
     *
     * @param args cmd args.
     */
    public static void main(String[] args) {
        LOG.info("Starting application");
        final ActorSystem system = ActorSystem.create(
            SERVICE_NAME, ConfigFactory.load().getConfig(SERVICE_NAME));
        system.actorOf(ApplicationSupervisor.props(), "applicationSupervisor");
    }

}
