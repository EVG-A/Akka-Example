/*
 * Copyright 2019 Russian Post
 *
 * This source code is Russian Post Confidential Proprietary.
 * This software is protected by copyright. All rights and titles are reserved.
 * You shall not use, copy, distribute, modify, decompile, disassemble or reverse engineer the software.
 * Otherwise this violation would be treated by law and would be subject to legal prosecution.
 * Legal use of the software provides receipt of a license from the right holder only.
 */

package com.simple.example.messages;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Wrapper for File with additional data
 *
 * @author Afanasev E.V.
 * @version 1.0 5/4/2021
 */
@Data
@AllArgsConstructor
@Builder
public class FileWrapper {
    private final String batchId;
    private final ConsumerData.File file;
}
