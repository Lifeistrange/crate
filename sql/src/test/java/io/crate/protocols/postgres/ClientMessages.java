/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.protocols.postgres;

import org.jboss.netty.buffer.ChannelBuffer;

import java.nio.charset.StandardCharsets;

/**
 * Class to encode postgres client messages and write them onto a buffer.
 *
 * For more information about the messages see {@link Messages} and {@link ConnectionContext} or refer to
 * the PostgreSQL protocol documentation.
 */
class ClientMessages {

    static void sendStartupMessage(ChannelBuffer buffer, String dbName) {
        byte[] dbKey = "database".getBytes(StandardCharsets.UTF_8);
        byte[] dbValue = dbName.getBytes(StandardCharsets.UTF_8);
        buffer.writeInt(8 + dbKey.length + 1 + dbValue.length + 1);
        buffer.writeInt(3);
        buffer.writeBytes(dbKey);
        buffer.writeByte(0);
        buffer.writeBytes(dbValue);
        buffer.writeByte(0);
    }

    static void sendParseMessage(ChannelBuffer buffer, String stmtName, String query, int[] paramOids) {
        buffer.writeByte('P');
        byte[] stmtNameBytes = stmtName.getBytes(StandardCharsets.UTF_8);
        byte[] queryBytes = query.getBytes(StandardCharsets.UTF_8);
        buffer.writeInt(
            4 +
            stmtNameBytes.length + 1 +
            queryBytes.length + 1 +
            2 +
            paramOids.length * 4);
        writeCString(buffer, stmtNameBytes);
        writeCString(buffer, queryBytes);
        buffer.writeShort(paramOids.length);
        for (int paramOid : paramOids) {
            buffer.writeInt(paramOid);
        }
    }

    private static void writeCString(ChannelBuffer buffer, byte[] bytes) {
        buffer.writeBytes(bytes);
        buffer.writeByte(0);
    }

    static void sendFlush(ChannelBuffer buffer) {
        buffer.writeByte('H');
        buffer.writeInt(4);
    }
}