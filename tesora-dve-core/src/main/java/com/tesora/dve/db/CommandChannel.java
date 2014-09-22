package com.tesora.dve.db;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.db.mysql.*;
import com.tesora.dve.db.mysql.libmy.MyMessage;

import java.nio.charset.Charset;
import java.util.UUID;

/**
 *
 */
public interface CommandChannel {
    String getName();
    UUID getPhysicalID();
    Charset getTargetCharset();
    StorageSite getStorageSite();
    DBConnection.Monitor getMonitor();

    boolean isOpen();
    boolean isWritable();
    void write(MysqlCommandBundle command);
    void writeAndFlush(MysqlCommandBundle command);
    void write(MysqlMessage outboundMessage, MysqlCommandResultsProcessor resultsProcessor);
    void writeAndFlush(MysqlMessage outboundMessage, MysqlCommandResultsProcessor resultsProcessor);

    CompletionHandle<Boolean> getExceptionDeferringPromise();
    Exception getAndClearPendingException();


}
