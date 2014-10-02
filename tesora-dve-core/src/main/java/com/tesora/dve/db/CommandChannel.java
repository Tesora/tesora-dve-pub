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
import com.tesora.dve.db.mysql.MysqlCommand;
import com.tesora.dve.db.mysql.MysqlCommandResultsProcessor;
import com.tesora.dve.db.mysql.libmy.MyMessage;

/**
 *
 */
public interface CommandChannel {
    String getName();
    StorageSite getStorageSite();
    DBConnection.Monitor getMonitor();

    boolean isOpen();
    void write(MysqlCommand command);
    void writeAndFlush(MysqlCommand command);
    void write(MyMessage outboundMessage, MysqlCommandResultsProcessor resultsProcessor);
    void writeAndFlush(MyMessage outboundMessage, MysqlCommandResultsProcessor resultsProcessor);

    CompletionHandle<Boolean> getExceptionDeferringPromise();
    Exception getAndClearPendingException();
}
