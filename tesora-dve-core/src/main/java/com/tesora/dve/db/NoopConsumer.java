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

import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.server.messaging.SQLCommand;

/**
 *
 */
public class NoopConsumer extends DBResultConsumer {
    public static final NoopConsumer SINGLETON = new NoopConsumer();
    @Override
    public Bundle getDispatchBundle(CommandChannel channel, SQLCommand sql, CompletionHandle<Boolean> promise) {
        //this command does nothing, but can be useful for making workergroup/workers set the current database and setting session parameters.
        promise.success(true);
        return Bundle.NO_COMM;
    }
}
