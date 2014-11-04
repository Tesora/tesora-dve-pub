package com.tesora.dve.db.mysql;

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
import com.tesora.dve.db.CommandChannel;

import com.tesora.dve.db.mysql.portal.protocol.MSPComQueryRequestMessage;
import org.apache.log4j.Logger;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.messaging.SQLCommand;

public class MysqlKillResultDiscarder extends DBResultConsumer {

	static Logger logger = Logger.getLogger(MysqlKillResultDiscarder.class);

	public static final MysqlKillResultDiscarder INSTANCE = new MysqlKillResultDiscarder();

    @Override
    public Bundle getDispatchBundle(CommandChannel channel, SQLCommand sql, CompletionHandle<Boolean> promise) {
		if (logger.isDebugEnabled())
			logger.debug(promise + ", " + channel + " write " + sql.getRawSQL());
        MysqlMessage message = MSPComQueryRequestMessage.newMessage(sql.getBytes());
        return new Bundle(message, new MysqlExecuteCommand(sql, channel.getMonitor(), null, promise));
	}

}
