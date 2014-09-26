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

import java.util.List;

import com.tesora.dve.db.mysql.MysqlMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPComStmtCloseRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MysqlGroupedPreparedStatementId;
import com.tesora.dve.db.mysql.MysqlStmtCloseCommand;
import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;

public class MysqlStmtCloseDiscarder extends DBResultConsumer  {
	
	final MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt;

	public MysqlStmtCloseDiscarder(
			MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt) {
		super();
		this.pstmt = pstmt;
	}

    @Override
    public Bundle getDispatchBundle(CommandChannel channel, SQLCommand sql, CompletionHandle<Boolean> promise) {
        int preparedID = (int)pstmt.getStmtId().getStmtId(channel.getPhysicalID());
        MysqlMessage message = MSPComStmtCloseRequestMessage.newMessage(preparedID);
        return new Bundle(message, new MysqlStmtCloseCommand(preparedID, promise) );
	}

}
