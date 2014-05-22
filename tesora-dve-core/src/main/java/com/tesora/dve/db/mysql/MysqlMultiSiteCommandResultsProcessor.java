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

import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.exceptions.PEException;
import io.netty.channel.ChannelHandlerContext;

import com.tesora.dve.common.catalog.StorageSite;

public interface MysqlMultiSiteCommandResultsProcessor extends
		MysqlCommandResultsProcessor {

    //subclasses both assume packet is OK or ERR
    @Override
    boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException;

    public void addSite(StorageSite site, ChannelHandlerContext ctx);
}
