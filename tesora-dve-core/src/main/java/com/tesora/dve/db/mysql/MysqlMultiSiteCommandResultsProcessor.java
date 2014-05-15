// OS_STATUS: public
package com.tesora.dve.db.mysql;

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
