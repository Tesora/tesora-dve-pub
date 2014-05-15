// OS_STATUS: public
package com.tesora.dve.db.mysql.portal;

import com.tesora.dve.charset.NativeCharSet;
import com.tesora.dve.charset.mysql.MysqlNativeCharSetCatalog;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.mysql.portal.protocol.InboundMysqlAuthenticationHandlerV10;
import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.libmy.MyOKResponse;
import com.tesora.dve.db.mysql.portal.protocol.MSPAuthenticateV10MessageMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.db.mysql.common.MysqlHandshake;
import com.tesora.dve.server.connectionmanager.messages.ExecuteRequestExecutor;
import com.tesora.dve.worker.UserCredentials;
import io.netty.channel.ChannelHandlerContext;

import com.tesora.dve.server.connectionmanager.SSConnection;
import io.netty.util.CharsetUtil;

public class MSPAuthenticateHandlerV10 extends InboundMysqlAuthenticationHandlerV10 {

    public MSPAuthenticateHandlerV10() {
    }

    protected MysqlHandshake doHandshake(ChannelHandlerContext ctx) {
        MysqlHandshake handshake;SSConnection ssCon = ctx.channel().attr(ConnectionHandlerAdapter.SSCON_KEY).get();
        handshake = ssCon.getHandshake();
        return handshake;
    }

    protected MyMessage doAuthenticate(ChannelHandlerContext ctx, MSPAuthenticateV10MessageMessage authMessage) throws Throwable {
        MyMessage mysqlResp;
        byte sequenceId = authMessage.getSequenceID();
        byte clientCharsetId = authMessage.getCharsetID();
        String username = authMessage.getUsername();
        String password = authMessage.getPassword();

        SSConnection ssCon = ctx.channel().attr(ConnectionHandlerAdapter.SSCON_KEY).get();
        ssCon.setClientCapabilities(authMessage.getClientCapabilities());

        UserCredentials userCred = new UserCredentials(username, password, false);
        ssCon.startConnection(userCred);
        String initialDB = authMessage.getInitialDatabase();
        if (! "".equals(authMessage.getInitialDatabase()) ) {
            final DBEmptyTextResultConsumer resultConsumer = new DBEmptyTextResultConsumer();
            byte[] query = ("USE " + initialDB).getBytes(CharsetUtil.UTF_8);
            ExecuteRequestExecutor.execute(ssCon, resultConsumer, query);
        }

        NativeCharSet cliendCharSet = MysqlNativeCharSetCatalog.DEFAULT_CATALOG.findNativeCharsetById(clientCharsetId);
        if (cliendCharSet != null) {
            mysqlResp = new MyOKResponse();
            mysqlResp.setPacketNumber(sequenceId + 1);
            ssCon.setClientCharSet(cliendCharSet);
        } else {
            mysqlResp = new MyErrorResponse(new PEException("Unsupported character set specified (id=" + clientCharsetId + ")"));
            mysqlResp.setPacketNumber(sequenceId + 1);
        }
        return mysqlResp;
    }
}
