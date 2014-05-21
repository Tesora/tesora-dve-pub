package com.tesora.dve.db.mysql.portal.protocol;

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

import com.tesora.dve.db.mysql.common.MysqlHandshake;
import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.libmy.MyOKResponse;
import com.tesora.dve.db.mysql.libmy.MyServerGreetingErrorResponse;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class InboundMysqlAuthenticationHandlerV10 extends ChannelInboundHandlerAdapter {
    static final Logger logger = LoggerFactory.getLogger(InboundMysqlAuthenticationHandlerV10.class);

    public enum AuthState { UNAUTHENTICATED, AUTHENTICATED, FAILED }

    protected AuthState currentAuthState = AuthState.UNAUTHENTICATED;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        boolean forwarded = false;
        try {
            switch (currentAuthState){
                case AUTHENTICATED:
                    //already authenticated, pass decoded message through to other handler.
                    forwarded = true;
                    ctx.fireChannelRead(msg);
                    break;
                case FAILED:
                    ReferenceCountUtil.release(msg);
                    ctx.channel().close();
                    break;
                case UNAUTHENTICATED: {
                    if (!(msg instanceof MSPAuthenticateV10MessageMessage))
                        throw new PECodingException("Expecting authentication message, received, " + msg);

                    MSPAuthenticateV10MessageMessage authMessage = (MSPAuthenticateV10MessageMessage)msg;
                    authenticateClient(ctx, authMessage);
                }
                break;
                default:
                    throw new PECodingException("Unexpected authorization state, " + currentAuthState);
            }
        } finally {
            if (!forwarded)
                ReferenceCountUtil.release(msg);
        }

    }

    @Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		super.channelActive(ctx);

		try {

            MysqlHandshake handshake = null;
            handshake = doHandshake(ctx);

			// if we don't have a ssCon available then we don't need to send the greeting
			if (handshake != null) {
                sendGreeting(ctx, handshake);
            }

		} catch (Exception e) {
			ctx.channel().write(new MyServerGreetingErrorResponse(e));
		}
	}


    protected void sendGreeting(ChannelHandlerContext ctx, MysqlHandshake handshake) {
        int connectionId = handshake.getConnectionId();
        String salt = handshake.getSalt();
        int serverCapabilities = (int) handshake.getServerCapabilities();
        String serverVersion = handshake.getServerVersion();
        byte serverCharSet = handshake.getServerCharSet();
        String pluginData = handshake.getPluginData();

        MSPServerGreetingRequestMessage.write(ctx, connectionId, salt, serverCapabilities, serverVersion, serverCharSet, pluginData);
    }

    void authenticateClient(ChannelHandlerContext ctx, MSPAuthenticateV10MessageMessage authMessage) throws PEException {
        byte seqID = authMessage.getSequenceID();

        // Login in the SSConnection
        MyMessage mysqlResp;
        try {
            mysqlResp = doAuthenticate(ctx, authMessage);
        } catch (PEException e) {
            mysqlResp = new MyErrorResponse(e.rootCause());
            mysqlResp.setPacketNumber(seqID + 1);
        } catch (Throwable t) {
            mysqlResp = new MyErrorResponse(new Exception(t.getMessage()));
            mysqlResp.setPacketNumber(seqID + 1);
        }
        if (mysqlResp instanceof MyOKResponse)
            currentAuthState = AuthState.AUTHENTICATED;
        else
            currentAuthState = AuthState.FAILED;

        ctx.write(mysqlResp);
        ctx.flush();
    }

    protected abstract MyMessage doAuthenticate(ChannelHandlerContext ctx, MSPAuthenticateV10MessageMessage authMessage) throws Throwable;
    protected abstract MysqlHandshake doHandshake(ChannelHandlerContext ctx);

}
