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

import com.tesora.dve.db.mysql.MysqlNativeConstants;
import com.tesora.dve.db.mysql.common.JavaCharsetCatalog;
import com.tesora.dve.db.mysql.common.SimpleCredentials;
import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyHandshakeErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyHandshakeV10;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.AttributeKey;

import java.net.SocketAddress;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PESQLException;

import io.netty.util.CharsetUtil;
import org.apache.log4j.Logger;

public class MysqlClientAuthenticationHandler extends ByteToMessageDecoder {
    static final Logger log = Logger.getLogger(MysqlClientAuthenticationHandler.class);
    private static final long MAXIMUM_WAITTIME_MINUTES=5;  //abort operation if site doesn't at least authenticate us after 5 minutes.
	private static final int MESSAGE_HEADER_LEN = 4;

	enum AuthenticationState { AWAIT_GREETING, AWAIT_ACKNOWLEGEMENT, AUTHENTICATED, FAILURE };

	public static final AttributeKey<MyHandshakeV10> HANDSHAKE_KEY = new AttributeKey<MyHandshakeV10>("ServerHandshake");

    CountDownLatch finished = new CountDownLatch(1);
	volatile AuthenticationState state = AuthenticationState.AWAIT_GREETING;  //must be volatile, it is read by multiple threads that haven't sync'ed on the same monitor.
	private SimpleCredentials userCredentials;
	private int serverThreadID;

    JavaCharsetCatalog javaCharsetCatalog;
	Charset serverCharset = CharsetUtil.UTF_8;

	long clientCapabilities;

	public MysqlClientAuthenticationHandler(SimpleCredentials userCredentials, long clientCapabilities, JavaCharsetCatalog javaCharsetCatalog) {
		this.userCredentials = userCredentials;
        this.javaCharsetCatalog = javaCharsetCatalog;
        this.clientCapabilities = clientCapabilities;
	}


	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		decode(ctx, in, out, false);
	}

	@Override
	protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		decode(ctx, in, out, true);
	}

	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out, boolean isLastBytes) throws Exception {
		ByteBuf leBuf = in.order(ByteOrder.LITTLE_ENDIAN);

		leBuf.markReaderIndex();
		boolean messageProcessed = false;

		try {
			if (leBuf.readableBytes() > MESSAGE_HEADER_LEN) {
				int payloadLen = leBuf.readMedium();
				leBuf.readByte(); // seq

				if (leBuf.readableBytes() >= payloadLen) {
					ByteBuf payload = leBuf.slice(leBuf.readerIndex(), payloadLen).order(ByteOrder.LITTLE_ENDIAN);
					Byte protocolVersion = leBuf.readByte();
					leBuf.skipBytes(payloadLen-1);
					messageProcessed = true;

					if (state == AuthenticationState.AWAIT_GREETING)
						processGreeting(ctx, payload, protocolVersion);
					else
						processAcknowlegement(ctx, payload);
				}
			}

		} finally {
			if (!messageProcessed)
				leBuf.resetReaderIndex();

            if (isLastBytes && (state == AuthenticationState.AWAIT_ACKNOWLEGEMENT || state == AuthenticationState.AWAIT_GREETING) ){
                //we are waiting for handshake packets from mysql, but no more packets will ever arrive.  release blocked callers.
                Channel channel = ctx.channel();
                SocketAddress addr = (channel == null ? null : channel.remoteAddress() );
                log.warn("Socket closed in middle of authentication handshake on socket "+addr);
                enterState(AuthenticationState.FAILURE);
            }
		}
	}

	private void processGreeting(ChannelHandlerContext ctx, ByteBuf payload, Byte protocolVersion) throws Exception {
		if (protocolVersion == MyErrorResponse.ERRORPKT_FIELD_COUNT) {
			processErrorPacket(ctx, payload);
		} else {
			MyHandshakeV10 handshake = new MyHandshakeV10();
			handshake.unmarshallMessage(payload);
			ctx.channel().attr(HANDSHAKE_KEY).set(handshake);
			serverCharset = handshake.getServerCharset( javaCharsetCatalog );
			serverThreadID = handshake.getThreadID();
			ByteBuf out = Unpooled.buffer().order(ByteOrder.LITTLE_ENDIAN);
			try {
                String userName = userCredentials.getName();
                String userPassword = userCredentials.getPassword();
                String salt = handshake.getSalt();
                Charset charset = javaCharsetCatalog.findJavaCharsetById(handshake.getServerCharsetId());
                int mysqlCharsetID = MysqlNativeConstants.MYSQL_CHARSET_UTF8;
                int capabilitiesFlag = (int) clientCapabilities;
                handshake.setServerCharset((byte) mysqlCharsetID);

                MSPAuthenticateV10MessageMessage.write(out, userName, userPassword, salt, charset, mysqlCharsetID, capabilitiesFlag);
                ctx.write(out);
			} catch (Exception e) {
				out.release();
                log.debug("Couldn't write auth handshake to socket",e);
			}
			ctx.flush();
            enterState(AuthenticationState.AWAIT_ACKNOWLEGEMENT);
		}
	}

	private void processAcknowlegement(ChannelHandlerContext ctx, ByteBuf payload) throws Exception {
		byte fieldCount = payload.getByte(payload.readerIndex());
		if (fieldCount == MyErrorResponse.ERRORPKT_FIELD_COUNT) {
			processErrorPacket(ctx, payload);
		} else {
			ctx.pipeline().remove(this);

			// By removing this handler from the pipeline then normal release processing will
			// not happen on the payload ByteBuf. Therefore release it here
			payload.release();

			enterState(AuthenticationState.AUTHENTICATED);
		}
	}

	private void processErrorPacket(ChannelHandlerContext ctx, ByteBuf payload)
			throws Exception {
		enterState(AuthenticationState.FAILURE);
		MyHandshakeErrorResponse errorResp = new MyHandshakeErrorResponse(serverCharset);
		errorResp.unmarshallMessage(payload);
		throw errorResp.asException();
	}

	private synchronized void enterState(AuthenticationState newState) {
        boolean alreadyFinished = true;
        try {
            alreadyFinished = finished.await(0, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {}//shouldn't happen, we aren't waiting, just checking status.
        if (alreadyFinished){
            log.warn("we already unlatched, current state is " + state + " , new state is "+newState);
            return;
        }

        this.state = newState;
        if (state == AuthenticationState.AUTHENTICATED || state == AuthenticationState.FAILURE)
            finished.countDown();
	}

	public void assertAuthenticated() throws PESQLException {
        try {
            boolean receivedResponse = finished.await(MAXIMUM_WAITTIME_MINUTES, TimeUnit.MINUTES);
            if (!receivedResponse){
                throw new PESQLException("Timeout trying to authenticate as user " + userCredentials.getName());
            }
            AuthenticationState check = state;
            if (check == AuthenticationState.FAILURE)
                throw new PESQLException("Failed to authenticate as user " + userCredentials.getName());

            if (check != AuthenticationState.AUTHENTICATED)
                throw new PECodingException("unexpected state after unlatch, "+check);

        } catch (InterruptedException e) {
            throw new PESQLException("Interrupted while authenticating as user "+userCredentials.getName());
        }
	}
	
	public int getThreadID() {
		return serverThreadID;
	}
}
