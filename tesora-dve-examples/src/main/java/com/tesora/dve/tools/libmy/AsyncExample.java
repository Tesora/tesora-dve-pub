// OS_STATUS: public
package com.tesora.dve.tools.libmy;

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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.CharsetUtil;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.db.mysql.common.JavaCharsetCatalog;
import com.tesora.dve.db.mysql.common.SimpleCredentials;
import com.tesora.dve.db.mysql.libmy.MyFieldPktResponse;
import com.tesora.dve.db.mysql.libmy.MyTextResultRow;
import com.tesora.dve.db.mysql.portal.protocol.MSPComQueryRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPComQuitRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPEncoder;
import com.tesora.dve.db.mysql.portal.protocol.MSPProtocolEncoder;
import com.tesora.dve.db.mysql.portal.protocol.MyBackendDecoder;
import com.tesora.dve.db.mysql.portal.protocol.MysqlClientAuthenticationHandler;

public class AsyncExample {

	public static void main(String[] args) throws Exception {
		final String mysqlHost = "localhost";
		final int mysqlPort = 3307;
		final String username = "root";
		final String password = "password";
		final boolean isClearText = true;

		final InetSocketAddress serverAddress = new InetSocketAddress(mysqlHost, mysqlPort);

		final MyBackendDecoder.CharsetDecodeHelper charsetHelper = constructCharsetDecodeHelper();
		final SimpleCredentials cred = constructCredentials(username, password, isClearText);
		final JavaCharsetCatalog javaCharsetCatalog = constructJavaCharsetCatalog();
		final MysqlClientAuthenticationHandler authHandler = new MysqlClientAuthenticationHandler(cred, javaCharsetCatalog);

		final NioEventLoopGroup connectionEventGroup = new NioEventLoopGroup(1);
		final Bootstrap mysqlBootstrap = new Bootstrap();
		mysqlBootstrap // .group(inboundChannel.eventLoop())
				.channel(NioSocketChannel.class)
				.group(connectionEventGroup)
				.option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT)
				.handler(new ChannelInitializer<Channel>() {
					@Override
					protected void initChannel(Channel ch) throws Exception {

						ch.pipeline()
								.addLast(authHandler)
								.addLast(MSPProtocolEncoder.class.getSimpleName(), new MSPProtocolEncoder())
								.addLast(MSPEncoder.class.getSimpleName(), MSPEncoder.getInstance())
								.addLast(MyBackendDecoder.class.getSimpleName(), new MyBackendDecoder(charsetHelper))
								.addLast(new ChannelDuplexHandler() {

									@Override
									public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
										System.out.println("WRITE:" + msg);
										super.write(ctx, msg, promise);
									}

									@Override
									public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
										if (msg instanceof MyFieldPktResponse) {
											final MyFieldPktResponse myFieldPktResponse = (MyFieldPktResponse) msg;
											System.out.println("COLUMN: " + myFieldPktResponse.getOrig_column() + "," + myFieldPktResponse.getColumn_type());
										} else if (msg instanceof MyTextResultRow) {
											final StringBuilder builder = new StringBuilder();
											builder.append("ROW:");
											final MyTextResultRow textRow = (MyTextResultRow) msg;
											for (int i = 0; i < textRow.size(); i++) {
												builder.append('\t');
												builder.append(textRow.getString(i));
											}
											System.out.println(builder.toString());
										}
										super.channelRead(ctx, msg);
									}

									@Override
									public void channelInactive(ChannelHandlerContext ctx) throws Exception {
										super.channelInactive(ctx);
										System.out.println("CLOSE. ");
									}
								});
					}
				});

		final ChannelFuture connectFut = mysqlBootstrap.connect(serverAddress);
		connectFut.sync();
		System.out.println("Waiting to finish authenticate handshake");

		authHandler.assertAuthenticated();//don't write a request until you know the login is complete.

		System.out.println("Connected, and authenticated, getting channel to write requests");

		final Channel chan = connectFut.channel();

		System.out.println("Sending two pipelined requests without blocking for first response.");

		chan.write(MSPComQueryRequestMessage.newMessage((byte) 0, "show databases", CharsetUtil.UTF_8));
		chan.write(MSPComQueryRequestMessage.newMessage((byte) 0, "select * from information_schema.tables", CharsetUtil.UTF_8));
		chan.flush();//NOTE:  nothing is sent until this is called.  Use writeAndFlush if you want it to go out immediately.

		System.out.println("Sleeping 5 sec so all results come back"); //normally you would sync to responses here.
		TimeUnit.SECONDS.sleep(5);
		System.out.println("Closing socket.");
		chan.writeAndFlush(new MSPComQuitRequestMessage());//send friendly hangup message. Probably correct to also wait for server close or an OK packet.

		chan.close();
		chan.closeFuture().sync();
		System.out.println("Exiting.");
		System.exit(0);
	}

	protected static JavaCharsetCatalog constructJavaCharsetCatalog() {
		return new JavaCharsetCatalog() {
			@Override
			public Charset findJavaCharsetById(int clientCharsetId) {
				if (clientCharsetId == 33) {
					return CharsetUtil.UTF_8;
				} else if (clientCharsetId == 8) {
					return CharsetUtil.ISO_8859_1;
				} else {
					return null;
				}
			}
		};
	}

	protected static SimpleCredentials constructCredentials(final String username, final String password, final boolean isClearText) {
		return new SimpleCredentials() {
			@Override
			public String getName() {
				return username;
			}

			@Override
			public String getPassword() {
				return password;
			}

			@Override
			public boolean isCleartext() {
				return isClearText;
			}
		};
	}

	protected static MyBackendDecoder.CharsetDecodeHelper constructCharsetDecodeHelper() {
		return new MyBackendDecoder.CharsetDecodeHelper() {

			@Override
			public long lookupMaxLength(byte mysqlCharsetID) {
				if (mysqlCharsetID == 33) {
					return 3;
				} else if (mysqlCharsetID == 8) {
					return 1;
				} else {
					return -1;
				}
			}

			@Override
			public boolean typeSupported(MyFieldType fieldType, short flags, int maxDataLen) {
				return true;
			}
		};
	}
}
