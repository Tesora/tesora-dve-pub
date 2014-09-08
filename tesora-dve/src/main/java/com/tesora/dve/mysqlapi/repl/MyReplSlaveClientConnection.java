package com.tesora.dve.mysqlapi.repl;

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

import com.tesora.dve.db.mysql.DefaultResultProcessor;
import com.tesora.dve.db.mysql.MysqlConnection;
import com.tesora.dve.db.mysql.MysqlNativeConstants;
import com.tesora.dve.db.mysql.SynchronousResultProcessor;
import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.db.mysql.portal.protocol.ClientCapabilities;
import com.tesora.dve.db.mysql.portal.protocol.MSPAuthenticateV10MessageMessage;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.*;
import com.tesora.dve.mysqlapi.repl.messages.MyComBinLogDumpRequest;
import com.tesora.dve.mysqlapi.repl.messages.MyComRegisterSlaveRequest;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.List;

public class MyReplSlaveClientConnection {
    private static final Logger logger = Logger.getLogger(MyReplSlaveClientConnection.class);

    private final MyReplicationSlaveService plugin;
    private final MyClientConnectionContext context;

    private final DefaultResultProcessor logEventProcessor;

    protected boolean connected = false;
    private Channel channel = null;
    private EventLoopGroup workerGroup = null;

    enum State { WAITING_FOR_GREETING, AUTHENTICATING, REGISTERING_AS_SLAVE, PROCESSING_EVENTS }

    public MyReplSlaveClientConnection(MyReplSlaveConnectionContext rsContext, MyReplicationSlaveService plugin) {
        this(rsContext,plugin, new MyReplSlaveAsyncHandler(rsContext, plugin));
    }

    public MyReplSlaveClientConnection(MyReplSlaveConnectionContext rsContext, MyReplicationSlaveService plugin, DefaultResultProcessor processor) {
        this.context = rsContext;
        this.plugin = plugin;
        this.logEventProcessor = processor;
    }

    private void registerAsSlave() {
        //TODO: this method issues a slave register, then triggers the binlog dump on success.  It's ready to move over to the traditional stack. -sgossard
        MyComRegisterSlaveRequest rsr = new MyComRegisterSlaveRequest(plugin.getSlaveServerID(), plugin.getMasterPort());

        channel.pipeline().remove(MyServerHandshakeHandler.class.getSimpleName());

        channel.pipeline().addLast(
                MyServerSlaveRegisterHandler.class.getSimpleName(),
                adaptDefaultProcessor(new MyServerSlaveRegisterHandler())
        );

        channel.writeAndFlush(rsr);
    }

	public void dumpBinlog() throws PEException {
        //TODO: this method issues a dump binlog request, then processes all the responses.  It's ready to move over to the traditional stack. -sgossard
        MyBinLogPosition blp;
        try {
            blp = plugin.getBinLogPosition();
        } catch (SQLException e) {
            this.stop();
            throw new PEException("Cannot find binlog_status information for "
                    + plugin.getMasterHost(), e);
        }

        MyDecoder newDecoder = new MyClientSideAsyncDecoder();

        if (logger.isDebugEnabled())
            logger.debug("Switching MyClientConnection to asynchronous mode");

        channel.pipeline().addFirst(newDecoder.getClass().getSimpleName(),
                newDecoder);

        channel.pipeline()
                .remove(MyClientSideSyncDecoder.class.getSimpleName());

        channel.pipeline().addLast(logEventProcessor.getClass().getSimpleName(),
                adaptDefaultProcessor(logEventProcessor));
        connected = true;

        if (logger.isDebugEnabled())
            logger.debug("Sending binlog dump request to "
                    + plugin.getMasterLocator() + " for "
                    + plugin.getSlaveInfo());

        MyComBinLogDumpRequest blr = new MyComBinLogDumpRequest(
                plugin.getSlaveServerID(), blp.getPosition(), blp.getFileName());

        channel.writeAndFlush(blr);
    }

    public boolean start(){
        boolean ret = false;

        workerGroup = new NioEventLoopGroup();
        ChannelFuture f = null;
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_REUSEADDR, true);
            b.option(ChannelOption.TCP_NODELAY, true);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.option(ChannelOption.ALLOCATOR,
                    MysqlConnection.USE_POOLED_BUFFERS ? PooledByteBufAllocator.DEFAULT
                            : UnpooledByteBufAllocator.DEFAULT);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(MyEncoder.class.getSimpleName(),
                            new MyEncoder());
                    ch.pipeline().addLast(
                            MyClientSideSyncDecoder.class.getSimpleName(),
                            new MyClientSideSyncDecoder());
                    ch.pipeline().addLast(
                            MyServerHandshakeHandler.class.getSimpleName(),
                            new MyServerHandshakeHandler());
                }
            });

            // Start the client.
            f = b.connect(context.getConnectHost(), context.getConnectPort())
                    .sync();
            if (f != null) {
                channel = f.channel();
                ret = isStarted();
            }

        } catch (Exception e) {
            stop();
        }

        connected = false;

        return ret;
    }

    public void stop() {
        if (workerGroup != null) {
            if (!workerGroup.isShuttingDown()) {
                workerGroup.shutdownGracefully();
                workerGroup = null;
            }
        }
    }

    public boolean isStarted() {
        if (channel == null)
            return false;

        return channel.isActive();
    }

    public boolean isConnected() {
        if (!isStarted()) {
            return false;
        }
        return connected;
    }

    protected void setConnected(boolean connected){
        //really only used by testing infrastructure.
        this.connected = connected;
    }

    public MyClientConnectionContext getContext() {
        return context;
    }

    public static MessageToMessageDecoder<MyMessage> adaptDefaultProcessor(final DefaultResultProcessor resultProcessor) {
        return new MessageToMessageDecoder<MyMessage>() {

            @Override
            public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
                resultProcessor.packetStall(ctx);
                super.channelReadComplete(ctx);
            }

            @Override
            public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                resultProcessor.active(ctx);
                super.handlerAdded(ctx);
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                if (cause instanceof Exception)
                    resultProcessor.failure((Exception) cause);
                else
                    resultProcessor.failure(new PEException(cause));

                super.exceptionCaught(ctx, cause);
            }

            @Override
            protected void decode(ChannelHandlerContext ctx, MyMessage msg, List<Object> out) throws Exception {
                resultProcessor.processPacket(ctx, msg);
            }

        };
    }

    public class MyClientSideSyncDecoder extends MyDecoder {
        static final byte RESP_TYPE_OK = (byte) 0x00;
        static final byte RESP_TYPE_ERR = (byte) 0xff;
        static final byte RESP_TYPE_EOF = (byte) 0xfe;

        @Override
        protected MyMessage instantiateMessage(ByteBuf frame) {
            MyMessage nativeMsg;
            MyMessageType mt = MyMessageType.UNKNOWN;
            if (!isHandshakeDone()) {
                // if the handshake isn't done, then the message coming in
                // must be a SERVER_GREETING_RESPONSE
                setHandshakeDone(true);
                mt = MyMessageType.SERVER_GREETING_RESPONSE;
            } else {
                // if the handshake is done, the message must be one of the
                // response packets
                switch (frame.readByte()) {
                case RESP_TYPE_OK:
                    mt = MyMessageType.OK_RESPONSE;
                    break;
                case RESP_TYPE_ERR:
                    mt = MyMessageType.ERROR_RESPONSE;
                    break;
                case RESP_TYPE_EOF:
                    mt = MyMessageType.EOFPKT_RESPONSE;
                    break;
                default:
                     mt = MyMessageType.UNKNOWN;
                    break;
                }

                if (logger.isDebugEnabled())
                    logger.debug("Decoding message of type " + mt.toString());
            }
            // create a new instance of the appropriate message
            nativeMsg = super.newResponseInstance(mt);

            return nativeMsg;
        }

    }

    public class MyServerHandshakeHandler extends MessageToMessageDecoder<MyMessage> {
        State localState = State.WAITING_FOR_GREETING;

        public MyServerHandshakeHandler() {
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, MyMessage msg, List<Object> out) throws Exception {
            switch (localState) {
                case WAITING_FOR_GREETING:
                    onHandshake(ctx, msg);
                    break;
                case AUTHENTICATING:
                    onLogin(ctx,msg);
                    break;
                default:
                    throw new PECodingException( "Unexpected state " + localState);
            }
        }

        private void onHandshake(ChannelHandlerContext ctx, MyMessage rm) throws PEException {
            if (!(rm instanceof MyHandshakeV10)) {
                throw new PEException("Invalid state - expected MyServerGreetingResponse recieved "+ rm.getClass());
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Received Server Greeting Response for "
                        + plugin.getName() + " from "
                        + plugin.getMasterLocator());
            }
            context.setSgr((MyHandshakeV10) rm);

            if (logger.isDebugEnabled()) {
                logger.debug("Sending login request to "
                        + plugin.getMasterLocator() + " for "
                        + plugin.getName() + " as " + plugin.getMasterUserid());
            }

            long clientCapabilities = ClientCapabilities.CLIENT_LONG_PASSWORD
                    + ClientCapabilities.CLIENT_LONG_FLAG
                    + ClientCapabilities.CLIENT_LOCAL_FILES
                    + ClientCapabilities.CLIENT_PROTOCOL_41
                    + ClientCapabilities.CLIENT_TRANSACTIONS
                    + ClientCapabilities.CLIENT_SECURE_CONNECTION;

            String hashedPwd = MSPAuthenticateV10MessageMessage.computeSecurePasswordString(
                    plugin.getMasterPwd(), context.getSaltforPassword());

            MyLoginRequest lr = new MyLoginRequest(plugin.getMasterUserid(), hashedPwd)
                    .setClientCharset(MysqlNativeConstants.MYSQL_CHARSET_LATIN1)
                    .setMaxPacketSize(MysqlNativeConstants.MAX_PACKET_SIZE)
                    .setClientCapabilities(clientCapabilities)
                    .setPlugInData(getContext().getPlugInData());

            localState = State.AUTHENTICATING;
            ctx.writeAndFlush(lr);
        }

        void onLogin(ChannelHandlerContext ctx, MyMessage rm) throws PEException {
            if (!(((MyResponseMessage) rm).isOK())) {
                stop();
                throw new PEException(((MyErrorResponse) rm).getErrorMsg());
            }

            if (logger.isDebugEnabled())
                logger.debug("Successful login to " + plugin.getMasterLocator()
                        + " as " + plugin.getMasterUserid());

            if (logger.isDebugEnabled())
                logger.debug("Sending slave registration request to "
                        + plugin.getMasterLocator() + " for Slave #"
                        + plugin.getSlaveServerID());

            registerAsSlave();
        }
    }

    public class MyServerSlaveRegisterHandler extends SynchronousResultProcessor {

        public MyServerSlaveRegisterHandler() {
        }

        @Override
        public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
            try {
                decode(ctx, message);
            } catch (Exception e){
                super.failure(e);
            }
            return false;
        }

        protected void decode(ChannelHandlerContext ctx, MyMessage msg) throws Exception {
            try {
                onSlaveRegister(ctx, msg);
            } finally {
                ctx.pipeline().remove(MyServerSlaveRegisterHandler.class.getSimpleName());
            }

        }

        void onSlaveRegister(ChannelHandlerContext ctx, MyMessage rm)
                throws PEException {
            if (!(((MyResponseMessage) rm).isOK())) {
                stop();
                throw new PEException(((MyErrorResponse) rm).getErrorMsg());
            }

            if (logger.isDebugEnabled())
                logger.debug("Slave registration successful with "
                        + plugin.getMasterLocator() + " for Slave #"
                        + plugin.getSlaveServerID());

            // switch to Asynchronous messaging mode
            dumpBinlog();
        }

    }
}
