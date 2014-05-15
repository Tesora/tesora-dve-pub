// OS_STATUS: public
package com.tesora.dve.parlb;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

public class MysqlClientHandler extends ChannelInboundHandlerAdapter {

	private InetSocketAddress peServerAddress;

	private volatile Channel outboundChannel;

	public MysqlClientHandler(InetSocketAddress selectedPEServer) {
		this.peServerAddress = selectedPEServer;
	}

   @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        final Channel inboundChannel = ctx.channel();
        
        // Start the connection attempt.
        Bootstrap b = new Bootstrap();
        b.group(inboundChannel.eventLoop())
         .channel(NioSocketChannel.class)
         .handler(new LoadBalancerServerHandler(inboundChannel))
         .option(ChannelOption.AUTO_READ, false);
        ChannelFuture f = b.connect(peServerAddress);
        outboundChannel = f.channel();
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    // connection complete start to read first data
                    inboundChannel.read();
                } else {
                    // Close the connection if the connection attempt has failed.
                    inboundChannel.close();
                }
            }
        });
    }

   @Override
   public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
	   if (outboundChannel.isActive()) {
		   outboundChannel.writeAndFlush(msg).addListener(new ChannelFutureListener() {
			   @Override
			   public void operationComplete(ChannelFuture future) throws Exception {
				   if (future.isSuccess()) {
					   // was able to flush out data, start to read the next chunk
					   ctx.channel().read();
				   } else {
					   future.channel().close();
				   }
			   }
		   });
	   }
   }

   @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (outboundChannel != null) {
            closeOnFlush(outboundChannel);
            outboundChannel.flush();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        closeOnFlush(ctx.channel());
    }

    /**
     * Closes the specified channel after all queued write requests are flushed.
     */
    static void closeOnFlush(Channel ch) {
        if (ch.isActive()) {
        	ch.write(Unpooled.buffer()).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
