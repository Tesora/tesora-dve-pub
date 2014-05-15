// OS_STATUS: public
package com.tesora.dve.db.mysql.portal;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;

import com.tesora.dve.db.mysql.libmy.MyHandshakeErrorResponse;
import com.tesora.dve.server.connectionmanager.SSConnection;

@Sharable
public class ConnectionHandlerAdapter extends ChannelInboundHandlerAdapter {
//	private static final Logger logger = Logger.getLogger(ConnectionHandlerAdapter.class);

	static public AttributeKey<SSConnection> SSCON_KEY = new AttributeKey<SSConnection>("SSConnection");

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		super.channelActive(ctx);
		
		try {
			SSConnection ssConnection = new SSConnection();
			
			ctx.channel().attr(SSCON_KEY).set(ssConnection);
			
			ssConnection.addShutdownHook(new Thread(){
				@Override
				public void run() {
					ctx.channel().close();
				}
			});
		} catch (Exception e) {
			ctx.channel().write(new MyHandshakeErrorResponse(e));
			ctx.channel().flush();
		}
	}			

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		SSConnection ssConnection = ctx.channel().attr(SSCON_KEY).getAndRemove();

		if(ssConnection != null)
			ssConnection.close();
		
		super.channelInactive(ctx);
	}
	
	private static final class InstanceHolder {
		private static final ConnectionHandlerAdapter INSTANCE = new ConnectionHandlerAdapter();
	}
	
	public static ConnectionHandlerAdapter getInstance() {
		return InstanceHolder.INSTANCE;
	}
}
