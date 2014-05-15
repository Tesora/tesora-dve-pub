// OS_STATUS: public
package com.tesora.dve.mysqlapi;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.ByteOrder;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.libmy.MyMessageType;
import com.tesora.dve.db.mysql.libmy.MyResponseMessage;
import com.tesora.dve.exceptions.PEException;


public class MyEncoder extends MessageToByteEncoder<MyMessage> {
	private static final Logger logger = Logger.getLogger(MyEncoder.class);

	public MyEncoder() {
	}

	public ByteBuf encodeMessage(MyMessage message) throws PEException {

		if (logger.isDebugEnabled())
			logger.debug("Encoding message of type " + message.getMessageType().toString());

		ByteBuf buffer = Unpooled.buffer(2048).order(ByteOrder.LITTLE_ENDIAN);

        message.marshallFullMessage(buffer);

		return buffer;
	}

	@Override
	protected void encode(ChannelHandlerContext ctx, MyMessage msg, ByteBuf out)
			throws Exception {
		if (msg instanceof MyResultSetResponse) {
			if (((MyResultSetResponse) msg).getMessageType() == MyMessageType.RESULTSET_RESPONSE) {
				MyResultSetResponse rsresp = (MyResultSetResponse) msg;
				out.writeBytes(encodeMessage(rsresp));
				Iterator<MyResponseMessage> iter = rsresp.getListIterator();
				while (iter.hasNext()) {
					out.writeBytes(encodeMessage((MyResponseMessage) iter.next()));
				}
			}
		} else {
			out.writeBytes(encodeMessage((MyMessage)msg));
		}
	}

}
