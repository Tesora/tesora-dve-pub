// OS_STATUS: public
package com.tesora.dve.mysqlapi;

import io.netty.buffer.ByteBuf;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.libmy.MyMessageType;

public class MyClientSideSyncDecoder extends MyDecoder {
	private static final Logger logger = Logger.getLogger(MyClientSideSyncDecoder.class);
	static final byte RESP_TYPE_OK = (byte) 0x00;
	static final byte RESP_TYPE_ERR = (byte) 0xff;
	static final byte RESP_TYPE_EOF = (byte) 0xfe;

	@Override
	MyMessage instantiateMessage(ByteBuf frame) {
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
