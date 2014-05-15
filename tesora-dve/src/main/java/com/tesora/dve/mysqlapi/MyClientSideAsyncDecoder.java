// OS_STATUS: public
package com.tesora.dve.mysqlapi;

import io.netty.buffer.ByteBuf;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.libmy.MyMessageType;

public class MyClientSideAsyncDecoder extends MyDecoder {
	private static final Logger logger = Logger.getLogger(MyClientSideAsyncDecoder.class);
	static final byte RESP_TYPE_REPL = (byte) 0x00;
	static final byte RESP_TYPE_ERR = (byte) 0xff;
	static final byte RESP_TYPE_EOF = (byte) 0xfe;

	@Override
	MyMessage instantiateMessage(ByteBuf frame) {
		MyMessage nativeMsg;
		MyMessageType mt;
		switch (frame.readByte()) {
		case RESP_TYPE_REPL:
			mt = MyMessageType.REPL_EVENT_RESPONSE;
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

		// create a new instance of the appropriate message
		nativeMsg = super.newResponseInstance(mt);

		return nativeMsg;
	}
}
