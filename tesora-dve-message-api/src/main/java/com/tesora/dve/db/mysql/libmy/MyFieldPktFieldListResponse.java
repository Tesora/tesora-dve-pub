// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import io.netty.buffer.ByteBuf;

public class MyFieldPktFieldListResponse extends MyFieldPktResponse {

	@Override
	public void marshallMessage(ByteBuf cb) {
		super.marshallMessage(cb);
		MysqlAPIUtils.putLengthCodedString(cb, getDefaultValue(), true);
	}
	
	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.FIELDPKTFIELDLIST_RESPONSE;
	}
	
	public static class Factory extends MyFieldPktResponse.Factory {
		@Override
		public MyFieldPktResponse newInstance() {
			return new MyFieldPktFieldListResponse();
		}
	}
}
