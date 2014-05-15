// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl.messages;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.db.mysql.libmy.MyMessage;

public abstract class MyEventMessage extends MyMessage implements MyProcessEvent {

	@Override
	public boolean isMessageTypeEncoded() {
		return true;
	}

	@Override
	public String getSkipErrorMessage() {
		return StringUtils.EMPTY;
	}
}
