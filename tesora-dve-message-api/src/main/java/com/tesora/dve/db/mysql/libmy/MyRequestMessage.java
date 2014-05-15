// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;


public abstract class MyRequestMessage extends MyMessage {

	@Override
	public boolean isMessageTypeEncoded() {
		return true;
	}
}
