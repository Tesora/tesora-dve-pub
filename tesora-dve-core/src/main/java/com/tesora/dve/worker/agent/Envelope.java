// OS_STATUS: public
package com.tesora.dve.worker.agent;

public interface Envelope {

	Envelope from(String fromAddr);

	Envelope to(String toAddr);

	Object getPayload();

	String getReplyAddress();
}