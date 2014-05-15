// OS_STATUS: public
package com.tesora.dve.groupmanager;

public interface GroupTopicListener<M> {

	void onMessage(M message);
}
