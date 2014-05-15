// OS_STATUS: public
package com.tesora.dve.groupmanager;

public interface GroupTopicPublisher<M> {
    void publish(M message);
}
