// OS_STATUS: public
package com.tesora.dve.locking;

import com.tesora.dve.debug.Debuggable;
import com.tesora.dve.groupmanager.GroupTopicListener;
import com.tesora.dve.groupmanager.MembershipView;
import com.tesora.dve.locking.impl.IntentEntry;
import com.tesora.dve.lockmanager.LockClient;
import com.tesora.dve.resultset.ResultRow;

import java.util.List;


public interface ClusterLock extends GroupTopicListener<IntentEntry>, Debuggable {
    boolean isUnused();

    void sharedLock(LockClient client, String reason);
    void sharedUnlock(LockClient client,String reason);
    void exclusiveLock(LockClient client,String reason);
    void exclusiveUnlock(LockClient client,String reason);

    void onMembershipChange(MembershipView membershipView);

    void addShowRow(List<ResultRow> rows);
}
