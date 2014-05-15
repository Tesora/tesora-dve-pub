// OS_STATUS: public
package com.tesora.dve.locking;

import com.tesora.dve.debug.Debuggable;
import com.tesora.dve.lockmanager.LockClient;
import com.tesora.dve.resultset.ResultRow;

import java.util.EnumSet;
import java.util.List;


public interface LocalLockState extends Debuggable {
    LocalLockState mutableCopy();

    void setAllowedGrants(EnumSet<LockMode> grants);

    boolean isUnused();
    boolean isUnlocked();
    boolean isExclusive();
    boolean isShared();
    boolean isOwner(LockClient client);


    Latch acquire(LockClient client, LockMode mode, Thread caller, String reason);
    void release(LockClient client, LockMode mode, Thread caller, String reason);

    List<Latch> nextUnblockSet();
    List<LocalLockState.Latch> nextExclusiveUnblockSet();
    List<LocalLockState.Latch> nextSharedUnblockSet();

    boolean isBlockersEmpty();
    boolean isNextBlockerExclusive();
    boolean isNextBlockerShared();

    void unlatch(List<LocalLockState.Latch> latches);

    void addShowRow(String name, LockMode globalDeclare, List<ResultRow> rows);


    interface Latch {
        LockMode blockMode();
        boolean isAcquired();

        void awaitUnlatch();
        void unlatch();
    }
}
