// OS_STATUS: public
package com.tesora.dve.locking;

import com.tesora.dve.debug.Debuggable;
import com.tesora.dve.groupmanager.MembershipView;
import com.tesora.dve.locking.impl.IntentEntry;

import java.util.EnumSet;


public interface GlobalLockState extends Debuggable {
    GlobalLockState mutableCopy();

    LockMode getDeclaredMode();
    EnumSet<LockMode> getGrantedModes();

    EnumSet<LockMode> getBlockedModes();
    boolean isAcquiring();
    boolean isGranted(LockMode mode);

    boolean isBlocking(LockMode other);

    Runnable acquiringExclusive();
    Runnable acquiringShared();
    Runnable unlocking();
    Runnable downgradingToShared();
    Runnable process(IntentEntry someEntry);

    boolean isStale(IntentEntry someEntry);

    Runnable membershipChange(MembershipView newView);
}
