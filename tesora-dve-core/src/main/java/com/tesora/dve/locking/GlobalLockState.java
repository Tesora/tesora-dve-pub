package com.tesora.dve.locking;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import com.tesora.dve.debug.Debuggable;
import com.tesora.dve.membership.MembershipView;

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
