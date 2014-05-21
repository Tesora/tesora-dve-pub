// OS_STATUS: public
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
