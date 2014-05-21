// OS_STATUS: public
package com.tesora.dve.locking.impl;

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

import com.tesora.dve.debug.DebugHandle;
import com.tesora.dve.debug.Debuggable;
import com.tesora.dve.locking.ClusterLock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;


public class ReadWriteLockAdapter implements ReadWriteLock, Debuggable {
    ClusterLock clusterLock;

    public ReadWriteLockAdapter(ClusterLock clusterLock){
        this.clusterLock = clusterLock;
    }

    @Override
    public void writeTo(DebugHandle displayOut) {
        clusterLock.writeTo(displayOut);
    }

    @Override
    public Lock readLock() {
        return new Lock(){
            @Override
            public void lock() { clusterLock.sharedLock(null, "adapted read lock acquire"); }
            @Override
            public void unlock() { clusterLock.sharedUnlock(null, "adapted read lock release"); }

            @Override
            public void lockInterruptibly() throws InterruptedException { throw new UnsupportedOperationException(); }
            @Override
            public boolean tryLock() {throw new UnsupportedOperationException(); }
            @Override
            public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {throw new UnsupportedOperationException(); }
            @Override
            public Condition newCondition() {throw new UnsupportedOperationException(); }
        };
    }

    @Override
    public Lock writeLock() {
        return new Lock(){
            @Override
            public void lock() { clusterLock.exclusiveLock(null,"adapted write lock acquire"); }
            @Override
            public void unlock() { clusterLock.exclusiveUnlock(null, "adapted write lock release"); }

            @Override
            public void lockInterruptibly() throws InterruptedException { throw new UnsupportedOperationException(); }
            @Override
            public boolean tryLock() {throw new UnsupportedOperationException(); }
            @Override
            public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {throw new UnsupportedOperationException(); }
            @Override
            public Condition newCondition() {throw new UnsupportedOperationException(); }
        };
    }

}
