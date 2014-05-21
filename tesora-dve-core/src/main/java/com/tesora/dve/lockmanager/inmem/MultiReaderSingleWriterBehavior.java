// OS_STATUS: public
package com.tesora.dve.lockmanager.inmem;

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

import com.tesora.dve.lockmanager.AcquiredLock;
import com.tesora.dve.lockmanager.LockType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MultiReaderSingleWriterBehavior {


    public boolean canAcquire(AcquiredLock ml, Collection<? extends AcquiredLock> enqueued) {
        // ml cannot be granted if there is an exclusive lock before it in the queue.
        for(AcquiredLock cml : enqueued) {
            if (cml.getType() == LockType.EXCLUSIVE)
                return false;
            if (cml == ml)
                return true;
        }
        return false;
    }


    public <T extends AcquiredLock> List<T> release(T head, Collection<T> enqueued) {
        List<T> out = new ArrayList<T>();
        // if the head was exclusive, then signal everything up to next exclusive,
        // unless the first is exclusive, in which case, just signal it.
        T f = enqueued.iterator().next();

        if (f.getType() == LockType.EXCLUSIVE) {
            if (!f.acquiredLock())
                out.add(f);
        } else if (head.getType() == LockType.EXCLUSIVE) {
            int granted = 0;
            for(T s : enqueued) {
                granted++;
                if (s.getType() == LockType.EXCLUSIVE && granted > 1)
                    return out;
                if (!f.acquiredLock())
                    out.add(s);
            }
        }
        return out;
    }


    public <T extends AcquiredLock> String validateGrantLock(T lockToGrant, Collection<T> lockQueue) {
        if (lockToGrant.getType() == LockType.EXCLUSIVE) {
            if (lockQueue.isEmpty())
                return "Internal lock manager error: granting exclusive lock " + lockToGrant + " but is missing from queue";
            T first = lockQueue.iterator().next();
            if (first != lockToGrant)
                return "Internal lock manager error: granting exclusive lock " + lockToGrant + " but first lock is " + first;
        }
        return null;
    }


    public boolean allowsMultiQueueing() {
        // TODO Auto-generated method stub
        return true;
    }

}
