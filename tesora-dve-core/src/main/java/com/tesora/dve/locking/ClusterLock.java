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
import com.tesora.dve.membership.GroupTopicListener;
import com.tesora.dve.membership.MembershipView;
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
