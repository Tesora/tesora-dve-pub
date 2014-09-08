package com.tesora.dve.db.mysql;

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

import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.concurrent.PEDefaultPromise;
import com.tesora.dve.concurrent.SynchronousCompletion;

/**
 *
 */
public class SynchronousResultProcessor extends DefaultResultProcessor implements SynchronousCompletion<Boolean> {
    SynchronousCompletion<Boolean> syncHandle;
    public SynchronousResultProcessor() {
        super( new PEDefaultPromise<Boolean>() );
        this.syncHandle = (PEDefaultPromise<Boolean>) promise;
    }

    @Override
    public Boolean sync() throws Exception {
        return syncHandle.sync();
    }
}
