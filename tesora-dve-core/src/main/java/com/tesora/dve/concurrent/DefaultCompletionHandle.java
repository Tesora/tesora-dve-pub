package com.tesora.dve.concurrent;

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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public abstract class DefaultCompletionHandle<T> implements CompletionHandle<T> {
    final AtomicBoolean fired = new AtomicBoolean(false);

    protected abstract void onSuccess(T returnValue);
    protected abstract void onFailure(Exception e);

    @Override
    public final boolean trySuccess(T returnValue) {
        if (fired.compareAndSet(false, true)) {
            onSuccess(returnValue);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public final boolean isFulfilled() {
        return fired.get();
    }

    @Override
    public final void success(T returnValue) {
        trySuccess(returnValue);
    }

    @Override
    public final void failure(Exception e) {
        if (fired.compareAndSet(false,true))
            onFailure(e);
    }

}
