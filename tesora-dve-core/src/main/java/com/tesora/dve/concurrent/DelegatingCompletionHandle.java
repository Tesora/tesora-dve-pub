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

/**
 *
 */
public class DelegatingCompletionHandle<T> implements CompletionHandle<T> {
    CompletionHandle<T> delegate;


    public DelegatingCompletionHandle(CompletionHandle<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean trySuccess(T returnValue) {
        return delegate.trySuccess(returnValue);
    }

    @Override
    public boolean isFulfilled() {
        return delegate.isFulfilled();
    }

    @Override
    public void success(T returnValue) {
        delegate.success(returnValue);
    }

    @Override
    public void failure(Exception e) {
        delegate.failure(e);
    }
}
