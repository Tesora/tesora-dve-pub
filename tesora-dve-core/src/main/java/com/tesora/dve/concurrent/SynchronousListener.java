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

import com.tesora.dve.sql.node.expression.IntervalExpression;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *  An adapter that converts a CompletionTarget asynchronous callback into a SynchronousCompletion callback.
 *  This class is designed to have any number of consumer threads waiting for results.  This implementation will use the
 *  first callback, and ignore any additional success/failure calls.
 *  <br/>
*/
public class SynchronousListener<S> implements CompletionTarget<S>, SynchronousCompletion<S> , Future<S> {
    private static final int WAIT_TIMEOUT = 60 * 1000;

    enum WaitState {UNFINISHED, SUCCESS, FAILURE }

    private final Object localMonitor = new Object();

    private WaitState state = WaitState.UNFINISHED;
    private int peopleWaiting = 0;
    private S result = null;
    private Exception error = null;

    public SynchronousListener() {
    }

    @Override
    public void success(S returnValue) {

        synchronized (localMonitor){
            if (state != WaitState.UNFINISHED)
                return; //results already provided.

            result = returnValue;
            triggerFinish(WaitState.SUCCESS);
        }
    }

    @Override
    public void failure(Exception e) {

        synchronized (localMonitor){
            if (state != WaitState.UNFINISHED)
                return; //results already provided.

            error = e;
            triggerFinish(WaitState.FAILURE);
        }

    }

    private void triggerFinish(WaitState finishState) {
        state = finishState;
        if (peopleWaiting > 0)
            localMonitor.notifyAll();
    }

    public S sync() throws Exception {

        synchronized (localMonitor){
            waitUntilFinished();

            if (state == WaitState.SUCCESS)
                return result;
            else
                throw error;
        }
    }

    private void waitUntilFinished() throws InterruptedException {
        while (state == WaitState.UNFINISHED){
            try {
                peopleWaiting ++;
                localMonitor.wait(WAIT_TIMEOUT);
            } catch (InterruptedException ie) {
                //TODO: should we actually do anything if interrupted?
            }finally {
                peopleWaiting --;
            }
        }
    }

    public static <V> V sync(CompletionNotifier<V> notifier) throws Exception {
        SynchronousListener<V> listener = new SynchronousListener<>();
        notifier.addListener(listener);
        return listener.sync();
    }

    //Future implementation

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        synchronized (localMonitor){
            return state != WaitState.UNFINISHED;
        }
    }

    @Override
    public S get() throws InterruptedException, ExecutionException {
        try {
            return sync();
        } catch (InterruptedException | ExecutionException e){
            throw e;
        } catch (Throwable t){
            throw new ExecutionException(t);
        }
    }

    @Override
    public S get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }

}
