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

import com.tesora.dve.exceptions.PECodingException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class ChainedNotifier<R> implements CompletionTarget<R>, CompletionHandle<R>, CompletionNotifier<R>, SynchronousCompletion<R>, Future<R> {
    AtomicReference<State<R>> state;

    public ChainedNotifier() {
        state = new AtomicReference<State<R>>( new State<R>(State.WaitState.PARENT_SUCCESS_LOCAL_WAIT,null,null,null,null));
    }

    public ChainedNotifier(CompletionNotifier<R> dependsOn) {
        state = new AtomicReference<State<R>>( new State<R>());
        ParentAdapter listenForParentFinish = new ParentAdapter();
        dependsOn.addListener(listenForParentFinish);
    }


    @Override
    public void addListener(CompletionTarget<R> listener) {

        State<R> current;
        while (! (current = state.get()).isFullfilled() ){
            State<R> next = current.nextViaAddListener(listener);
            if (state.compareAndSet(current,next))
                return;  //installed new listener.
        }

        //didn't install listener, must already be finished, callback on register thread.
        if (current.isSuccess())
            listener.success(current.result);
        else
            listener.failure(current.error);

    }

    @Override
    public boolean trySuccess(R returnValue) {
        State<R> current;
        while (! (current = state.get()).isFullfilled() ){
            State<R> next = current.nextViaLocalSuccess(returnValue);
            if (state.compareAndSet(current,next)) {
                if (next.isFullfilled())
                    next.notifyListeners();//local success triggered success, tell listeners.
                return true;   //Caller is probably interested if someone already signaled local success, return true regardless of parent state.
            }
        }
        return false;
    }


    protected void handleParentSuccess(R returnValue) {
        State<R> current;
        while (! (current = state.get()).isFullfilled() ){
            State<R> next = current.nextViaParentSuccess(returnValue);
            if (state.compareAndSet(current,next)) {
                if (next.isFullfilled())
                    next.notifyListeners();//parent success triggered success, tell listeners.
                break;
            }
        }
    }

    protected void handleParentFailure(Exception e) {
        this.failure(e);
    }

    @Override
    public boolean isFulfilled() {
        return state.get().isFullfilled();
    }

    @Override
    public void success(R returnValue) {
        trySuccess(returnValue);
    }

    @Override
    public void failure(Exception e) {
        State<R> current;
        while (! (current = state.get()).isFullfilled() ){
            State<R> next = current.nextViaFailure(e);
            if (state.compareAndSet(current,next)) {
                next.notifyListeners();//failure triggered finish, tell listeners.
                break;
            }
        }
    }

    public static <V> ChainedNotifier<V> newInstance(){
        return new ChainedNotifier<>();
    }

    public static <V> ChainedNotifier<V> newInstance(CompletionNotifier<V> dependsOn){
        if (dependsOn == null)
            return newInstance();
        else
            return new ChainedNotifier<>(dependsOn);
    }

    @Override
    public R sync() throws Exception {
        SynchronousListener<R> syncListener = new SynchronousListener<R>();
        this.addListener(syncListener);
        return syncListener.sync();
    }

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
        return isFulfilled();
    }

    @Override
    public R get() throws InterruptedException, ExecutionException {
        try {
            return sync();
        } catch (InterruptedException | ExecutionException e) {
            throw e;
        } catch (Exception e){
            throw new ExecutionException(e);
        }
    }

    @Override
    public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }

    private static class State<V> {
        enum WaitState { WAITING_ON_BOTH, PARENT_SUCCESS_LOCAL_WAIT, LOCAL_SUCCESS_PARENT_WAIT, SUCCESS, FAILURE }

        final private WaitState waitStatus;
        final private V result;
        final private Exception error;
        final private CompletionTarget<V> listener;
        final private State<V> next;


        public State(){
            this(WaitState.WAITING_ON_BOTH,null,null,null,null);
        }

        public State(WaitState waitStatus, V result, Exception error,CompletionTarget<V> listener, State<V> next) {
            this.waitStatus = waitStatus;
            this.result = result;
            this.error = error;
            this.listener = listener;
            this.next = next;
        }

        public boolean isSuccess(){
            return waitStatus == WaitState.SUCCESS;
        }

        public boolean isFailure(){
            return waitStatus == WaitState.FAILURE;
        }

        public boolean isFullfilled(){
            return isSuccess() || isFailure();
        }

        public State<V> nextViaParentSuccess(V someResult){
            switch (waitStatus) {
                case SUCCESS:
                case FAILURE:
                case PARENT_SUCCESS_LOCAL_WAIT:
                    return this;
                case WAITING_ON_BOTH:
                    return new State<V>(WaitState.PARENT_SUCCESS_LOCAL_WAIT,null,null,null,this);
                case LOCAL_SUCCESS_PARENT_WAIT:
                    return new State<V>(WaitState.SUCCESS,this.result,null,null,this);
                default:
                    return throwUnexpectedState();
            }
        }

        public State<V> nextViaLocalSuccess(V someResult){
            switch (waitStatus) {
                case SUCCESS:
                case FAILURE:
                case LOCAL_SUCCESS_PARENT_WAIT:
                    return this;
                case WAITING_ON_BOTH:
                    return new State<V>(WaitState.LOCAL_SUCCESS_PARENT_WAIT,someResult,null,null,this);
                case PARENT_SUCCESS_LOCAL_WAIT:
                    return new State<V>(WaitState.SUCCESS,someResult,null,null,this);
                default:
                    return throwUnexpectedState();
            }
        }

        public State<V> nextViaFailure(Exception e){
            switch (waitStatus) {
                case SUCCESS:
                case FAILURE:
                    return this;
                case WAITING_ON_BOTH:
                case LOCAL_SUCCESS_PARENT_WAIT:
                case PARENT_SUCCESS_LOCAL_WAIT:
                    return new State<V>(WaitState.FAILURE,null,e,null,this);
                default:
                    return throwUnexpectedState();
            }
        }

        public State<V> nextViaAddListener(CompletionTarget<V> listener){
            switch (waitStatus) {
                case SUCCESS:
                case FAILURE:
                    return this;
                case WAITING_ON_BOTH:
                case LOCAL_SUCCESS_PARENT_WAIT:
                case PARENT_SUCCESS_LOCAL_WAIT:
                    return new State<V>(this.waitStatus,this.result,this.error,listener,this);
                default:
                    return throwUnexpectedState();
            }
        }

        protected void notifyListeners(){
            notifyFinished(this);
        }

        protected static <T> void notifyFinished(State<T> triggerState){
            if (triggerState.isSuccess())
                notifySuccess(triggerState);
            else
                notifyFailure(triggerState);
        }

        protected static <T> void notifySuccess(State<T> triggerState){
            T result = triggerState.result;
            while (triggerState != null){
                if (triggerState.listener != null)
                    triggerState.listener.success(result);
                triggerState = triggerState.next;
            }
        }

        protected static <T> void notifyFailure(State<T> triggerState){
            Exception e = triggerState.error;
            while (triggerState != null){
                if (triggerState.listener != null)
                    triggerState.listener.failure(e);
                triggerState = triggerState.next;
            }
        }


        private State<V> throwUnexpectedState() {
            throw new PECodingException("Unexpected state in enum,"+waitStatus);
        }
    }

    private class ParentAdapter implements CompletionTarget<R> {
        @Override
        public void success(R returnValue) {
            handleParentSuccess(returnValue);
        }

        @Override
        public void failure(Exception e) {
            handleParentFailure(e);
        }
    }

}
