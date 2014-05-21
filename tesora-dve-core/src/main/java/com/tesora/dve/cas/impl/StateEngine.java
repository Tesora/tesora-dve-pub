package com.tesora.dve.cas.impl;

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

import com.tesora.dve.cas.AtomicState;
import com.tesora.dve.cas.CopyOnInvoke;
import com.tesora.dve.cas.EngineControl;

import org.apache.log4j.Logger;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;


public class StateEngine<S> implements EngineControl<S>{
    static final Logger log = Logger.getLogger(StateEngine.class);
    static ThreadLocal<StateEngine.EngineInvocation> invocationOnThisThread = new ThreadLocal<StateEngine.EngineInvocation>();

    protected String name;
    protected AtomicState<S> atomicState;
    protected Class<S> stateClass;

    AtomicLong stateChangeOK = new AtomicLong(0L);
    AtomicLong stateChangeCollide = new AtomicLong(0L);

    public StateEngine(String name,Class<S> stateClass) {
        this(name,stateClass,new SimpleAtomicState<S>(stateClass,null));
    }

    public StateEngine(String name,Class<S> stateClass, AtomicState<S> stateHolder) {
        if (name == null)
            throw new NullPointerException("name cannot be null");
        if (stateClass == null)
            throw new NullPointerException("state class cannot be null");
        if (stateHolder == null)
            throw new NullPointerException("state holder cannot be null");
        this.name = name;
        this.stateClass = stateClass;
        this.atomicState = stateHolder;
    }


    @SuppressWarnings("unchecked")
    void verifyLegalProxy(Class clazz) {
        if (! clazz.isInterface() || !clazz.isAssignableFrom(this.stateClass) )
            throw new IllegalArgumentException("requested proxy class is not an interface implemented by state class "+ this.stateClass);
    }


    @Override
    public void redispatch() {
        if (invocationOnThisThread.get() == null){
            throw new IllegalStateException("Cannot invoke redispatch from outside a state machine invocation");
        } else {
            invocationOnThisThread.get().doRetry();
        }
    }

    @Override
    public void clearRedispatch() {
        if (invocationOnThisThread.get() == null){
            throw new IllegalStateException("Cannot invoke redispatch from outside a state machine invocation");
        } else {
            invocationOnThisThread.get().doClearRetry();
        }
    }

    @Override
    public <P> P getProxy(Class<P> firstRequestedInterface, Class... additionalRequestedInterfaces){
        verifyLegalProxy(firstRequestedInterface);
        for (Class clazz : additionalRequestedInterfaces)
            verifyLegalProxy(clazz);

        Class[] allRequested = Arrays.copyOf(additionalRequestedInterfaces,additionalRequestedInterfaces.length + 1);
        allRequested[additionalRequestedInterfaces.length] = firstRequestedInterface;

        return firstRequestedInterface.cast(
                Proxy.newProxyInstance(
                StateEngine.class.getClassLoader(),
                allRequested,
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        return StateEngine.this.proxyDispatch(proxy, method, args);
                    }
                }
            )
        );

    }


    @Override
    public boolean trySetState(S expected, S newState) {
        return atomicState.compareAndSet(expected,newState);
    }

    @Override
    public boolean trySetState(S newState) {
        if (invocationOnThisThread.get() == null){
            throw new IllegalStateException("Cannot invoke single arg trySetState from outside a state machine invocation");
        }

        boolean changed = atomicState.compareAndSet(stateClass.cast(invocationOnThisThread.get().baseState), newState);
        if (changed)
            stateChangeSuccess();
        else
            stateChangeFailure();
        return changed;
    }

    void stateChangeFailure() {
        stateChangeCollide.incrementAndGet();
    }

    void stateChangeSuccess() {
        stateChangeOK.incrementAndGet();
    }

    protected Object proxyDispatch(Object proxy, Method method, Object[] args) throws Exception {
        EngineInvocation invocation = new EngineInvocation();
        invocation.meth = method;
        invocation.args = args;
        invocation.shouldRedispatch = false;
        return invocation.invoke();
    }

    public String toString(){
        return String.format("StateEngine[name=%s,currentState=%s]", name, atomicState.get());
    }


    //****************************************************************************************************
    //INNER CLASSES
    //****************************************************************************************************

    class EngineInvocation {
        Object baseState;
        Method meth;
        Object[] args;
        long spin = 0;

        boolean shouldRedispatch = false;


        public void doRetry(){
            this.shouldRedispatch = true;
        }

        public void doClearRetry() {
            this.shouldRedispatch = false;
        }

        public Object invoke() throws Exception {
            final EngineInvocation previouslyRunning = invocationOnThisThread.get();
            invocationOnThisThread.set(this);
            try {
                for(;;){
                    this.shouldRedispatch = false;
                    Object retVal;
                    baseState = atomicState.get();
                    Object callState;
                    if (baseState instanceof CopyOnInvoke){
                        CopyOnInvoke cloner = (CopyOnInvoke)baseState;
                        callState = stateClass.cast(cloner.mutableCopy(StateEngine.this));
                    } else
                        callState = baseState;
                    if (log.isDebugEnabled()){
                        String message = String.format("invocation [engine.name=%s,thread=%s,method=%s, baseState=%s, spin=%s]\n", name, Thread.currentThread(), meth.getName(), baseState, spin);
                        log.debug(message);
                    }
                    try {
                        retVal = meth.invoke(callState, args);
                    } catch (IllegalAccessException e) {
                        throw e;
                    } catch (InvocationTargetException e) {
                        Throwable t = e.getCause();
                        if (t == null)
                            throw e;
                        else if (t instanceof Error)
                            throw (Error)t;
                        else if (t instanceof RuntimeException)
                            throw (RuntimeException)t;
                        else
                            throw (Exception)t;
                    }

                    if(shouldRedispatch){
                        spin ++;
                        continue;
                    }
                    else
                        return retVal;
                }
            } finally {
                invocationOnThisThread.set(previouslyRunning);
            }
        }


    }
}
