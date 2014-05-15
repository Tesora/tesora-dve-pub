// OS_STATUS: public
package com.tesora.dve.locking.impl;


public class RunnableNoop implements Runnable {
    public static final RunnableNoop NOOP = new RunnableNoop();

    @Override
    public void run() {
    }

    public String toString(){
        return "NOOP";
    }

}
