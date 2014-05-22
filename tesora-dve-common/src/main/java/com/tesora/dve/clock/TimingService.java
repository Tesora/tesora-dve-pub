package com.tesora.dve.clock;

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
 *  Service for dissecting execution time of requests/activities in the system.  When a client request comes in,
 *  various tasks are performed to satisfy that request.  By noting the start and stop of each of the tasks and if
 *  they execute in sequence or parallel, operators and developers can get a finer grained idea of what work is being
 *  done by the server, why specific requests are fast or slow, and look for ways to improve the system.
 *  <br/>
 *  Timers returned by this service have the obvious start/stop methods that collect the times, but also have categories, sub-categories,
 *  and a nesting hierarchy.  The categories and sub-categories are used to identify the task, while the hierarchy indicates
 *  that a timer was triggered by a parent task.  Typically this means that execution time of the child will contribute to
 *  the execution time of the parent.  In some cases, parent timers may finish before children timers, for example, deleting temp tables
 *  may happen after a response is returned to the client.  In this case, child time doesn't directly contribute to parent time,
 *  but was triggered by the parent activity.  The associated work does not directly impact the performance of the parent request, but may
 *  impact performance of following requests.
 *  <br/>
 *
 *  Unless otherwise noted, methods in the timing service and related timer classes should never return null, so that
 *  callers never need to check for a null return value.  This reduces complexity in the callers making them easier to read, write,
 *  and maintain, and reduces CPU work done in the callers, which is important for hot areas of code in the data path.
 *  <br/>
 *  Also, unless otherwise noted, calls should fail silently rather than throw exceptions.  The intended usage of this code
 *  is to collect timing performance in hot data paths.  If the code can throw exceptions, than it forces the callers to
 *  catch the exceptions or the data path will be impacted.
 *  <br/>
 *  Because of the two mentioned principles above, the timers provided may not be 100% trustworthy, and caution should be
 *  used if results from the timings are used to alter product behavior, rather than gain performance insights.
 */
public interface TimingService {
    /**
     * Returns the logical operation timer for the current thread.  If a pre-existing timer has been stuck to this thread,
     * it will be returned.  If no pre-existing timer was stuck, or has since been cleared, returns a 'root' timer instance.
     * This 'root' instance may be returned repeatedly, or may be a new 'root' instance for each call.
     *
     * @return parent timing context for this thread, never null.
     */
    Timer getTimerOnThread();

    /**
     * Unsticks any timer associated with this thread, and resets the timer returned by getTimerOnThread to 'root' timer instance(s).
     */
    void detachTimerOnThread();

    /**
     * Binds a given timer to the calling thread, so that any future calls to getTimerOnThread will return that timer.
     * This is mainly useful for 'sleazing' timers past methods/objects that were not designed to carry additional data,
     * and cannot be easily modified.
     * <br/>
     * The timing service is required to accept subclasses of timer that it did not construct, and cannot assume that
     * the provided timer is a specific subtype it created.
     * <br/>
     * If the caller provides a null timer, this method should behave like resetTimerOnThread, rather than
     * do nothing (leaves a timer tied to a thread), or throw an exception.
     *
     * @param parent sets the parent timer for calls on this thread, or resets the thread to 'root' if null is provided.
     * @return returns the previously attached timer, or null.
     */
    Timer attachTimerOnThread(Timer parent);

    /**
     * Convenience method to get the current context timer, create a nested sub-timer, and start it.
     * @param location the location of the sub-timer, or null to use the context timers location
     * @return the sub-timer that has been started as a side effect of the call.
     */
    Timer startSubTimer(Enum location);
}
