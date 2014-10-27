package com.tesora.dve.db.mysql.portal.protocol;

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

import java.util.IdentityHashMap;

/**
 *
 */
public class DownstreamFlowControlSet implements CanFlowControl {
    IdentityHashMap<CanFlowControl,CanFlowControl> registeredSet = new IdentityHashMap<>();
    IdentityHashMap<CanFlowControl,CanFlowControl> blockedSet = new IdentityHashMap<>();
    FlowControl upstreamControl = FlowControl.NOOP;

    @Override
    public void setUpstreamControl(FlowControl control) {
        verifyLegalUpstream(control);
        //NOTE: does not pause/upause any previous upstream control.
        this.upstreamControl = control;
        checkFlowControlState();
    }

    public void register(CanFlowControl control){
        verifyLegalDownstream(control);

        boolean alreadyExisted = registeredSet.put(control,control) != null;
        if (!alreadyExisted) {
            SiteFlowControl handle = new SiteFlowControl(control);
            control.setUpstreamControl(handle);
            checkFlowControlState();
        }
    }

    public void unregister(CanFlowControl control){
        verifyLegalDownstream(control);

        if (registeredSet.containsKey(control)) {
            control.setUpstreamControl(FlowControl.NOOP);
            registeredSet.remove(control); //dupe effort, but control might have called back into us on setUpstreamControl.
            blockedSet.remove(control);
            checkFlowControlState();
        }
    }

    private void checkFlowControlState(){
        if (blockedSet.isEmpty()) {
            upstreamControl.resumeSourceStreams();
        } else {
            upstreamControl.pauseSourceStreams();
        }
    }

    private void verifyLegalDownstream(CanFlowControl control) {
        if (control == null)
            throw new NullPointerException();

        if (control == this)
            throw new IllegalArgumentException();
    }

    private void verifyLegalUpstream(FlowControl control) {
        if (control == null)
            throw new NullPointerException();

        if (control == this)
            throw new IllegalArgumentException();
    }

    private void pauseRequested(CanFlowControl downstream) {
        blockedSet.put(downstream,downstream);
        checkFlowControlState();
    }

    private void resumeRequested(CanFlowControl downstream) {
        blockedSet.remove(downstream);
        checkFlowControlState();
    }

    private class SiteFlowControl implements FlowControl {
        CanFlowControl downstream;

        public SiteFlowControl(CanFlowControl downstream) {
            this.downstream = downstream;
        }

        @Override
        public void pauseSourceStreams() {
            pauseRequested(downstream);
        }

        @Override
        public void resumeSourceStreams() {
            resumeRequested(downstream);
        }
    }
}
