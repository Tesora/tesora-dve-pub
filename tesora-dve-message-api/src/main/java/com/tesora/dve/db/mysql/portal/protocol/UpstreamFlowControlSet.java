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
public class UpstreamFlowControlSet implements FlowControl {
    boolean paused;
    IdentityHashMap<FlowControl,FlowControl> upstreamSet = new IdentityHashMap<>();

    @Override
    public void pauseSourceStreams() {
        paused = true;
        checkFlowControlState();
    }

    @Override
    public void resumeSourceStreams() {
        paused = false;
        checkFlowControlState();
    }

    private void checkFlowControlState(){
        if (paused) {
            for (FlowControl upstream : upstreamSet.keySet())
                upstream.pauseSourceStreams();
        } else {
            for (FlowControl upstream : upstreamSet.keySet())
                upstream.resumeSourceStreams();
        }
    }

    public void registerUpstream(FlowControl control) {
        verifyLegalUpstream(control);
        boolean existing = upstreamSet.put(control,control) != null;

        if (existing)
            return;

        //get the new addition in line.
        if (paused)
            control.pauseSourceStreams();
        else
            control.resumeSourceStreams();
    }

    public void unregisterUpstream(FlowControl control) {
        //NOTE: does not pause/unpause control.
        verifyLegalUpstream(control);
        upstreamSet.remove(control);
    }

    private void verifyLegalUpstream(FlowControl control) {
        if (control == null)
            throw new NullPointerException();

        if (control == this)
            throw new IllegalArgumentException();
    }
}
