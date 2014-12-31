package com.tesora.dve.server.global;

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

import com.fasterxml.uuid.impl.TimeBasedGenerator;
import com.tesora.dve.variables.VariableService;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 *
 */
public interface HostService {
    String getBroadcastMessageAgentAddress();

    void setBroadcastMessageAgentAddress(String broadcastMessageAgentAddress);

    String getNotificationManagerAddress();

    void setNotificationManagerAddress(String notificationManagerAddress);

    String getStatisticsManagerAddress();

    void setStatisticsManagerAddress(String statisticsManagerAddress);

    String getWorkerManagerAddress();

    Properties getProperties();

    String getName();

    String getHostName();

    String getDefaultProjectName();

    String getServerVersion();

    String getServerVersionComment();

    String getDveServerVersion();

    String getDveServerVersionComment();

    int getPortalPort(Properties props);

    long getDBConnectionTimeout();

    TimeBasedGenerator getUuidGenerator();

    long getServerUptime();

    void resetServerUptime();

    <T> Future<T> submit(Callable<T> task);

    <T> Future<T> submit(String name, Callable<T> task);

    void execute(Runnable task);

    void execute(String name, Runnable task);

    void onGarbageEvent();

}
