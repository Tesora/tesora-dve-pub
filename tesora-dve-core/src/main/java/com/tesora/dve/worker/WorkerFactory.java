package com.tesora.dve.worker;

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

import com.tesora.dve.common.catalog.ISiteInstance;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import io.netty.channel.EventLoopGroup;

import java.util.HashMap;
import java.util.Map;

/**
*
*/
public abstract class WorkerFactory {
    abstract public Worker newWorker(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, StorageSite site, EventLoopGroup preferredEventLoop) throws PEException;
    abstract public void onSiteFailure(StorageSite site) throws PEException;
    abstract public String getInstanceIdentifier(StorageSite site, ISiteInstance instance);

    public static final String SINGLE_DIRECT_HA_TYPE = "Single";
    public static final SingleDirectFactory SINGLE_DIRECT_SINGLE_DIRECT_FACTORY = new SingleDirectFactory();

    public static final String MASTER_MASTER_HA_TYPE = "MasterMaster";
    public static final WorkerFactory MASTER_MASTER_FACTORY = new MasterMasterFactory();

    static Map<String, WorkerFactory> workerFactoryMap = new HashMap<String, WorkerFactory>() {
        private static final long serialVersionUID = 1L;
        {
            put(SINGLE_DIRECT_HA_TYPE, SINGLE_DIRECT_SINGLE_DIRECT_FACTORY);
            put(MASTER_MASTER_HA_TYPE, MASTER_MASTER_FACTORY);
        }
    };

    public static WorkerFactory getWorkerFactory(String haType) {
        WorkerFactory theFactory;
        if (workerFactoryMap.containsKey(haType))
            theFactory = workerFactoryMap.get(haType);
        else
            throw new PECodingException("Invalid ha type for worker lookup for type " + haType);
        return theFactory;
    }

    public static void registerFactory(String haType, WorkerFactory factory) {
        workerFactoryMap.put(haType, factory);
    }

    public static boolean hasFactoryFor(String haType) {
        return workerFactoryMap.containsKey(haType);
    }



    public static class MasterMasterFactory extends WorkerFactory {
        @Override
        public Worker newWorker(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, StorageSite site, EventLoopGroup preferredEventLoop)
                throws PEException {
            return new Worker(auth, additionalConnInfo, site, preferredEventLoop, Worker.AvailabilityType.MASTER_MASTER);
        }

        @Override
        public void onSiteFailure(StorageSite site) throws PEException {
            // We know a MasterMasterWorker can only work on a PersistentSite, so to
            // avoid implementing empty methods everywhere, we'll cast
            ((PersistentSite)site).ejectMaster();
        }

        @Override
        public String getInstanceIdentifier(StorageSite site, ISiteInstance instance) {
            return site.getName();
        }
    }

    public static class SingleDirectFactory extends WorkerFactory {
        @Override
        public Worker newWorker(UserAuthentication auth, AdditionalConnectionInfo additionalConnInfo, StorageSite site, EventLoopGroup preferredEventLoop)
                throws PEException {
            return new Worker(auth, additionalConnInfo, site, preferredEventLoop, Worker.AvailabilityType.SINGLE);
        }

        @Override
        public void onSiteFailure(StorageSite site) {
        }

        @Override
        public String getInstanceIdentifier(StorageSite site,
                ISiteInstance instance) {
            return site.getName();
        }
    }
}
