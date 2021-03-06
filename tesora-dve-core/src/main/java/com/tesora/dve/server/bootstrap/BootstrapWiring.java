package com.tesora.dve.server.bootstrap;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2015 Tesora Inc.
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

import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ErrorHandlingService;
import com.tesora.dve.sql.infoschema.spi.CatalogGenerator;
import com.tesora.dve.sql.schema.ErrorHandlingServiceImpl;
import com.tesora.dve.sql.transexec.spi.TransientEngineFactory;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.upgrade.CatalogSchemaGenerator;

/**
 *
 */
public class BootstrapWiring {

    public static void rewire(){
        Singletons.replace(BehaviorConfiguration.class, new DefaultBehaviorConfiguration());
        Singletons.replace(ErrorHandlingService.class, new ErrorHandlingServiceImpl() );
        Singletons.replace(CatalogGenerator.class, CatalogSchemaGenerator.GENERATOR);
        Singletons.replace(TransientEngineFactory.class, TransientExecutionEngine.FACTORY);
    }
}
