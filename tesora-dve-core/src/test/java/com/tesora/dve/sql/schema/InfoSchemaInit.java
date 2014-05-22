package com.tesora.dve.sql.schema;

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

import java.util.List;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.junit.Test;

import com.tesora.dve.persist.InsertEngine;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.InformationSchemas;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.transform.TransformTest;

public class InfoSchemaInit extends TransformTest {

	public InfoSchemaInit() {
		super("InfoSchemaInit");
		// TODO Auto-generated constructor stub
	}

	@Test
	public void testLogical() {
        InformationSchemas schema = Singletons.require(HostService.class).getInformationSchema();
		System.out.println("Logical tables:");
		for(LogicalInformationSchemaTable list : schema.getLogical().getTables(null)) {
			System.out.println(list);
			for(LogicalInformationSchemaColumn isc : list.getColumns(null))
				System.out.println("   " + isc);
		}
	}
	
	@Test
	public void testShow() {
        InformationSchemas schema = Singletons.require(HostService.class).getInformationSchema();
		System.out.println("Show tables:");
		for(InformationSchemaTableView list : schema.getShowSchema().getTables(null)) {
			System.out.println(list);
			for(AbstractInformationSchemaColumnView isc : list.getColumns(null))
				System.out.println("   " + isc);
		}		
	}
	
	@Test
	public void testInfoSchema() {
        InformationSchemas schema = Singletons.require(HostService.class).getInformationSchema();
		System.out.println("Info schema tables:");
		for(InformationSchemaTableView list : schema.getInfoSchema().getTables(null)) {
			System.out.println(list);
			for(AbstractInformationSchemaColumnView isc : list.getColumns(null))
				System.out.println("   " + isc);
		}				
	}

	@Test
	public void testMysqlSchema() {
        InformationSchemas schema = Singletons.require(HostService.class).getInformationSchema();
		System.out.println("Mysql tables:");
		for(InformationSchemaTableView list : schema.getMysqlSchema().getTables(null)) {
			System.out.println(list);
			for(AbstractInformationSchemaColumnView isc : list.getColumns(null))
				System.out.println("   " + isc);
		}		
	}
	
	@Test
	public void testGen() throws Throwable {
        InformationSchemas schema = Singletons.require(HostService.class).getInformationSchema();
		List<PersistedEntity> ents = schema.buildEntities(1, 2, "mycharset", "mycollation");
		InsertEngine ie = new InsertEngine(ents,null);
		List<String> gen = ie.dryrun();
		for(String s : gen) {
			System.out.println(s + ";");
		}
		
	}
	
}
