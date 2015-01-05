package com.tesora.dve.upgrade;

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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.persistence.EntityManagerFactory;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.infoschema.spi.CatalogGenerator;
import com.tesora.dve.sql.infoschema.InformationSchemaService;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.InsertEngine;
import com.tesora.dve.persist.PersistedEntity;

// responsible for yanking the sql out of hibernate and adding our own modifications.  catalog sql is always
// created via this class.
public class CatalogSchemaGenerator {
	public static final CatalogGenerator GENERATOR = new CatalogGeneratorImpl();
	
	private static void installCurrentSchema(CatalogDAO c, Properties catalogProperties) throws PEException {
		String[] commands = buildCreateCurrentSchema(c, catalogProperties);
		DBHelper helper = new DBHelper(catalogProperties);
		installSchema(helper, commands);
	}
	
	public static void installSchema(DBHelper helper, String[] commands) throws PEException {
		helper.connect();
		try {
			for(int i = 0; i < commands.length; i++) try {
				helper.executeQuery(commands[i]);
			} catch (SQLException sqle) {
				throw new PEException("Unable to install current schema.  Statement '" + commands[i] + "' failed.",sqle);
			}
		} finally {
			helper.disconnect();
		}		
	}
	
	private static String[] buildCreateCurrentSchema(CatalogDAO c, Properties catalogProperties) throws PEException {
		ArrayList<String> buf = new ArrayList<String>();
		EntityManagerFactory emf = c.getEntityManager().getEntityManagerFactory();
		Configuration cfg = new Configuration();
		Metamodel model = emf.getMetamodel();
		for(EntityType<?> e : model.getEntities()) {
			cfg.addAnnotatedClass(e.getBindableJavaType());
		}
		String[] out = cfg.generateSchemaCreationScript(Dialect.getDialect(catalogProperties));
		buf.addAll(Arrays.asList(out));
		buf.addAll(Arrays.asList(getAdditionalCommands()));
		// current version table must be last - necessary for the upgraded code needed test
		buf.addAll(CatalogVersions.getCurrentVersion().buildCurrentVersionTable());
		return buf.toArray(new String[0]);
	}

	// any hand coded additions to the catalog schema.
	private static String[] getAdditionalCommands() {
		return new String[] {
				"alter table container_tenant add key `cont_ten_idx` (container_id, discriminant(80))",
				"alter table shape add unique key `unq_shape_idx` (database_id, name, typehash)",
		};
	}	
	
	public static String[] buildTestCurrentInfoSchema() throws PEException {
        InformationSchemaService info = Singletons.require(InformationSchemaService.class);
        List<PersistedEntity> ents = info.buildEntities(1001, 2002,
				Singletons.require(DBNative.class).getDefaultServerCharacterSet(),
				Singletons.require(DBNative.class).getDefaultServerCollation());
		InsertEngine ie = new InsertEngine(ents, null);
		return ie.dryrun().toArray(new String[0]);
	}

	private static class CatalogGeneratorImpl implements CatalogGenerator {

		@Override
		public String[] buildCreateCurrentSchema(CatalogDAO c, Properties catalogProperties) throws PEException {
			return CatalogSchemaGenerator.buildCreateCurrentSchema(c,catalogProperties);
		}

		@Override
		public void installCurrentSchema(CatalogDAO c, Properties catalogProperties) throws PEException {
			CatalogSchemaGenerator.installCurrentSchema(c,catalogProperties);
		}


	}
}
