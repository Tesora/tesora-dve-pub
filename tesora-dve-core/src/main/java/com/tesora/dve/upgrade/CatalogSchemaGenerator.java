// OS_STATUS: public
package com.tesora.dve.upgrade;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.persistence.EntityManagerFactory;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.InsertEngine;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.infoschema.InformationSchemas;

// responsible for yanking the sql out of hibernate and adding our own modifications.  catalog sql is always
// created via this class.
public class CatalogSchemaGenerator {
	
	public static void installCurrentSchema(CatalogDAO c, Properties catalogProperties) throws PEException {
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
	
	public static String[] buildCreateCurrentSchema(CatalogDAO c, Properties catalogProperties) throws PEException {
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
        InformationSchemas info = Singletons.require(HostService.class).getInformationSchema();
        List<PersistedEntity> ents = info.buildEntities(1001, 2002,
				Singletons.require(HostService.class).getDBNative().getDefaultServerCharacterSet(),
				Singletons.require(HostService.class).getDBNative().getDefaultServerCollation());
		InsertEngine ie = new InsertEngine(ents, null);
		return ie.dryrun().toArray(new String[0]);
	}
	
}
