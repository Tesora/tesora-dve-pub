// OS_STATUS: public
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.distribution.RandomDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.DBHelperProvider;
import com.tesora.dve.persist.InsertEngine;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.infoschema.InformationSchemas;

// the info schema upgrade is specifically concerned with repopulating the catalog rows representing
// our own info schema.  we always delete the old representation before adding the new representation.
public class InfoSchemaUpgrader {

	public static void upgradeInfoSchema(DBHelper helper, DBNative dbn) throws PEException {
		int groupID = getInfoSchemaGroupID(helper);
		int modelID = getID(helper,
				"select id from distribution_model where name = '" + RandomDistributionModel.MODEL_NAME + "'");
		clearCurrentInfoSchema(helper,groupID);
		installNewInfoSchema(helper,groupID, modelID, dbn);
	}
	
	public static int getInfoSchemaGroupID(DBHelper helper) throws PEException {
		return getID(helper,
				"select persistent_group_id from persistent_group where name = '" + PEConstants.INFORMATION_SCHEMA_GROUP_NAME + "'");
	}
	
	public static int getID(DBHelper helper, String sql) throws PEException {
		try {
			ResultSet rs = null;
			try {
				helper.executeQuery(sql);
				rs = helper.getResultSet();
				if (rs.next())
					return rs.getInt(1);
				return -1;
			} finally {
				if (rs != null)
					rs.close();
			}
		} catch (SQLException sqle) {
			throw new PEException("Unable to execute: '" + sql + "'",sqle);
		}
	}
	
	public static void clearCurrentInfoSchema(DBHelper helper, int groupid) throws PEException {
		try {
			helper.executeQuery("delete uc from user_column uc inner join user_table ut on uc.user_table_id = ut.table_id where ut.persistent_group_id = " + groupid);
			helper.executeQuery("delete from user_table where persistent_group_id = " + groupid);
			helper.executeQuery("delete from user_database where default_group_id = " + groupid);
		} catch (SQLException sqle) {
			throw new PEException("Unable to drop info schema",sqle);
		}
	}
	
	private static void installNewInfoSchema(DBHelper helper, int groupid, int modelid, DBNative dbn) throws PEException {
		InformationSchemas is = InformationSchemas.build(dbn);
		List<PersistedEntity> ents = is.buildEntities(groupid, modelid,
				dbn.getDefaultServerCharacterSet(),
				dbn.getDefaultServerCollation());
		InsertEngine ie = new InsertEngine(ents, new DBHelperProvider(helper));
		ie.populate();
	}
}
