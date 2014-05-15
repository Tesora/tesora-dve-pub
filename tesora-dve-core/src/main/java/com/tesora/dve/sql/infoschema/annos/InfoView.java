// OS_STATUS: public
package com.tesora.dve.sql.infoschema.annos;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.DatabaseView.DatabaseViewCacheKey;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.util.UnaryFunction;

public enum InfoView {

	INFORMATION(PEConstants.INFORMATION_SCHEMA_DBNAME,InformationSchemaTableView.regularNameFunc),
	SHOW(PEConstants.SHOW_SCHEMA_DBNAME,InformationSchemaTableView.regularNameFunc),
	MYSQL(PEConstants.MYSQL_SCHEMA_DBNAME,InformationSchemaTableView.regularNameFunc);
	
	private final String persistentDBName;
	private final UnaryFunction<Name[], AbstractInformationSchemaColumnView> nameFunc;
	private final DatabaseViewCacheKey cacheKey;
	
	private InfoView(String pdbn, UnaryFunction<Name[], AbstractInformationSchemaColumnView> nf) {
		persistentDBName = pdbn;
		nameFunc = nf;
		cacheKey = new DatabaseViewCacheKey(pdbn);
	}
	
	public String getUserDatabaseName() {
		return persistentDBName;
	}
	
	public UnaryFunction<Name[], AbstractInformationSchemaColumnView> getNameFunction() {
		return nameFunc;
	}
	
	public DatabaseViewCacheKey getCacheKey() {
		return cacheKey;
	}
	
}
