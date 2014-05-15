// OS_STATUS: public
package com.tesora.dve.sql.infoschema.logical.catalog;


import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class CatalogInformationSchemaTable extends LogicalInformationSchemaTable {

	// hibernate entity class name
	protected Class<?> entityClass;
	protected String entityName;
	protected String tableName;
	
	protected InfoSchemaTable config;
	
	public CatalogInformationSchemaTable(Class<?> entKlass, InfoSchemaTable anno, String catTabName) {
		super(new UnqualifiedName(anno.logicalName()));
		entityClass = entKlass;
		entityName = entKlass.getSimpleName();
		tableName = catTabName;
		config = anno;
	}

	@Override
	public String toString() {
		return "CatalogInformationSchemaTable{logicalName=" + getName().getSQL() + ", entity=" + entityName + ", table=" + tableName + "}";
	}

	@Override
	public Class<?> getEntityClass() {
		return entityClass;
	}
	
	@Override
	public String getEntityName() {
		return entityName;
	}

	@Override
	public String getTableName() {
		return tableName;
	}	
}
