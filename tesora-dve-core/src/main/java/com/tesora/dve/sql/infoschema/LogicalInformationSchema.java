// OS_STATUS: public
package com.tesora.dve.sql.infoschema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.Lookup;
import com.tesora.dve.sql.schema.Schema;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class LogicalInformationSchema implements
		Schema<LogicalInformationSchemaTable> {

	protected List<LogicalInformationSchemaTable> tables;
	protected Lookup<LogicalInformationSchemaTable> lookup;

	protected boolean frozen;
	
	public LogicalInformationSchema() {
		super();
		tables = new ArrayList<LogicalInformationSchemaTable>();
		lookup = new Lookup<LogicalInformationSchemaTable>(tables, LogicalInformationSchemaTable.getNameFunc, false, false);
		frozen = false;
	}
		
	public void freeze(DBNative dbn) {
		for(LogicalInformationSchemaTable list : tables) 
			list.prepare(this, dbn);
		for(LogicalInformationSchemaTable list : tables)
			list.inject(this);
		for(LogicalInformationSchemaTable list : tables) 
			list.freeze();
		frozen = true;
	}
	
	@Override
	public LogicalInformationSchemaTable addTable(SchemaContext sc,
			LogicalInformationSchemaTable t) {
		if (frozen)
			throw new InformationSchemaException("Information schema for logical information schema is frozen, cannot add table");
		LogicalInformationSchemaTable already = lookup.lookup(t.getName());
		if (already != null)
			return already;
		tables.add(t);
		lookup.refreshBacking(tables);
		return t;

	}

	@Override
	public Collection<LogicalInformationSchemaTable> getTables(SchemaContext sc) {
		return tables;
	}

	public LogicalInformationSchemaTable lookup(String s) {
		LogicalInformationSchemaTable list = lookup.lookup(s); 
		if (list == null)
			throw new InformationSchemaException("Unable to find logical table " + s + " in logical schema");
		return list;
	}

	@Override
	public TableInstance buildInstance(SchemaContext sc, UnqualifiedName n, LockInfo ignored, boolean domtchecks) {
		LogicalInformationSchemaTable list = lookup.lookup(n);
		if (list == null) return null;
		return new TableInstance(list, false);
	}

	@Override
	public TableInstance buildInstance(SchemaContext sc, UnqualifiedName n, LockInfo ignored) {
		LogicalInformationSchemaTable list = lookup.lookup(n);
		if (list == null) return null;
		return new TableInstance(list, false);
	}

	@Override
	public UnqualifiedName getSchemaName(SchemaContext sc) {
		return new UnqualifiedName("Internal Logical Catalog Schema");
	}
}
