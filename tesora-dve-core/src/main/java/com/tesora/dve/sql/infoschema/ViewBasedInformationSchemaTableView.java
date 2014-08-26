package com.tesora.dve.sql.infoschema;

import java.util.List;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.infoschema.persist.CatalogColumnEntity;
import com.tesora.dve.sql.infoschema.persist.CatalogDatabaseEntity;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.infoschema.persist.CatalogTableEntity;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEViewTable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class ViewBasedInformationSchemaTableView implements InformationSchemaTableView {

	// we could maybe delegate from the backing PEViewTable here
	// and then have something that when we build a table instance - we route it through the istv to build the
	// actual instance - that would work

	private final PEViewTable backing;
	private final boolean privileged;
	private final boolean extension;
	private final InfoView view;
	
	private final Column orderByColumn;
	private final Column identColumn;
	
	public ViewBasedInformationSchemaTableView(SchemaContext sc, InfoView view, PEViewTable viewTab,
			boolean privileged, boolean extension,
			String orderByColumn, String identColumn) {
		this.backing = viewTab;
		this.privileged = privileged;
		this.extension = extension;
		this.view = view;
		this.orderByColumn = (orderByColumn == null ? null : viewTab.lookup(sc, orderByColumn));
		this.identColumn = (identColumn == null ? null : viewTab.lookup(sc,identColumn));
	}
	
	@Override
	public Column addColumn(SchemaContext sc, Column c) {
		throw new InformationSchemaException("Illegal addColumn call");
	}

	@Override
	public List getColumns(SchemaContext sc) {
		return backing.getColumns(sc);
	}

	@Override
	public Column lookup(SchemaContext sc, Name n) {
		return backing.lookup(sc, n);
	}

	@Override
	public Name getName(SchemaContext sc) {
		return backing.getName(sc);
	}

	@Override
	public boolean isInfoSchema() {
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Database<?> getDatabase(SchemaContext sc) {
		return ((Database<?>)sc.getSource().find(sc, view.getCacheKey()));
	}

	
	@Override
	public Name getName() {
		return backing.getName();
	}

	@Override
	public InfoView getView() {
		return view;
	}

	@Override
	public void prepare(SchemaView view, DBNative dbn) {
		// does nothing
	}

	@Override
	public void inject(SchemaView view, DBNative dbn) {
		// does nothing
	}

	@Override
	public void freeze() {
		// already frozen
	}

	@Override
	public LogicalInformationSchemaTable getLogicalTable() {
		// no backing table
		return null;
	}

	@Override
	public void buildTableEntity(CatalogSchema cs, CatalogDatabaseEntity db,
			int dmid, int storageid, List<PersistedEntity> acc)
			throws PEException {
		buildTableEntity(cs,db,dmid,storageid,acc,backing);
	}

	@Override
	public Column getOrderByColumn() {
		return orderByColumn;
	}

	@Override
	public Column getIdentColumn() {
		return identColumn;
	}

	@Override
	public boolean requiresPriviledge() {
		return privileged;
	}

	@Override
	public boolean isExtension() {
		return extension;
	}

	public static void buildTableEntity(CatalogSchema cs, CatalogDatabaseEntity db, int dmid, int storageid, 
			List<PersistedEntity> acc, PEAbstractTable<?> table) throws PEException {
		CatalogTableEntity cte = new CatalogTableEntity(cs,db,table.getName().get(),dmid,storageid,"MEMORY");
		acc.add(cte);
		int counter = 0;
		for(PEColumn pec : table.getColumns(null)) {
			CatalogColumnEntity cce = new CatalogColumnEntity(cs,cte);
			cce.setName(pec.getName().get());
			cce.setNullable(pec.isNullable());
			cce.setType(pec.getType());
			cce.setPosition(counter);
			acc.add(cce);
		}
	}

	@Override
	public void annotate(SchemaContext sc, ViewQuery vq, SelectStatement in,
			TableKey onTK) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isVariablesTable() {
		// TODO Auto-generated method stub
		return false;
	}
	
	public void assertPermissions(SchemaContext sc) {
		if (!privileged) return;
		if (!sc.getPolicyContext().isRoot())
			throw new InformationSchemaException("You do not have permissions to query " + getName().get());	
	}

	@Override
	public boolean isView() {
		return true;
	}

	
}
