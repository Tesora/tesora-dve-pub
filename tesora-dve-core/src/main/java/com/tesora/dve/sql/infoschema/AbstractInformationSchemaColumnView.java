// OS_STATUS: public
package com.tesora.dve.sql.infoschema;

import java.util.List;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.engine.ScopedColumnInstance;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.infoschema.persist.CatalogTableEntity;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.UnqualifiedName;

public abstract class AbstractInformationSchemaColumnView implements Column<InformationSchemaTableView> {

	protected InfoView view;
	
	protected UnqualifiedName name;
	protected InformationSchemaTableView table;
	protected int position = -1;
	
	protected InformationSchemaTableView returnType;
	protected boolean frozen;

	public AbstractInformationSchemaColumnView(InfoView view, UnqualifiedName nameInView) {
		super();
		this.view = view;
		this.name = nameInView;
		this.frozen = false;
	}
	
	protected AbstractInformationSchemaColumnView(AbstractInformationSchemaColumnView copy) {
		super();
		view = copy.view;
		name = copy.name;
		returnType = copy.returnType;
		frozen = copy.frozen;
	}
	
	public abstract AbstractInformationSchemaColumnView copy();
	
	@Override
	public InformationSchemaTableView getTable() {
		return table;
	}

	@Override
	public void setTable(InformationSchemaTableView t) {
		table = t;
	}

	protected void validate(SchemaView ofView, InformationSchemaTableView ofTable) {
		
	}
	
	public void prepare(SchemaView ofView, InformationSchemaTableView ofTable, DBNative dbn) {
	}
	
	@Override
	public Name getName() {
		return this.name;
	}

	@Override
	public boolean isTenantColumn() {
		return false;
	}

	public boolean isOrderByColumn() {
		return false;
	}
	
	public boolean isIdentColumn() {
		return false;
	}
	
	public boolean requiresPrivilege() {
		return false;
	}

	public boolean isExtension() {
		return false;
	}
	
	public boolean isVisible() {
		return true;
	}
	
	public boolean isInjected() {
		return false;
	}
	
	@Override
	public int getPosition() {
		return position;
	}
	
	public void setPosition(int v) {
		position = v;
	}

	public ColumnInstance buildNameTest(ColumnInstance ci) {
		return new ScopedColumnInstance(returnType.getIdentColumn(),ci);
	}

	public ColumnInstance buildNameTest(TableInstance in) {
		return buildNameTest(new ColumnInstance(null,this,in));
	}
		
	public InformationSchemaTableView getReturnType() {
		return returnType;
	}

	public boolean isBacked() {
		return isSynthetic() ? false : getLogicalColumn() != null;
	}
	
	public boolean isSynthetic() {
		return false;
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "{name=" + getName() + ", type=" + getType() + "}";
	}
	
	public abstract LogicalInformationSchemaColumn getLogicalColumn();
	
	public abstract void buildColumnEntity(CatalogSchema schema, CatalogTableEntity cte, int ordinal_position, List<PersistedEntity> acc) throws PEException;
	
}
