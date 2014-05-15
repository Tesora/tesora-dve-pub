// OS_STATUS: public
package com.tesora.dve.sql.infoschema;

import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class CatalogInformationSchemaColumnView extends
		InformationSchemaColumnView {

	protected ColumnView anno;
	
	public CatalogInformationSchemaColumnView(ColumnView cv, LogicalInformationSchemaColumn basedOn) {
		super(cv.view(), basedOn, new UnqualifiedName(cv.name()));
		anno = cv;
	}

	public CatalogInformationSchemaColumnView(CatalogInformationSchemaColumnView ciscv) {
		super(ciscv);
		anno = ciscv.anno;
	}
	
	@Override
	public InformationSchemaColumnView copy() {
		return new CatalogInformationSchemaColumnView(this);
	}
	
	@Override
	public boolean isOrderByColumn() {
		return anno.orderBy();
	}
	
	@Override
	public boolean isIdentColumn() {
		return anno.ident();
	}
	
	@Override
	public boolean requiresPrivilege() {
		return anno.priviledged();
	}

	@Override
	public boolean isExtension() {
		return anno.extension();
	}
	
	@Override
	public boolean isVisible() {
		return anno.visible();
	}
	
	@Override
	public boolean isInjected() {
		return anno.injected();
	}
}
