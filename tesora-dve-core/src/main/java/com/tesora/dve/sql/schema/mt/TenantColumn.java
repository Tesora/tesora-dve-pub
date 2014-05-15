// OS_STATUS: public
package com.tesora.dve.sql.schema.mt;


import java.util.Collections;

import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.modifiers.TypeModifier;
import com.tesora.dve.sql.schema.modifiers.TypeModifierKind;
import com.tesora.dve.sql.schema.types.BasicType;
import com.tesora.dve.sql.schema.types.Type;

public class TenantColumn extends PEColumn {

	public static final String TENANT_COLUMN = "___mtid";
	public static final Type TENANT_COLUMN_TYPE = BasicType.buildType("int", 0,
			Collections.singletonList(new TypeModifier(TypeModifierKind.UNSIGNED))).normalize();

	private static final UnqualifiedName tenantColumnName = new UnqualifiedName("`" + TENANT_COLUMN + "`");

	public TenantColumn(SchemaContext sc) {
		super(sc,tenantColumnName,TENANT_COLUMN_TYPE,(short)0,null,null);
		makeNotNullable();
	}
	
	public TenantColumn(UserColumn us, SchemaContext sc) {
		super(us,sc,true);
	}
	
	@Override
	public boolean isTenantColumn() {
		return true;
	}
}
