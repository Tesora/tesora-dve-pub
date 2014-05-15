// OS_STATUS: public
package com.tesora.dve.sql.infoschema.engine;

import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.transform.CopyContext;

// specifically for handling hibernate path expressions, i.e. ut.database.name
public class ScopedColumnInstance extends ColumnInstance {

	private ColumnInstance relativeTo;
	
	public ScopedColumnInstance(Column<?> c, ColumnInstance relativeTo) {
		super(c, relativeTo.getTableInstance());
		this.relativeTo = relativeTo;
	}

	protected ScopedColumnInstance(ScopedColumnInstance oth) {
		this(oth.getColumn(),oth.relativeTo);
	}
	
	
	public ColumnInstance getRelativeTo() {
		return relativeTo;
	}

	@Override
	protected ColumnInstance copySelf(CopyContext cc) {
		if (cc == null) 
			return new ScopedColumnInstance(this);
		throw new InformationSchemaException("ScopedColumnInstance cannot be copied with copy context");
	}

	
}
