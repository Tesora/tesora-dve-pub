// OS_STATUS: public
package com.tesora.dve.sql.infoschema;

import com.tesora.dve.sql.infoschema.engine.ScopedColumnInstance;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.Type;

public abstract class SyntheticLogicalInformationSchemaColumn extends
		LogicalInformationSchemaColumn {

	public SyntheticLogicalInformationSchemaColumn(UnqualifiedName columnName,
			Type t) {
		super(columnName, t);		
	}

	public boolean matches(ColumnInstance subject) {
		return (subject.getColumn() == this);
	}
	
	public abstract LanguageNode explode(ColumnInstance subject);
	
	protected ColumnInstance buildColumnInstance(ColumnInstance subject, LogicalInformationSchemaColumn lisc) {
		if (subject instanceof ScopedColumnInstance) {
			ScopedColumnInstance sci = (ScopedColumnInstance) subject;
			return new ScopedColumnInstance(lisc,sci.getRelativeTo());
		} else {
			return new ColumnInstance(lisc,subject.getTableInstance());
		}
	}
	
	public Object getValue() {
		return null;
	}
}
