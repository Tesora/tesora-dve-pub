// OS_STATUS: public
package com.tesora.dve.sql.schema.validate;

import java.util.Map;

import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class ForeignKeyValidateResult extends ValidateResult {

	public enum FKValidateKind {
		NO_UNIQUE_KEY,
		NOT_COLOCATED 
	}
	
	PEForeignKey subject;
	PETable targTab;
	boolean error;
	FKValidateKind kind;
	
	// used in mt - we use this to map mangled names into scope names
	Map<PETable, UnqualifiedName> mapping;
	
	public ForeignKeyValidateResult(SchemaContext sc, PEForeignKey pefk, FKValidateKind variety, boolean error) {
		this.subject = pefk;
		this.targTab = pefk.getTargetTable(sc);
		this.error = error;
		this.kind = variety;
		this.mapping = null;
	}
		
	// have to use a target name rather than a scope as the scope may not exist yet
	public void setMTMapping(Map<PETable,UnqualifiedName> visible) {
		mapping = visible;
	}
	
	@Override
	public boolean isError() {
		return error;
	}
	
	private String getTableName(PETable tab) {
		if (mapping != null) {
			UnqualifiedName any = mapping.get(tab);
			if (any != null) 
				return any.getUnquotedName().get();
		}
		return tab.getName().getUnqualified().getUnquotedName().get();
	}
	
	@Override
	public String getMessage(SchemaContext sc) {
		if (this.kind == FKValidateKind.NO_UNIQUE_KEY) {
			String fmt = "Invalid foreign key in table %s: no matching unique key in table %s";
			return String.format(fmt, getTableName(subject.getTable(sc)), getTableName(subject.getTargetTable(sc)));
		} else {
			String fmt = "Invalid foreign key %s.%s: table %s is not colocated with %s";
			String encName = getTableName(subject.getTable(sc));
			return String.format(fmt, encName, subject.getSymbol().get(), encName, getTableName(subject.getTargetTable(sc)));
		}
	}
	
	@Override
	public Persistable<?, ?> getSubject() {
		return subject;
	}

	
}
