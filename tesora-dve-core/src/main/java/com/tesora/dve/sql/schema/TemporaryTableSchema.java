package com.tesora.dve.sql.schema;

import java.util.Collection;
import java.util.HashMap;

import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.node.expression.TableInstance;

// Temporary tables have wierd lookup rules.  They appear to be:
// + a temporary table always hides a persistent table
// + always schema extend unqualified names with the current db
public class TemporaryTableSchema implements Schema<ComplexPETable> {

	final HashMap<QualifiedName, ComplexPETable> tables = new HashMap<QualifiedName, ComplexPETable>();
	
	private static final UnqualifiedName unnamed = new UnqualifiedName("");
	
	@Override
	public ComplexPETable addTable(SchemaContext sc, ComplexPETable t) {
		Name encName = t.getDatabaseName(sc);
		QualifiedName lookupName = buildLookupName(encName,t.getName());
		return tables.put(lookupName, t);
	}

	public void removeTable(SchemaContext sc, ComplexPETable t) {
		Name encName = t.getDatabaseName(sc);
		QualifiedName lookupName = buildLookupName(encName,t.getName());
		tables.remove(lookupName);
	}
	
	private static QualifiedName buildLookupName(Name enc, Name tname) {
		return new QualifiedName(enc.getUnquotedName().getUnqualified(),
				tname.getUnquotedName().getUnqualified());
	}
	
	@Override
	public Collection<ComplexPETable> getTables(SchemaContext sc) {
		return tables.values();
	}

	public TableInstance buildInstance(SchemaContext sc, Name n) {
		QualifiedName ln = null;
		if (n.isQualified()) {
			QualifiedName qn = (QualifiedName) n;
			ln = buildLookupName(qn.getNamespace(),qn.getUnqualified());
		} else {
			Database<?> db = sc.getCurrentDatabase(false);
			if (db == null)
				return null;
			ln = buildLookupName(db.getName(),n);
		}
		ComplexPETable matching = tables.get(ln);
		if (matching != null)
			return new TableInstance(matching,sc.getOptions().isResolve());
		return null;
	}
	
	@Override
	public TableInstance buildInstance(SchemaContext sc, UnqualifiedName n,
			LockInfo lockType, boolean domtchecks) {
		throw new SchemaException(Pass.SECOND, "Invalid lookup method for temporary tables");
	}

	@Override
	public TableInstance buildInstance(SchemaContext sc, UnqualifiedName n,
			LockInfo lockType) {
		throw new SchemaException(Pass.SECOND, "Invalid lookup method for temporary tables");
	}

	@Override
	public UnqualifiedName getSchemaName(SchemaContext sc) {
		return unnamed;
	}

}
