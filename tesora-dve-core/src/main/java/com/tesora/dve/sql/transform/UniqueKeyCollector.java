// OS_STATUS: public
package com.tesora.dve.sql.transform;

import java.util.Collection;
import java.util.List;

import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ListSet;

public class UniqueKeyCollector extends KeyCollector {

	private final SchemaContext context;
	private ListSet<Part> parts;
	private boolean analyzed;
	
	public UniqueKeyCollector(SchemaContext sc, LanguageNode ln) {
		super(sc,ln);
		context = sc;
		parts = null;
		analyzed = false;
	}
	
	public UniqueKeyCollector(SchemaContext sc, Edge<?,?> e) {
		super(sc,e);
		context = sc;
		parts = null;
		analyzed = false;
	}
	
	private void analyze() {
		if (analyzed) return;
		parts = getParts();
		analyzed = true;
	}
	
	public Integer getPKRows() {
		analyze();
		if (parts == null) return null;
		int counter = 0;
		for(Part p : parts) {
			if (p instanceof OredParts) {
				OredParts op = (OredParts) p;
				counter += op.getParts().size();
			} else {
				counter += 1;
			}
		}
		return (counter == 0 ? null : counter);
	}
	
	@Override
	protected boolean isQualifyingColumn(ColumnInstance ci) {
		return isQualifyingColumn(ci.getPEColumn());
	}

	@Override
	protected boolean isQualifyingColumn(PEColumn c) {
		return c.isPrimaryKeyPart();
	}

	@Override
	protected boolean isComplete(EqualityPart sp) {
		PEKey pk = sp.getTableKey().getAbstractTable().asTable().getPrimaryKey(context);
		return isSame(sp.getColumns(), pk.getColumns(context));
	}

	@Override
	protected boolean isComplete(AndedParts cp) {
		PEKey pk = cp.getTableKey().getAbstractTable().asTable().getPrimaryKey(context);
		return isSame(cp.getColumns(),pk.getColumns(context));
	}

	@Override
	protected AndedParts maybeMakeComplete(EqualityPart sp) {
		return maybeMakeComplete(sp,context.getPolicyContext().getTenantIDLiteral(false));
	}

	@Override
	protected AndedParts maybeMakeComplete(AndedParts cp) {
		return maybeMakeComplete(cp,context.getPolicyContext().getTenantIDLiteral(false));
	}

	@Override
	protected List<ColumnKey> getNeeded(Collection<ColumnKey> in) {
		TableKey tk = in.iterator().next().getTableKey();
		PEKey pk = tk.getAbstractTable().asTable().getPrimaryKey(context);
		ListSet<ColumnKey> pkcols = buildKeyColumns(tk, pk.getColumns(context)); 
		pkcols.removeAll(in);
		return pkcols;
	}

}
