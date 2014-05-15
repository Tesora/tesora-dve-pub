// OS_STATUS: public
package com.tesora.dve.sql.node.test;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ListSet;

public class Dependent extends DerivedAttribute<ListSet<TableKey>> {

	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return true;
	}

	@Override
	public ListSet<TableKey> computeValue(SchemaContext sc, LanguageNode ln) {
		ListSet<TableKey> out = new ListSet<TableKey>();
		if (EngineConstant.COLUMN.has(ln)) {
			ColumnInstance ci = (ColumnInstance) ln;
			out.add(ci.getTableInstance().getTableKey());
		} else if (EngineConstant.CONSTANT.has(ln)) {
			// constants don't depend on anything
		} else if (EngineConstant.TABLE.has(ln)) {
			TableInstance ti = (TableInstance) ln;
			out.add(ti.getTableKey());
		} else {
			for(Edge<?,? extends LanguageNode> e : ln.getEdges()) {
				if (e.isMulti()) {
					MultiEdge<?, ? extends LanguageNode> multi = (MultiEdge<?, ? extends LanguageNode>) e;
					for(LanguageNode sn : multi.getMulti()) {
						out.addAll(EngineConstant.DEPENDENT.getValue(sn,sc));
					}
				} else {
					SingleEdge<?, ? extends LanguageNode> single = (SingleEdge<?, ? extends LanguageNode>) e;
					out.addAll(EngineConstant.DEPENDENT.getValue(single.get(),sc));
				}
			}
		}
		return out;
	}

}
