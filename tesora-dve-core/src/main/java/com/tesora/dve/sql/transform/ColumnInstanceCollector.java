// OS_STATUS: public
package com.tesora.dve.sql.transform;



import java.util.Collection;

import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.GeneralCollectingTraversal;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.util.ListSet;

public final class ColumnInstanceCollector extends GeneralCollectingTraversal {

	public static ListSet<ColumnInstance> getColumnInstances(LanguageNode ln) {
		return GeneralCollectingTraversal.collect(ln, new ColumnInstanceCollector());
	}
	
	public static ListSet<ColumnKey> getColumnKeys(LanguageNode ln) {
		return getColumnKeys(getColumnInstances(ln));
	}
	
	public static ListSet<ColumnInstance> getColumnInstances(Collection<ExpressionNode> in) {
		return GeneralCollectingTraversal.collect(in, new ColumnInstanceCollector());
	}
	
	public static ListSet<ColumnKey> getColumnKeys(ListSet<ColumnInstance> in) {
		ListSet<ColumnKey> out = new ListSet<ColumnKey>();
		for(ColumnInstance ci : in)
			out.add(ci.getColumnKey());
		return out;
	}

	public static ListSet<TableKey> getTableKeys(ListSet<ColumnInstance> in) {
		ListSet<TableKey> out = new ListSet<TableKey>();
		for(ColumnInstance ci : in)
			out.add(ci.getTableInstance().getTableKey());
		return out;
	}
	
	public static ListSet<TableKey> getTableKeys(LanguageNode ln) {
		ListSet<ColumnInstance> cols = getColumnInstances(ln);
		return getTableKeys(cols);
	}
		

	private ColumnInstanceCollector() {
		super(Order.POSTORDER, ExecStyle.ONCE);
	}

	@Override
	public boolean is(LanguageNode ln) {
		return EngineConstant.COLUMN.has(ln);
	}

}
