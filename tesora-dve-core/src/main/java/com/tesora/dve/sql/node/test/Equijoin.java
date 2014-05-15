// OS_STATUS: public
package com.tesora.dve.sql.node.test;

import java.util.Iterator;
import java.util.List;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.util.ListSet;

// we define an equijoin as:
// [1] it uses the equality operator
// [2] there are column references on both sides from different tables
// 
// this encompasses both simple equijoins (a.p1 = b.p1) 
// as well as the more complex form f(a.p1) = g(b.p1)
class Equijoin extends DerivedAttribute<Boolean> {

	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		if (!EngineConstant.FUNCTION.has(ln, EngineConstant.EQUALS))
			return false;
		FunctionCall fc = (FunctionCall) ln;
		List<ColumnInstance> lcols = ColumnInstanceCollector.getColumnInstances(fc.getParametersEdge().get(0));
		List<ColumnInstance> rcols = ColumnInstanceCollector.getColumnInstances(fc.getParametersEdge().get(1));
		if (lcols.isEmpty() || rcols.isEmpty()) 
			return false;
		ListSet<TableKey> ltks = new ListSet<TableKey>();
		ListSet<TableKey> rtks = new ListSet<TableKey>();
		for(ColumnInstance ci : lcols)
			ltks.add(ci.getColumnKey().getTableKey());
		for(ColumnInstance ci : rcols)
			rtks.add(ci.getColumnKey().getTableKey());
		if (ltks.size() == 1 && rtks.size() == 1) {
			if (ltks.get(0).equals(rtks.get(0)))
				// a.id = a.fid - not an equijoin
				// also f(a.id) = g(a.fid) - not an equijoin
				return false;
			return true;
		}
		for(Iterator<TableKey> iter = ltks.iterator(); iter.hasNext();) {
			TableKey tk = iter.next();
			if (rtks.contains(tk)) {
				iter.remove();
				rtks.remove(tk);
			}
		}
		for(Iterator<TableKey> iter = rtks.iterator(); iter.hasNext();) {
			TableKey tk = iter.next();
			if (ltks.contains(tk)) {
				iter.remove();
				ltks.remove(tk);
			}
		}
		return !ltks.isEmpty() && !rtks.isEmpty();
	}

	@Override
	public Boolean computeValue(SchemaContext sc, LanguageNode ln) {
		return Boolean.TRUE;
	}

}
