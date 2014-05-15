// OS_STATUS: public
package com.tesora.dve.sql.transform;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.TransformKey.TransformKeySeries;
import com.tesora.dve.sql.transform.TransformKey.TransformKeySimple;
import com.tesora.dve.sql.util.ListSet;

public class DistKeyCollector extends KeyCollector {

	private final SchemaContext context;
	private final Collection<TransformKeySimple> singles;
	private final Collection<TransformKeySeries> series;
	private ListSet<Part> parts;
	private boolean analyzed;
	
	public DistKeyCollector(SchemaContext sc, Edge<?,?> containingEdge) {
		super(sc,containingEdge);
		context = sc;
		singles = new ListSet<TransformKeySimple>();
		series = new ListSet<TransformKeySeries>();
		parts = null;
		analyzed = false;
	}

	private void analyze() {
		if (analyzed) return;
		parts = getParts();
		analyzed = true;
		if (parts == null) return;
		for(Part p : parts) {
			TransformKey converted = convert(p);
			if (converted instanceof TransformKeySeries) {
				series.add((TransformKeySeries)converted);
			} else if (converted instanceof TransformKeySimple) {
				singles.add((TransformKeySimple)converted);
			}
		}
	}
	
	private TransformKey convert(Part p) {
		if (p instanceof EqualityPart) {
			return convertEqualityPart((EqualityPart)p);
		} else if (p instanceof AndedParts) {
			return convertAndedPart((AndedParts)p);
		} else if (p instanceof OredParts) {
			return convertOredPart((OredParts)p);
		} else {
			throw new SchemaException(Pass.PLANNER, "Unknown kind of part");
		}
	}
	
	private TransformKeySimple convertEqualityPart(EqualityPart ep) {
		return new TransformKeySimple((ExpressionNode)ep.getParent(), ep.getColumn(), ep.getLiteral());
	}
	
	private TransformKeySimple convertAndedPart(AndedParts ap) {
		ArrayList<TransformKeySimple> bits = new ArrayList<TransformKeySimple>();
		for(Part p : ap.getParts()) {
			bits.add((TransformKeySimple)convert(p));
		}
		return new TransformKeySimple((ExpressionNode)ap.getParent(),bits);
	}

	private TransformKeySeries convertOredPart(OredParts op) {
		ArrayList<TransformKeySimple> bits = new ArrayList<TransformKeySimple>();
		for(Part p : op.getParts()) {
			TransformKey tk = convert(p);
			if (tk instanceof TransformKeySimple) {
				bits.add((TransformKeySimple)convert(p));
			} else if (tk instanceof TransformKeySeries) {
				bits.addAll(((TransformKeySeries)tk).getKeySource());
			}
		}
		return new TransformKeySeries((ExpressionNode)op.getParent(), bits);
	}
	
	public Collection<TransformKeySimple> getSingles() {
		analyze();
		return singles;
	}
	
	public Collection<TransformKeySeries> getSeries() {
		analyze();
		return series;
	}
	
	@Override
	protected boolean isQualifyingColumn(ColumnInstance ci) {
		return isQualifyingColumn(ci.getPEColumn());
	}

	@Override
	protected boolean isQualifyingColumn(PEColumn c) {
		return c.isPartOfDistributionVector();
	}

	@Override
	protected boolean isComplete(EqualityPart sp) {
		return sp.getColumn().getPEColumn().isCompleteDistributionVector(context);
	}

	@Override
	protected boolean isComplete(AndedParts cp) {
		PEAbstractTable<?> tab = cp.getTableKey().getAbstractTable();
		DistributionVector dvect = tab.getDistributionVector(context);
		return isSame(cp.getColumns(), dvect.getColumns(context));
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
		DistributionVector dvect = tk.getAbstractTable().getDistributionVector(context);
		ListSet<ColumnKey> dvck = new ListSet<ColumnKey>();
		for(PEColumn c : dvect.getColumns(context))
			dvck.add(new ColumnKey(tk,c));
		dvck.removeAll(in);
		return dvck;
	}
}
