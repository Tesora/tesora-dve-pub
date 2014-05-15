// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.nested;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TempTableInstance;
import com.tesora.dve.sql.node.test.EdgeTest;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.util.ListSet;

/*
 * A from clause subquery is converted into a temp table, which is then pasted back into the parent query.
 */
public class HandleFromClauseSubquery extends StrategyFactory {

	@Override
	public NestingStrategy adapt(SchemaContext sc, EdgeTest location,
			DMLStatement enclosing, Subquery sq, ExpressionPath path)
			throws PEException {
		if (EngineConstant.FROMCLAUSE == location)
			return new FromClauseHandler(sq, path);
		return null;
	}

	
	protected static class FromClauseHandler extends NestingStrategy {

		public FromClauseHandler(Subquery nested, ExpressionPath pathTo) {
			super(nested, pathTo);
		}

		@Override
		public DMLStatement afterChildPlanning(PlannerContext pc, DMLStatement parent, DMLStatement preRewrites, 
				FeaturePlanner planner, List<FeatureStep> collector) throws PEException {
			final SchemaContext sc = pc.getContext();
			ExpressionPath ep = getPathWithinEnclosing();
			ProjectingStatement origChild = (ProjectingStatement) ep.apply(parent);
			// the parent is a subquery node, so we want the parent of the parent
			Subquery sq = (Subquery) origChild.getParent();
			RedistFeatureStep rfs = buildChildTempTable(pc, planner);
			setStep(rfs);
			collector.add(rfs);
			TempTable ct = rfs.getTargetTempTable();
			// now - we originally had a subquery; now we're going to build a new table instance from
			// the temp table and replace the subquery with the table instance
			TableInstance ti = new TempTableInstance(sc,ct,sq.getAlias().getUnqualified()); 
			sq.getParentEdge().set(ti);
			parent.getDerivedInfo().getLocalNestedQueries().remove(origChild);
			parent.getDerivedInfo().addLocalTable(ti.getTableKey());
			parent.getDerivedInfo().removeLocalTable(sq.getTable());
			// build a forwarding table for columns
			HashMap<PEColumn,PEColumn> columnForwarding = new HashMap<PEColumn,PEColumn>();
			List<PEColumn> origColumns = sq.getTable().getColumns(sc);
			List<PEColumn> newColumns = ct.getColumns(sc);
			if (origColumns.size() != newColumns.size())
				throw new PEException("Mismatched columns after subquery planning, had " + origColumns.size() + ", but now have " + newColumns.size());
			for(int i = 0; i < origColumns.size(); i++) {
				columnForwarding.put(origColumns.get(i), newColumns.get(i));
			}
			parent = (DMLStatement) new TableForwarder(columnForwarding,sq.getTable(),ti).traverse(parent);
			return parent;
		}
		
		protected RedistFeatureStep buildChildTempTable(PlannerContext pc, FeaturePlanner planner) throws PEException {
			ProjectingFeatureStep pfs = (ProjectingFeatureStep) planned;
			ProjectingStatement nss = (ProjectingStatement) pfs.getPlannedStatement();
			PEStorageGroup tg = pc.getTempGroupManager().getGroup(pfs.getCost().getGroupScore());

			List<ExpressionNode> proj = nss.getProjections().get(0);     
			ListSet<PEColumn> projColumns = new ListSet<PEColumn>();
			for(ExpressionNode en : proj) {
				ExpressionNode targ = ExpressionUtils.getTarget(en);
				if (targ instanceof ColumnInstance) {
					ColumnInstance ci = (ColumnInstance) targ;
					projColumns.add(ci.getPEColumn());
				}
			}
			
			List<Integer> distKey = new ArrayList<Integer>(); 
			DistributionVector dv = EngineConstant.BROADEST_DISTRIBUTION_VECTOR.getValue(nss, pc.getContext()); 
			if (dv != null && dv.getModel() == Model.STATIC) { 
				for(PEColumn col : dv.getColumns(pc.getContext())) {
					if (projColumns.contains(col))
						distKey.add(col.getPosition()); 
				} 
			} 
			if (distKey.isEmpty()) {
				for(int i = 0; i < proj.size(); i++) 
					distKey.add(i); 
			} 
			return pfs.redist(pc, 
					planner,
					new TempTableCreateOptions(Model.STATIC,tg)
						.withRowCount(pfs.getCost().getRowCount())
						.distributeOn(distKey),
					null,
					null);			
		}
		
		
	}


	private static class TableForwarder extends Traversal {

		private Map<PEColumn, PEColumn> columnForwarding;
		private PEAbstractTable<?> sqt;
		private TableInstance nti;
		
		public TableForwarder(Map<PEColumn,PEColumn> forwarding, PEAbstractTable<?> origTable, TableInstance nti) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			columnForwarding = forwarding;
			sqt = origTable;
			this.nti = nti;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) in;
				PEColumn nc = columnForwarding.get(ci.getColumn());
				if (nc != null) {
					if (ci.getTableInstance().getTable() == sqt) {
						return new ColumnInstance(nc,nti);
					}
				}
			}
			return in;
		}
		
	}


	
}
