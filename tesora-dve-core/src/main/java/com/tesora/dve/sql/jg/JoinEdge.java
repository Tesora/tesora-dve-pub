// OS_STATUS: public
package com.tesora.dve.sql.jg;


import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.BinaryProcedure;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;

public class JoinEdge extends DGEdge<DPart> {
	
	DGJoin join;
	
	// only equijoins for now
	private ListOfPairs<ExpressionNode, ExpressionNode> joinCondition =
		new ListOfPairs<ExpressionNode, ExpressionNode>();
	
	// key matches on each side, used during costing.  we prefer whole matches.
	private Pair<Boolean, PEKey> leftKey;
	private Pair<Boolean, PEKey> rightKey;
	
	Map<PEColumn, PEColumn> mapping = null;

	private Boolean colocated = null;

	private TableKey lhs;
	private TableKey rhs;
	
	
	public JoinEdge(int id,DunPart lhs, DunPart rhs, DGJoin theJoin) {
		super(lhs,rhs,id);
		from = lhs;
		to = rhs;
		join = theJoin;
		this.lhs = from.getTables().get(0);
		this.rhs = to.getTables().get(0);
		join.addEdge(this);
	}
	
	public TableKey getLHSTab() {
		return lhs;
	}
	
	public TableKey getRHSTab() {
		return rhs;
	}
	
	public DGJoin getJoin() {
		return join;
	}
	
	public boolean isInnerJoin() {
		return join.isInnerJoin();
	}
	
	public JoinSpecification getJoinType() {
		return join.getJoinType();
	}
	
	public boolean isColocated(SchemaContext sc) {
		if (colocated == null) 
			colocated = Boolean.valueOf(computeColocated(sc));
		return colocated.booleanValue();
	}
	
	public void addJoinExpression(ExpressionNode lhs, ExpressionNode rhs) {
		joinCondition.add(lhs,rhs);
	}
		
	public void setFrom(DPart dp) {
		from = dp;
	}
	
	public void setTo(DPart dp) {
		to = dp;
	}
	
	public static boolean computeColocated(SchemaContext sc, 
			DPart leftPartition, Collection<TableKey> left,
			DPart rightPartition, TableKey right,
			Map<PEColumn,PEColumn> mapping, JoinedTable enclosingJoin, boolean isFKTest) {
		PEAbstractTable<?> rtab = right.getAbstractTable();
		HashSet<PEStorageGroup> leftGroups = new HashSet<PEStorageGroup>();
		int nbcast = 0;
		int nojbcast = 0;
		int nten = 0;

		ListSet<PEColumn> leftCols = new ListSet<PEColumn>();
		ListSet<PEColumn> rightCols = new ListSet<PEColumn>();
		leftCols.addAll(mapping.keySet());
		rightCols.addAll(mapping.values());

		DistributionVector rvect = rightPartition.getGoverningVector(sc, right);
		
		for(TableKey tk : left) {
			boolean skipReqdColumnsCheck = false;
			DistributionVector lvect = leftPartition.getGoverningVector(sc, tk);
			PEAbstractTable<?> ltab = tk.getAbstractTable();
			if (leftGroups.add(ltab.getStorageGroup(sc))) {
				if (!ltab.getStorageGroup(sc).isSubsetOf(sc, rtab.getStorageGroup(sc)))
					return false;
			}
			if (!lvect.comparableForDistribution(sc, rvect, mapping, isFKTest)) {
				return false;
			} else if (lvect.isBroadcast() && rvect.isBroadcast()) {
				nbcast++;
				skipReqdColumnsCheck = true;
			} else if (lvect.isBroadcast() || rvect.isBroadcast()) {
				// so, we might not be colocated if this is an outer join.  in that case even though we technically 
				// are colocated, we won't execute correctly because the rows from the bcast table will be repeated.
				if (enclosingJoin != null && 
						((enclosingJoin.getJoinType().isLeftOuterJoin() && lvect.isBroadcast())
								|| (enclosingJoin.getJoinType().isRightOuterJoin() && rvect.isBroadcast()))) {
					return false;
				} else if (isFKTest) {
					// early return, since fks are only between two tables
					return !lvect.isBroadcast();
				} else {
					nojbcast++;
					skipReqdColumnsCheck = true;
				}
			}
			if ((isFKTest || (sc.getPolicyContext().isSchemaTenant() || sc.getPolicyContext().isDataTenant())) 
					&& lvect.getDistributedWhollyOnTenantColumn(sc) != null &&
							rvect.getDistributedWhollyOnTenantColumn(sc) != null)
				nten++;
			else if (!skipReqdColumnsCheck && !lvect.hasJoinRequiredColumns(sc,leftCols))
				return false;
		}
		if (nbcast == left.size() || nojbcast == left.size() || nten == left.size())
			return true;
		
		if (!rvect.hasJoinRequiredColumns(sc, rightCols))
			return false;
		
		return true;		
	}
	
	public boolean isSimpleJoin() {
		for(Pair<ExpressionNode, ExpressionNode> p : joinCondition) {
			if (p.getFirst() instanceof ColumnInstance && p.getSecond() instanceof ColumnInstance)
				continue;
			return false;
		}
		return true;
	}
	
	public ListOfPairs<ColumnInstance,ColumnInstance> getSimpleColumns() {
		ListOfPairs<ColumnInstance, ColumnInstance> out = new ListOfPairs<ColumnInstance, ColumnInstance>();
		for(Pair<ExpressionNode,ExpressionNode> c : joinCondition) {
			if (c.getFirst() instanceof ColumnInstance && c.getSecond() instanceof ColumnInstance)
				out.add((ColumnInstance)c.getFirst(), (ColumnInstance)c.getSecond());
		}
		return out;
	}
	
	private boolean computeColocated(SchemaContext sc) {
		ListSet<TableKey> leftKeys = new ListSet<TableKey>();
		leftKeys.addAll(join.getLeftTables());
		TableKey right = join.getRightTable();
		mapping = new HashMap<PEColumn, PEColumn>();
		for(Pair<ColumnInstance, ColumnInstance> p : getSimpleColumns()) {
			mapping.put(p.getFirst().getPEColumn(), p.getSecond().getPEColumn());
		}
 		return computeColocated(sc,getFrom(),leftKeys,getTo(),right,mapping,join.enclosingJoin,false);
	}

	List<ExpressionNode> getRedistJoinExpressions(final TableKey forTable) {
		// using the joinCondition will guarantee that the dv is the same for both - same ordering of values
		return Functional.apply(joinCondition, new UnaryFunction<ExpressionNode, Pair<ExpressionNode, ExpressionNode>>() {

			@Override
			public ExpressionNode evaluate(Pair<ExpressionNode, ExpressionNode> object) {
				if (getFrom().getTables().contains(forTable))
					return object.getFirst();
				else
					return object.getSecond();
			}
			
		});
		
	}
	
	public PEKey getLeftKey(SchemaContext sc) {
		if (leftKey == null)
			leftKey = computeBestKey(sc, new UnaryFunction<PEColumn,Pair<ColumnInstance,ColumnInstance>>(){

				@Override
				public PEColumn evaluate(
						Pair<ColumnInstance, ColumnInstance> object) {
					return object.getFirst().getPEColumn();
				}
				
			});
		if (leftKey.getFirst().booleanValue())
			return leftKey.getSecond();
		return null;
	}

	public PEKey getRightKey(SchemaContext sc) {
		if (rightKey == null) 
			rightKey = computeBestKey(sc, new UnaryFunction<PEColumn,Pair<ColumnInstance,ColumnInstance>>() {

				@Override
				public PEColumn evaluate(
						Pair<ColumnInstance, ColumnInstance> object) {
					return object.getSecond().getPEColumn();
				}
				
			});
		if (rightKey.getFirst().booleanValue())
			return rightKey.getSecond();
		return null;
	}
	
	private Pair<Boolean,PEKey> computeBestKey(SchemaContext sc, UnaryFunction<PEColumn,Pair<ColumnInstance,ColumnInstance>> sideFilter) {
		List<PEColumn> cols = Functional.apply(getSimpleColumns(),sideFilter);
		ListSet<PEKey> match = buildMatching(sc, cols);
		PEKey uniq = null;
		PEKey index = null;
		for(PEKey p : match) {
			if (uniq == null && (p.isUnique()))
				uniq = p;
			else if (!p.isUnique()) {
				if (index == null) index = p;
				else if (p.getCardRatio(sc) < index.getCardRatio(sc)) index = p;
			}
		}
		if (uniq != null) 
			return new Pair<Boolean,PEKey>(true,uniq);
		if (index != null)
			return new Pair<Boolean,PEKey>(true,index);
		return new Pair<Boolean,PEKey>(false, null);
	}
	

	// return keys that wholly match one or more of the columns
	private ListSet<PEKey> buildMatching(SchemaContext sc, List<PEColumn> cols) {
		ListSet<PEKey> candidates = new ListSet<PEKey>();
		for(PEColumn pec : cols) {
			List<PEKey> refs = pec.getReferencedBy(sc);
			for(PEKey pek : refs) {
				HashSet<PEColumn> uses = new HashSet<PEColumn>(pek.getColumns(sc));
				uses.removeAll(cols);
				if (uses.isEmpty()) 
					candidates.add(pek);
			}
		}
		return candidates;
	}

	@Override
	protected void describeInternal(SchemaContext sc, String indent, StringBuilder buf) {
		join.describe(sc, buf);
		buf.append(" [").append(from.getGraphID()).append(",").append(to.getGraphID()).append("] on ");
		Functional.join(joinCondition, buf, ", ", new BinaryProcedure<Pair<ExpressionNode,ExpressionNode>,StringBuilder>() {

			@Override
			public void execute(Pair<ExpressionNode, ExpressionNode> aobj,
					StringBuilder bobj) {
				bobj.append(aobj.getFirst().toString()).append("=").append(aobj.getSecond().toString());
			}
			
		});
	}

	@Override
	public String getGraphRole() {
		return "Join";
	}

	public boolean isSame(JoinEdge other) {
		boolean ret = false;

		if (!this.lhs.equals(other.lhs) || !this.rhs.equals(other.rhs)) {
			return ret;
		}
		
		for(Pair<ExpressionNode, ExpressionNode> thisPair : this.joinCondition) {
			ret = false;
			for(Pair<ExpressionNode, ExpressionNode> otherPair : other.joinCondition) {
				if (thisPair.getFirst().getRewriteKey().equals(otherPair.getFirst().getRewriteKey()) &&
					thisPair.getSecond().getRewriteKey().equals(otherPair.getSecond().getRewriteKey())) {
					ret = true;
					continue;
				}
			}
			
			if (!ret) { 
				break;
			}
		}
		
		return ret;
	}	
}
