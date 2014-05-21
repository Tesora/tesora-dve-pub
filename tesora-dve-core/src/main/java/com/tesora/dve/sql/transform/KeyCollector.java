// OS_STATUS: public
package com.tesora.dve.sql.transform;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.node.test.EngineToken;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

// collect expressions of the following forms from a given expression tree
// a in (1,2,3)
// a = 1 or a = 2 or a = 3
// a = 1 and b = 2
// (a = 1 and b = 2) or (a = 3 and b = 4)
// must have combination of simpler exprs into more complex (i.e. be able to combine a = 1, b = 2 into a = 1 and b = 2)
// finally, have a pluggable predicate, in case we care about tenant columns

public abstract  class KeyCollector {

	private LanguageNode rootNode;
	private Edge<?,?> rootEdge;
	protected final SchemaContext context;
	public KeyCollector(SchemaContext sc,LanguageNode rn) {
		rootNode = rn;
		rootEdge = null;
		context = sc;
	}
	
	public KeyCollector(SchemaContext sc, Edge<?,?> e) {
		rootNode = null;
		rootEdge = e;
		context = sc;
	}

	public ListSet<Part> getParts() {
		CollectorTraversal action = new CollectorTraversal(this);
		if (rootNode != null) {
			LanguageNode flattened = new AndOrCollector().traverse(rootNode);
			action.traverse(flattened);
			return action.getCompletedParts();
		} else if (rootEdge != null) {
			new AndOrCollector().traverse(rootEdge);
			action.traverse(rootEdge);
			return action.getCompletedParts();			
		} else
			throw new SchemaException(Pass.PLANNER, "No subject given for key collector");
	
	}
		
	protected abstract boolean isQualifyingColumn(ColumnInstance ci);
	protected abstract boolean isQualifyingColumn(PEColumn c);
	
	protected AndedParts completeKey(EqualityPart existing, PEColumn c, ConstantExpression litex) {
		EqualityPart np = makeNewEqualityPart(existing,c,litex);
		FunctionCall andEx = new FunctionCall(FunctionName.makeAnd(),(ExpressionNode)existing.getParent(),(ExpressionNode)np.getParent());
		ArrayList<Part> subp = new ArrayList<Part>();
		subp.add(existing);
		subp.add(np);
		return buildAndedParts(andEx,subp);
	}
	
	protected AndedParts completeKey(AndedParts existing, PEColumn c, ConstantExpression litex) {
		EqualityPart ep = (EqualityPart) existing.getParts().get(0);
		EqualityPart np = makeNewEqualityPart(ep,c,litex);
		ArrayList<Part> subp = new ArrayList<Part>();
		subp.addAll(existing.getParts());
		subp.add(np);
		List<ExpressionNode> subexprs = Functional.apply(subp, Part.castToExpression);
		FunctionCall andEx = new FunctionCall(FunctionName.makeAnd(),subexprs);
		return buildAndedParts(andEx,subp);
	}
	
	protected ListSet<ColumnKey> buildKeyColumns(TableKey tk, Collection<PEColumn> cols) {
		ListSet<ColumnKey> out = new ListSet<ColumnKey>();
		for(PEColumn c : cols)
			out.add(new ColumnKey(tk, c));
		return out;
	}
		
	private EqualityPart makeNewEqualityPart(EqualityPart existing, PEColumn c, ConstantExpression litex) {
		TableKey tk = existing.getColumn().getColumnKey().getTableKey();
		ColumnInstance nc = new ColumnInstance(c,tk.toInstance());
		FunctionCall eq = new FunctionCall(FunctionName.makeEquals(),nc,litex);
		EqualityPart eqp = buildEqualityPart(eq, nc, litex);
		return eqp;
	}
	
	protected EqualityPart buildEqualityPart(LanguageNode parent, ColumnInstance ci, ConstantExpression litex) {
		return new EqualityPart(parent, ci, litex);
	}
	
	protected OredParts buildOredParts(LanguageNode parent,TableKey tk, List<Part> comps) {
		return new OredParts(parent, tk, comps);
	}
	
	protected AndedParts buildAndedParts(LanguageNode parent, TableKey tk, List<Part> parts) {
		return new AndedParts(parent, tk, parts);
	}
	
	protected final OredParts buildOredParts(LanguageNode parent, List<Part> comps) {
		return buildOredParts(parent, assertSingleTableKey(comps), comps);
	}
	
	protected final AndedParts buildAndedParts(LanguageNode parent, List<Part> comps) {
		return buildAndedParts(parent, assertSingleTableKey(comps), comps);
	}
	
	protected TableKey assertSingleTableKey(List<Part> comps) {
		TableKey tk = null;
		for(Part p : comps) {
			if (tk == null)
				tk = p.getTableKey();
			else if (!tk.equals(p.getTableKey())) 
				throw new SchemaException(Pass.PLANNER, "Mixed table keys for key collector");
		}
		return tk;
	}
	
	protected abstract boolean isComplete(EqualityPart sp);
	protected abstract boolean isComplete(AndedParts cp);
	
	// helpful for the isComplete methods
	// essentially, the part is complete if it has at least all the columns required
	protected boolean isSame(List<ColumnKey> ck, List<PEColumn> c) {
		List<Column<?>> ac = Functional.apply(ck, ColumnKey.getColumn);
		HashSet<Column<?>> copy = new HashSet<Column<?>>(c);
		copy.removeAll(ac);
		return copy.isEmpty();
	}
	
	protected abstract AndedParts maybeMakeComplete(EqualityPart sp);
	protected abstract AndedParts maybeMakeComplete(AndedParts cp);
	
	protected abstract List<ColumnKey> getNeeded(Collection<ColumnKey> in);
	
	protected AndedParts maybeMakeComplete(EqualityPart sp, ConstantExpression litex) {
		if (litex == null)
			return null;
		List<ColumnKey> needed = getNeeded(Collections.singletonList(sp.getColumn().getColumnKey()));
		if (needed.isEmpty())
			return null;
		else if (needed.size() == 1) {
			PEColumn c = needed.get(0).getPEColumn();
			if (c.isTenantColumn()) {
				return completeKey(sp,c,litex);
			}
		}
		return null;
	}
	
	protected AndedParts maybeMakeComplete(AndedParts cp, ConstantExpression tenantEx) {
		if (tenantEx == null) return null;
		List<ColumnKey> needed = getNeeded(cp.getColumns());
		if (needed.isEmpty())
			return null;
		else if (needed.size() == 1) {
			PEColumn c = needed.get(0).getPEColumn();
			if (c.isTenantColumn()) {
				return completeKey(cp,c,tenantEx);
			}
		}
		return null;
	}
	
	private static class CollectorTraversal extends Traversal {
		
		private KeyCollector parent;
		private boolean stopped;
		
		// the target is a Part of some variety
		protected Map<LanguageNode, Part> state;
		
		protected ListSet<Part> completedParts;
		
		private CollectorTraversal(KeyCollector implementor) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			parent = implementor;
			stopped = false;
			state = new HashMap<LanguageNode, Part>();
			completedParts = new ListSet<Part>();
		}
		
		public boolean allow(Edge<?,?> e) {
			return !stopped;
		}
		
		public boolean allow(LanguageNode ln) {
			return !stopped;
		}
		
		protected ListSet<Part> getCompletedParts() {
			if (stopped)
				return null;
			ListSet<Part> cols = new ListSet<Part>();
			for(Part p : completedParts) {
				if (p instanceof OredParts)
					cols.add(p);
			}
			completedParts.removeAll(cols);
			for(Part p : cols) {
				for(Part sp : p.getParts()) {
					completedParts.remove(sp);
				}
			}
			completedParts.addAll(cols);
			return completedParts;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (stopped) return in;
			if (EngineConstant.FUNCTION.has(in)) {
				FunctionCall fc = (FunctionCall)in;
				LanguageNode after = in;
				if (fc.getFunctionName().isEquals()) 
					after = handleEqualsFunction(fc);
				else if (fc.getFunctionName().isAnd()) 
					after = handleAndFunction(fc);
				else if (fc.getFunctionName().isOr()) 
					after = handleOrFunction(fc);
				else if (fc.getFunctionName().isEffectiveNot())
					after = handleNotFunction(fc);
				else if (fc.getFunctionName().isIn())
					after = handleInFunction(fc);
				return after;
			}
			return in;
		}

		private void broadening() {
			stopped = true;
		}
		
		private void setComplete(Part p) {
			p.setComplete();
			completedParts.add(p);
		}
		
		private LanguageNode handleEqualsFunction(FunctionCall fc) {
			ExpressionNode lhs = fc.getParameters().get(0);
			ExpressionNode rhs = fc.getParameters().get(1);
			if (EngineConstant.CONSTANT.has(rhs) && EngineConstant.COLUMN.has(lhs) && 
					parent.isQualifyingColumn((ColumnInstance)lhs)) {
				ColumnInstance ci = (ColumnInstance)lhs;
				ConstantExpression litex = (ConstantExpression)rhs;
				PEColumn c = ci.getPEColumn();
				if (parent.isQualifyingColumn(c)) {
					Part p = buildPart(fc, ci, litex);
					return p.getParent();
				}
			}
			return fc;
		}
		
		private LanguageNode handleAndFunction(FunctionCall fc) {
			// and functions take incomplete simple parts and turn them into complete parts, if so desired
			ArrayList<Part> incompletes = new ArrayList<Part>();
			ArrayList<ExpressionNode> ok = new ArrayList<ExpressionNode>();
			ArrayList<Part> subparts = new ArrayList<Part>();
			for(ExpressionNode en : fc.getParameters()) {
				Part p = state.get(en);
				if (p == null || p.isComplete()) {
					ok.add(en);
					if (p != null) subparts.add(p);
					continue;
				}
				incompletes.add(p);
			}
			if (incompletes.isEmpty())
				return fc;
			
			// now we have the problem of a mishmash of incompletes.  some may be complex, some may be simple
			// some may be collections.  we need to handle cases like the following:
			// (a = 1) and (b = 2) {a,b} (1 key)
			// (a = 1) and (b = 2 or b = 3) {a,b} (2 keys)
			// (a = 1 or a = 2) and (b = 3) {a,b} (2 keys)
			// (a = 1 or a = 2) and (b = 3 or b = 4) {a,b} (4 keys here)
			// all of the above, where the result is still not complete due to missing tenant column
						
			MultiMap<Part,ColumnKey> needed = new MultiMap<Part,ColumnKey>();
			MultiMap<ColumnKey, Part> classified = new MultiMap<ColumnKey, Part>();
			for(Part p : incompletes) {
				ListSet<ColumnKey> has = new ListSet<ColumnKey>();
				has.addAll(p.getColumns());
				needed.put(p, parent.getNeeded(has));
				for(ColumnKey ck : has) {
					classified.put(ck, p);
				}
			}
			// so let's say we have a part that is (a = 1 and b = 2), needs c and tenant, and we have a part
			// that is c in (1,2,3).  The needed for (a = 1 and b = 2) is {c,tenant}.  we'll pull (c in (1,2,3))
			// so we'll get at least (a = 1 and b = 2 and c = 3) or (a = 1 and b =2 and c = 3) ...
			// these we can then individually try to complete.

			while(!needed.isEmpty()) {
				combineParts(needed, classified, ok, subparts);
			}
			for(Part p : subparts) 
				state.put(p.getParent(), p);
			if (ok.size() == 1) 
				return ok.get(0);
			else {
				// what's left is a mix of unrelated and complete or incomplete subexprs.  unrelated nodes
				// would come in from above, as would previously complete.
				return new FunctionCall(FunctionName.makeAnd(),ok);
			}
		}

		private void combineParts(MultiMap<Part, ColumnKey> needed, MultiMap<ColumnKey,Part> classified,
				List<ExpressionNode> andexprs, List<Part> andparts) {
			Part p = needed.keySet().iterator().next();
			Collection<ColumnKey> missing = needed.get(p);
			if (missing == null || missing.isEmpty()) {
				andexprs.add((ExpressionNode)p.getParent());
				needed.remove(p);
				andparts.add(p);
				return;
			}
			ListSet<Part> containingMissing = new ListSet<Part>();
			boolean first = true;
			for(ColumnKey ck : missing) {
				Collection<Part> matching = classified.get(ck);
				if (matching == null || matching.isEmpty()) continue;
				if (first)
					containingMissing.addAll(matching);
				else {
					containingMissing.retainAll(matching);
				}
			}
			if (containingMissing.isEmpty()) {
				andexprs.add((ExpressionNode)p.getParent());
				andparts.add(p);
				needed.remove(p);
			} else {
				ListSet<Part> toCombine = new ListSet<Part>();
				toCombine.add(p);
				toCombine.addAll(containingMissing);
				ListSet<Part> toRemove = new ListSet<Part>();
				toRemove.addAll(toCombine);
				Part clhs = choosePartForAndCombo(toCombine);
				toCombine.remove(clhs);
				while(!toCombine.isEmpty()) {
					Part crhs = choosePartForAndCombo(toCombine);
					toCombine.remove(crhs);
					clhs = combineParts(clhs,crhs);
				}
				for(Part rp : toRemove) {
					needed.remove(rp);
					ArrayList<ColumnKey> classKeys = new ArrayList<ColumnKey>(classified.keySet());
					for(ColumnKey ck : classKeys) {
						Collection<Part> sub = classified.get(ck);
						if (sub == null || sub.isEmpty()) continue;
						if (sub.contains(rp))
							classified.remove(ck, rp);
					}
				}
				andexprs.add((ExpressionNode)clhs.getParent());
				andparts.add(clhs);
			}
		}
		
		private Part choosePartForAndCombo(ListSet<Part> in) {
			Part out = null;
			for(Part p : in) {
				if (p instanceof OredParts) {
					out = p;
					break;
				}
			}
			if (out == null)
				out = in.get(0);
			return out;
		}

		private Part combineParts(Part lp, Part rp) {
			if (!(lp instanceof OredParts) && !(rp instanceof OredParts)) {
				return buildNewComplexPart(lp, rp);
			} else if (lp instanceof OredParts && rp instanceof OredParts) {
				return buildNewMultiMultiPart((OredParts)lp,(OredParts)rp);
			} else if (lp instanceof OredParts && !(rp instanceof OredParts)) {
				return buildNewPartCollection((OredParts)lp,rp);
			} else if (!(lp instanceof OredParts) && rp instanceof OredParts) {
				return buildNewPartCollection((OredParts)rp,lp);
			} else {
				throw new SchemaException(Pass.PLANNER, "Can't combine parts: " + lp.getClass().getName() + " and " + rp.getClass().getName());
			}
				
		}
		
		private Part buildNewComplexPart(Part lp, Part rp) {
			ListSet<Part> allParts = new ListSet<Part>();
			allParts.addAll(lp.getParts());
			allParts.addAll(rp.getParts());
			FunctionCall andCall = new FunctionCall(FunctionName.makeAnd(), 
					Functional.apply(allParts, Part.castToExpression));
			AndedParts cp = parent.buildAndedParts(andCall, allParts);
			if (parent.isComplete(cp)) {
				setComplete(cp);
			} else if (!parent.isComplete(cp)) {
				AndedParts ncp = parent.maybeMakeComplete(cp);
				if (ncp != null) {
					setComplete(ncp);
					return ncp;
				}
			}
			return cp;
		}
		
		private Part buildNewPartCollection(OredParts lp, Part rp) {
			// rp could be simple or complex, lp has either simple or complex elements.
			// the strategy here is to build a new complex part for each item in the collection, and return a new collection.  
			ArrayList<Part> newParts = new ArrayList<Part>();
			for(Part sp : lp.getParts()) {
				newParts.add(buildNewComplexPart(sp, rp.copy()));				
			}
			FunctionCall orcall = new FunctionCall(FunctionName.makeOr(),
					Functional.apply(newParts, Part.castToExpression));
			OredParts op = parent.buildOredParts(orcall, newParts);
			if (op.isComplete())
				setComplete(op);
			return op;
		}

		private Part buildNewMultiMultiPart(OredParts lp, OredParts rp) {
			ArrayList<Part> newParts = new ArrayList<Part>();
			for(Part lpc : lp.getParts()) {
				for(Part rpc : rp.getParts()) {
					newParts.add(combineParts(lpc.copy(),rpc.copy()));
				}
			}
			FunctionCall orcall = new FunctionCall(FunctionName.makeOr(),
					Functional.apply(newParts, Part.castToExpression));
			OredParts op = parent.buildOredParts(orcall, newParts);
			if (op.isComplete())
				setComplete(op);
			return op;
		}
		
		private LanguageNode handleOrFunction(FunctionCall fc) {
			ArrayList<Part> subparts = new ArrayList<Part>();
			for(ExpressionNode en : fc.getParameters()) {
				Part p = state.get(en);
				if (p == null) {
					broadening();
					return fc;
				}
				subparts.add(p);
			}
			// now to figure out what we have.  we may have a a bunch of incomplete subexprs,
			// in which case we took a = 1 or a = 2 or a = 3 => a part collection of incompletes
			// or we may have (a = 1 and b = 2) or (a =3 and b =4) ... - likewise
			// or they maybe complete.  regardless, just build a partcollection and move on.
			// sort subparts by table key; if there's more than one let's just set broadening for now
			TableKey tk = null;
			for(Part p : subparts) {
				if (tk == null)
					tk = p.getTableKey();
				else if (!tk.equals(p.getTableKey())) {
					broadening();
					return fc;
				}
			}
			OredParts op = parent.buildOredParts(fc, subparts);
			if (op.isComplete())
				setComplete(op);
			
			state.put(fc, op);
			return fc;
		}
		
		private LanguageNode handleInFunction(FunctionCall fc) {
			ExpressionNode lhs = fc.getParameters().get(0);
			if (EngineConstant.COLUMN.has(lhs) && parent.isQualifyingColumn((ColumnInstance)lhs)) {
				// only matches if all the rhs are constant
				for(ExpressionNode en : fc.getParameters(1)) {
					if (!EngineConstant.CONSTANT.has(en))
						return fc;
				}
				ColumnInstance ci = (ColumnInstance) lhs;
				if (!parent.isQualifyingColumn(ci.getPEColumn()))
					return fc;
				ArrayList<ExpressionNode> subexprs = new ArrayList<ExpressionNode>();
				ArrayList<Part> parts = new ArrayList<Part>();
				for(ExpressionNode en : fc.getParameters(1)) {
					ColumnInstance tci = (ColumnInstance)ci.copy(null);
					ConstantExpression litex = (ConstantExpression) en;
					ExpressionNode subeq = new FunctionCall(FunctionName.makeEquals(),tci,litex);
					Part p = buildPart(subeq, tci, litex);
					parts.add(p);
					subexprs.add((ExpressionNode)p.getParent());
				}
				if (subexprs.size() > 1) {
					FunctionCall orcall = new FunctionCall(FunctionName.makeOr(),subexprs);
					OredParts pc = parent.buildOredParts(orcall,parts);
					if (pc.isComplete())
						setComplete(pc);
					orcall.setGrouped();
					state.put(orcall, pc);
					return orcall;
				} else {
					Part p = parts.get(0);
					return p.getParent();
				}
			}
			return fc;
		}
		
		private LanguageNode handleNotFunction(FunctionCall fc) {
			broadening();
			return fc;
		}
		
		private Part buildPart(LanguageNode parentNode, ColumnInstance ci, ConstantExpression litex) {
			EqualityPart sp = parent.buildEqualityPart(parentNode, ci, litex);
			if (parent.isComplete(sp)) {
				setComplete(sp);
				state.put(parentNode, sp);
			} else {
				AndedParts xformed = parent.maybeMakeComplete(sp);
				if (xformed != null) {
					setComplete(xformed);
					LanguageNode ret = xformed.getParent();
					state.put(ret, xformed);
					return xformed;
				} else {
					state.put(sp.getParent(), sp);
				}
			}
			return sp;
		}
	}
	
	public static abstract class Part {
		
		protected LanguageNode parent;
		protected TableKey tableKey;
		
		public Part(LanguageNode ln, TableKey tk) {
			parent = ln;
			tableKey = tk;
		}
		
		public Part(Part p) {
			parent = p.parent;
			tableKey = p.tableKey;
		}

		public LanguageNode getParent() {
			return parent;
		}
		
		public TableKey getTableKey() {
			return tableKey;
		}
		
		public abstract List<ColumnKey> getColumns();

		public abstract boolean isComplete();
		
		public abstract void setComplete();
		
		public abstract ListSet<Part> getParts();
		
		public abstract Part copy();

		public static final UnaryFunction<ExpressionNode, Part> castToExpression = new UnaryFunction<ExpressionNode,Part>() {

			@Override
			public ExpressionNode evaluate(Part object) {
				return (ExpressionNode) object.getParent();
			}
			
		};
		
		@Override
		public String toString() {
			return parent.toString();
		}
		
	}
	
	// collection of parts
	public static class OredParts extends Part{
		
		protected ListSet<Part> parts;
		
		public OredParts(LanguageNode parent, TableKey tk, Collection<Part> p) {
			super(parent,tk);
			parts = new ListSet<Part>();
			parts.addAll(p);
			((ExpressionNode)parent).setGrouped();
		}
		
		public OredParts(OredParts o) {
			super(o);
			parts = o.parts;
		}
		
		@Override
		public ListSet<Part> getParts() {
			return parts;
		}

		@Override
		public List<ColumnKey> getColumns() {
			ArrayList<ColumnKey> cols = new ArrayList<ColumnKey>();
			for(Part p : parts)
				cols.addAll(p.getColumns());
			return cols;
		}

		@Override
		public boolean isComplete() {
			for(Part p : parts)
				if (!p.isComplete())
					return false;
			return true;
		}

		@Override
		public void setComplete() {
			// does nothing - all state depends on children
		}
		
		@Override
		public Part copy() {
			List<Part> newparts = new ArrayList<Part>();
			for(Part p : parts)
				newparts.add(p.copy());
			FunctionCall newp = new FunctionCall(FunctionName.makeOr(), 
					Functional.apply(newparts, new UnaryFunction<ExpressionNode, Part>() {

						@Override
						public ExpressionNode evaluate(Part object) {
							return (ExpressionNode) object.getParent();
						}
						
					}));
			return new AndedParts(newp, getTableKey(), newparts, isComplete());
		}
	}
	
	public static class EqualityPart extends Part {
		
		protected ColumnInstance column;
		protected ConstantExpression constant;
		protected boolean complete;
		
		public EqualityPart(LanguageNode owning, ColumnInstance ci, ConstantExpression litex) {
			this(owning, ci, litex, false);
		}
		
		private EqualityPart(LanguageNode owning, ColumnInstance ci, ConstantExpression litex, boolean comp) {
			super(owning,ci.getColumnKey().getTableKey());
			column = ci;
			constant = litex;
			complete = comp;
		}
		
		public ColumnInstance getColumn() {
			return column;
		}
		
		public ConstantExpression getLiteral() {
			return constant;
		}
		
		@Override
		public List<ColumnKey> getColumns() {
			return Collections.singletonList(column.getColumnKey());
		}

		@Override
		public boolean isComplete() {
			return complete;
		}
		
		@Override
		public void setComplete() {
			complete = true;
		}

		@Override
		public ListSet<Part> getParts() {
			ListSet<Part> me = new ListSet<Part>();
			me.add(this);
			return me;
		}

		@Override
		public Part copy() {
			FunctionCall nfc = (FunctionCall)getParent().copy(null);
			ColumnInstance ci = (ColumnInstance) nfc.getParametersEdge().get(0);
			ConstantExpression litex = (ConstantExpression) nfc.getParametersEdge().get(1);
			return new EqualityPart(nfc, ci, litex,complete);
		}
	}
	
	public static class AndedParts extends Part {
		
		protected ListSet<Part> parts;
		protected boolean complete;

		public AndedParts(LanguageNode owning, TableKey tk, List<Part> inparts) {
			this(owning,tk,inparts,false);
			((ExpressionNode)owning).setGrouped();
		}
		
		private AndedParts(LanguageNode owning, TableKey tk, List<Part> inparts, boolean comp) {
			super(owning, tk);
			this.parts = new ListSet<Part>();
			this.parts.addAll(inparts);
			complete = comp;
		}
		
		@Override
		public ListSet<Part> getParts() {
			return this.parts;
		}

		@Override
		public List<ColumnKey> getColumns() {
			return Functional.apply(getParts(), new UnaryFunction<ColumnKey,Part>() {

				@Override
				public ColumnKey evaluate(Part object) {
					return object.getColumns().get(0);
				}
				
			});
		}

		@Override
		public boolean isComplete() {
			return complete;
		}
		
		@Override
		public void setComplete() {
			complete = true;
		}

		@Override
		public Part copy() {
			List<Part> newparts = new ArrayList<Part>();
			for(Part p : parts)
				newparts.add(p.copy());
			FunctionCall newp = new FunctionCall(FunctionName.makeAnd(), 
					Functional.apply(newparts, new UnaryFunction<ExpressionNode, Part>() {

						@Override
						public ExpressionNode evaluate(Part object) {
							return (ExpressionNode) object.getParent();
						}
						
					}));
			return new AndedParts(newp, getTableKey(), newparts, complete);
		}
	}
	
	
	// given and(a,and(b,c)), and(and(a,b),c), convert to and(a,b,c)
	// given and(and(a,b),and(c,d)) convert to and(a,b,c,d)
	public static class AndOrCollector extends Traversal {
		
		public AndOrCollector() {
			super(Order.POSTORDER, ExecStyle.REPEAT);
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (EngineConstant.FUNCTION.has(in, EngineConstant.AND)) {
				return fold(in, EngineConstant.AND);
			} else if (EngineConstant.FUNCTION.has(in, EngineConstant.OR)) {
				return fold(in, EngineConstant.OR);
			} else {
				return in;
			}
		}
		
		private LanguageNode fold(LanguageNode in, EngineToken variety) {
			FunctionCall fc = (FunctionCall) in;
			ArrayList<ExpressionNode> grouped = new ArrayList<ExpressionNode>();
			ArrayList<ExpressionNode> same = new ArrayList<ExpressionNode>();
			ArrayList<ExpressionNode> others = new ArrayList<ExpressionNode>();
			for(ExpressionNode p : fc.getParameters()) {
				if (EngineConstant.FUNCTION.has(p, variety)) {
					if (p.isGrouped())
						grouped.add(p);
					else
						same.add(p);
				} else {
					others.add(p);
				}
			}
			if (same.isEmpty() || !grouped.isEmpty())
				return in;
			// if others is empty, this is op(op(a,b),op(c,d)), build op(a,b,c,d)
			// if others is not empty, this is op(a,op(b,c)) or op(op(a,b),c), build op(a,b,c)
			ArrayList<ExpressionNode> allsubs = new ArrayList<ExpressionNode>();
			for(ExpressionNode p : same) {
				FunctionCall pfc = (FunctionCall) p;
				allsubs.addAll(pfc.getParameters());
			}
			allsubs.addAll(others);
			// we're going to group this as well
			FunctionCall ofc = new FunctionCall(fc.getFunctionName(), allsubs);
			ofc.setGrouped();
			return ofc;
		}
		
	}


	

}
