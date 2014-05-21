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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.node.test.EngineToken;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.constraints.ConstraintCollection;
import com.tesora.dve.sql.transform.constraints.PlanningConstraint;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

public class ConstraintCollector {

	public static PlanningConstraint chooseBest(SchemaContext sc, Iterable<PlanningConstraint> constraints) {
		PlanningConstraint best = null;
		for(PlanningConstraint pc : constraints) {
			if (best == null) best = pc;
			else if (best.compareTo(pc) >= 0) best = pc;
		}
		return best;
	}

	
	private final LanguageNode rootNode;
	private final SchemaContext context;
	
	private ListSet<Part> parts;
	
	private final boolean findDistKeys;
	private final boolean findKeys;
	private final boolean allowPartialMatches;
	
	public static PlanningConstraint findBestConstraint(SchemaContext sc, LanguageNode node, boolean partials) {
		ConstraintCollector cc = new ConstraintCollector(sc,node,false,true,partials);
		return chooseBest(sc,cc.getConstraints());
	}
	
	public ConstraintCollector(SchemaContext sc,LanguageNode rn, boolean distKeys, boolean keys, boolean partials) {
		rootNode = rn;
		context = sc;
		findDistKeys = distKeys;
		findKeys = keys;
		allowPartialMatches = partials;
	}
	
	private ListSet<Part> buildParts() {
		CollectorTraversal action = new CollectorTraversal(this);
		LanguageNode flattened = new AndOrCollector().traverse(rootNode);
		action.traverse(flattened);
		return action.getCompletedParts();
	}
	
	public ListSet<Part> getParts() {
		if (parts == null)
			parts = buildParts();
		return parts;
	}

	// convert the parts into useful information
	// for this - complete single parts are constraints, and complete series parts are constraint collections
	public ListSet<PlanningConstraint> getConstraints() {
		ListSet<PlanningConstraint> out = new ListSet<PlanningConstraint>();
		ListSet<Part> any = getParts();
		if (any == null) return out;
		for(Part p : any) {
			if (!p.isComplete()) continue;
			out.addAll(p.convertAll(context));
		}
		return out;
	}
	
	protected Set<MatchableKey> isQualifyingColumn(ColumnInstance ci){
		return isQualifyingColumn(ci.getPEColumn());
	}
	
	protected Set<MatchableKey> isQualifyingColumn(PEColumn c) {
		HashSet<MatchableKey> allSuch = new HashSet<MatchableKey>();
		if (findDistKeys) {
			ListSet<PEColumn> cols = c.getTable().getDistributionVector(context).getColumns(context);
			if (cols.contains(c))
				allSuch.add(c.getTable().getDistributionVector(context));
		}
		if (findKeys) 
			allSuch.addAll(c.getReferencedBy(context));
		return allSuch;
	}
	
	protected AndedParts completeKey(EqualityPart existing, PEColumn c, ConstantExpression litex, Set<MatchableKey> onKeys) {
		EqualityPart np = makeNewEqualityPart(existing,c,litex, onKeys);
		FunctionCall andEx = new FunctionCall(FunctionName.makeAnd(),(ExpressionNode)existing.getParent(),(ExpressionNode)np.getParent());
		ArrayList<Part> subp = new ArrayList<Part>();
		subp.add(existing);
		subp.add(np);
		return buildAndedParts(andEx,subp);
	}
	
	protected AndedParts completeKey(AndedParts existing, PEColumn c, ConstantExpression litex, Set<MatchableKey> onKeys) {
		EqualityPart ep = (EqualityPart) existing.getParts().get(0);
		EqualityPart np = makeNewEqualityPart(ep,c,litex, onKeys);
		ArrayList<Part> subp = new ArrayList<Part>();
		subp.addAll(existing.getParts());
		subp.add(np);
		List<ExpressionNode> subexprs = Functional.apply(subp, Part.castToExpression);
		FunctionCall andEx = new FunctionCall(FunctionName.makeAnd(),subexprs);
		return buildAndedParts(andEx,subp);
	}
	
	private EqualityPart makeNewEqualityPart(EqualityPart existing, PEColumn c, ConstantExpression litex, Set<MatchableKey> onKeys) {
		TableKey tk = existing.getColumn().getColumnKey().getTableKey();
		ColumnInstance nc = new ColumnInstance(c,tk.toInstance());
		FunctionCall eq = new FunctionCall(FunctionName.makeEquals(),nc,litex);
		EqualityPart eqp = buildEqualityPart(eq, nc, litex, onKeys);
		return eqp;
	}
	
	protected EqualityPart buildEqualityPart(LanguageNode parent, ColumnInstance ci, ConstantExpression litex, Set<MatchableKey> onKeys) {
		return new EqualityPart(context, parent, ci, litex, onKeys);
	}
	
	protected OredParts buildOredParts(LanguageNode parent,TableKey tk, List<Part> comps, Set<MatchableKey> onKeys) {
		return new OredParts(parent, tk, comps, onKeys);
	}
	
	protected AndedParts buildAndedParts(LanguageNode parent, TableKey tk, List<Part> parts, Set<MatchableKey> onKeys) {
		return new AndedParts(parent, tk, parts, onKeys);
	}
	
	protected final OredParts buildOredParts(LanguageNode parent, List<Part> comps) {
		return buildOredParts(parent, assertSingleTableKey(comps), comps, collapseKeys(comps));
	}
	
	protected final Set<MatchableKey> collapseKeys(List<Part> comps) {
		HashSet<MatchableKey> out = null;
		for(Part p : comps) {
			if (out == null)
				out = new HashSet<MatchableKey>(p.getKeys());
			else if (!out.equals(p.getKeys()))
				throw new SchemaException(Pass.PLANNER, "Collapse keys: multiple differing sets of keys");
		}
		return out;
	}
	
	protected final AndedParts buildAndedParts(LanguageNode parent, List<Part> comps) {
		// in this case, the anded parts is the intersection of the keys
		HashSet<MatchableKey> any = null;
		for(Part p : comps) {
			if (any == null)
				any = new HashSet<MatchableKey>(p.getKeys());
			else
				any.retainAll(p.getKeys());
		}
		if (any.isEmpty())
			return null;
		return buildAndedParts(parent, assertSingleTableKey(comps), comps, any);
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
	
	// return all keys for which it is complete
	protected Set<MatchableKey> isComplete(Part ep) {
		HashSet<PEColumn> cols = new HashSet<PEColumn>();
		for(ColumnKey ck : ep.getColumns())
			cols.add(ck.getPEColumn());
		HashSet<MatchableKey> complete = new HashSet<MatchableKey>();
		for(MatchableKey mk : ep.getKeys()) {
			if (mk.isComplete(context, cols, allowPartialMatches))
				complete.add(mk);
		}
		return complete;
	}
	
	protected MultiMap<MatchableKey, PEColumn> getNeeded(Collection<ColumnKey> colKeys, Set<MatchableKey> onKeys) {
		MultiMap<MatchableKey, PEColumn> needed = new MultiMap<MatchableKey,PEColumn>();
		List<PEColumn> have = Functional.apply(colKeys, ColumnKey.getPEColumn);
		for(MatchableKey mk : onKeys) {
			HashSet<PEColumn> buf = new HashSet<PEColumn>(mk.getColumns(context));
			buf.removeAll(have);
			if (!buf.isEmpty())
				needed.put(mk, buf);
		}
		return needed;
	}
	
	protected AndedParts maybeMakeComplete(Part sp, Set<MatchableKey> onKeys) {
		ConstantExpression litex = context.getPolicyContext().getTenantIDLiteral(false);
		if (litex == null)
			return null;
		MultiMap<MatchableKey, PEColumn> needed = getNeeded(sp.getColumns(),onKeys); 
		if (needed.isEmpty())
			return null;
		else {
			HashSet<MatchableKey> complete = new HashSet<MatchableKey>();
			PEColumn tenCol = null;
			for(MatchableKey mk : needed.keySet()) {
				Collection<PEColumn> sub = needed.get(mk);
				if (sub == null || sub.isEmpty()) continue;
				if (sub.size() == 1) {
					PEColumn c = sub.iterator().next();
					if (c.isTenantColumn()) {
						complete.add(mk);
						tenCol = c;
					}
				}
			}
			if (!complete.isEmpty()) {
				if (sp instanceof EqualityPart)
					return completeKey((EqualityPart)sp,tenCol,litex,complete);
				else
					return completeKey((AndedParts)sp,tenCol,litex,complete);
			}
		}
		return null;
	}
		
	private static class CollectorTraversal extends Traversal {
		
		private final ConstraintCollector parent;
		private boolean stopped;
		
		// the target is a Part of some variety
		protected Map<LanguageNode, Part> state;
		
		protected ListSet<Part> completedParts;
		
		protected LanguageNode avoiding = null;
		
		private CollectorTraversal(ConstraintCollector implementor) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			parent = implementor;
			stopped = false;
			state = new HashMap<LanguageNode, Part>();
			completedParts = new ListSet<Part>();
		}
		
		public void push(LanguageNode n) {
			super.push(n);
			if (n instanceof FunctionCall) {
				FunctionCall fc = (FunctionCall) n;
				if (fc.getFunctionName().isEffectiveNot())
					avoiding = n;
			}
		}
		
		public LanguageNode pop() {
			LanguageNode ln = super.pop();
			if (ln == avoiding)
				avoiding = null;
			return ln;
		}
		
		private static final EnumSet<EdgeName> denyMask =
				EnumSet.of(EdgeName.UPDATE_EXPRS,
						EdgeName.PROJECTION,
						EdgeName.ORDERBY, 
						EdgeName.GROUPBY, 
						EdgeName.HAVING,
						EdgeName.SUBQUERY);
		
		public boolean allow(Edge<?,?> e) {
			// we don't want to consider anything not in the from or where clause
			// we also don't want to cross subquery boundaries
			if (e.getName().any(denyMask))
				return false;
			return !stopped && avoiding == null;
		}
		
		public boolean allow(LanguageNode ln) {
			return !stopped && avoiding == null;
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
					return fc;
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
			if (EngineConstant.CONSTANT.has(rhs) && EngineConstant.COLUMN.has(lhs)) {
				Set<MatchableKey> matching = parent.isQualifyingColumn((ColumnInstance)lhs);
				if (!matching.isEmpty()) {
					ColumnInstance ci = (ColumnInstance)lhs;
					ConstantExpression litex = (ConstantExpression)rhs;
					Part p = buildPart(fc, ci, litex, matching);
					return p.getParent();
				}
			}
			return fc;
		}
		
		private LanguageNode handleAndFunction(FunctionCall fc) {
			// and functions take incomplete simple parts and turn them into complete parts, if so desired
			// note that not all subexprs of the and expression may be subparts or parts - in that case
			// just ignore them
			MultiMap<TableKey,Part> incompletes = new MultiMap<TableKey,Part>();
			ArrayList<ExpressionNode> ok = new ArrayList<ExpressionNode>();
			MultiMap<TableKey,Part> subparts = new MultiMap<TableKey,Part>();
			for(ExpressionNode en : fc.getParameters()) {
				Part p = state.get(en);
				if (p == null || p.isComplete()) {					
					ok.add(en);
					if (p != null) subparts.put(p.getTableKey(), p); 
					continue;
				}
				incompletes.put(p.getTableKey(),p);
			}
			if (incompletes.isEmpty())
				return fc;
			// ok has all the subexprs that are irrelevant, whereas incompletes has subexprs that may be pertinent
			
			// now we have the problem of a mishmash of incompletes.  some may be complex, some may be simple
			// some may be collections.  we need to handle cases like the following:
			// (a = 1) and (b = 2) {a,b} (1 key)
			// (a = 1) and (b = 2 or b = 3) {a,b} (2 keys)
			// (a = 1 or a = 2) and (b = 3) {a,b} (2 keys)
			// (a = 1 or a = 2) and (b = 3 or b = 4) {a,b} (4 keys)
			// all of the above, where the result is still not complete due to missing tenant column
			
			MultiMap<Part,PEColumn> needed = new MultiMap<Part,PEColumn>(new MultiMap.HashedCollectionFactory<PEColumn>());
			MultiMap<ColumnKey, Part> classified = new MultiMap<ColumnKey, Part>();
			for(Part p : incompletes.values()) {
				ListSet<ColumnKey> has = new ListSet<ColumnKey>();
				has.addAll(p.getColumns());
				MultiMap<MatchableKey,PEColumn> subn = parent.getNeeded(has, p.getKeys());
				for(MatchableKey mk : subn.keySet()) {
					needed.put(p, subn.get(mk));
				}
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
			for(Part p : subparts.values()) 
				state.put(p.getParent(), p);
			if (ok.size() == 1) 
				return ok.get(0);
			else {
				// what's left is a mix of unrelated and complete or incomplete subexprs.  unrelated nodes
				// would come in from above, as would previously complete.
				return new FunctionCall(FunctionName.makeAnd(),ok);
			}
		}

		private void combineParts(MultiMap<Part, PEColumn> needed, MultiMap<ColumnKey,Part> classified,
				List<ExpressionNode> andexprs, MultiMap<TableKey,Part> andparts) {
			Part p = needed.keySet().iterator().next();
			Collection<PEColumn> missing = needed.get(p);
			if (missing == null || missing.isEmpty()) {
				// nothing further to be done.
				andexprs.add((ExpressionNode)p.getParent());
				needed.remove(p);
				andparts.put(p.getTableKey(),p);
				return;
			}
			ListSet<Part> containingMissing = new ListSet<Part>();
			boolean first = true;
			for(PEColumn pec : missing) {
				ColumnKey ck = new ColumnKey(p.getTableKey(),pec);
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
				andparts.put(p.getTableKey(),p);
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
				andparts.put(clhs.getTableKey(),clhs);
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
			if (cp == null)
				return null;
			Set<MatchableKey> completeFor = parent.isComplete(cp);
			if (!completeFor.isEmpty())
				setComplete(cp);
			else {
				AndedParts ncp = parent.maybeMakeComplete(cp,cp.getKeys());
				if (ncp != null) {
					setComplete(ncp);
					return ncp;
				}
			}
			return cp;
		}
		
		private Part buildNewPartCollection(OredParts lp, Part rp) {
			// rp could be simple or complex, lp has either simple or complex elements.
			// the strategy here is to build a new complex part for each item in the collection, and return
			// a new collection.  Since PartCollections are only built for or/in, we use the or connector
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
			boolean allands = false;
			LanguageNode ln = fc.getParent();
			if (ln == parent.rootNode) {
				if (EngineConstant.FUNCTION.has(ln,EngineConstant.AND))
					allands = true;
			} else {
				while(ln != null && ln != parent.rootNode) {
					if (EngineConstant.FUNCTION.has(ln, EngineConstant.AND)) {
						allands = true;
					} else {
						allands = false;
						break;
					}
					ln = ln.getParent();
				}
			}
			
			ArrayList<Part> subparts = new ArrayList<Part>();
			for(ExpressionNode en : fc.getParameters()) {
				Part p = state.get(en);
				if (p == null) {
					if (!allands)
						broadening();
					return fc;
				}
				subparts.add(p);
			}
			// now to figure out what we have.  we may have a a bunch of incomplete subexprs,
			// in which case we took a = 1 or a = 2 or a = 3 => a part collection of incompletes
			// or we may have (a = 1 and b = 2) or (a =3 and b =4) ... - likewise
			// sort subparts by table key; if there's more than one let's just set broadening for now
			TableKey tk = null;
			// has to be the same key as well
			MatchableKey mk = null;
			for(Part p : subparts) {
				if (tk == null) {
					tk = p.getTableKey();
					mk = p.getKeys().iterator().next();
				} else if (!tk.equals(p.getTableKey()) || !mk.equals(p.getKeys().iterator().next())) {
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
			if (!EngineConstant.COLUMN.has(lhs))
				return fc;
			Set<MatchableKey> matching = parent.isQualifyingColumn((ColumnInstance)lhs);
			if (matching.isEmpty())
				return fc;
			// only matches if all the rhs are constant
			for(ExpressionNode en : fc.getParameters(1)) {
				if (!EngineConstant.CONSTANT.has(en))
					return fc;
			}
			ColumnInstance ci = (ColumnInstance) lhs;
			ArrayList<ExpressionNode> subexprs = new ArrayList<ExpressionNode>();
			ArrayList<Part> parts = new ArrayList<Part>();
			for(ExpressionNode en : fc.getParameters(1)) {
				ColumnInstance tci = (ColumnInstance)ci.copy(null);
				ConstantExpression litex = (ConstantExpression) en;
				ExpressionNode subeq = new FunctionCall(FunctionName.makeEquals(),tci,litex);
				Part p = buildPart(subeq, tci, litex, matching);
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
		
		private Part buildPart(LanguageNode parentNode, ColumnInstance ci, ConstantExpression litex, Set<MatchableKey> onKeys) {
			EqualityPart sp = parent.buildEqualityPart(parentNode, ci, litex, onKeys);
			Set<MatchableKey> complete = parent.isComplete(sp);
			if (!complete.isEmpty()) {
				setComplete(sp);
				state.put(parentNode, sp);
			} else {
				AndedParts xformed = parent.maybeMakeComplete(sp, onKeys);
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
	
	// make a common base class for parts so you can represent series of them
	// allows you to handle a in (1,2,3) and b = 2 as.. (a = 1 and b = 2) or (a = 2 and b = 2) or (a =3 and b = 3)
	
	public static abstract class Part {
		
		protected LanguageNode parent;
		// disallow parts to cross tables by recording table key as well
		protected TableKey tableKey;
		// parts are against a set of possibly matching keys
		protected Set<MatchableKey> keys;
		
		public Part(LanguageNode ln, TableKey tk, Set<MatchableKey> onKeys) {
			parent = ln;
			tableKey = tk;
			keys = new HashSet<MatchableKey>(onKeys);
		}
		
		public Part(Part p) {
			parent = p.parent;
			tableKey = p.tableKey;
			keys = p.keys;
		}

		public LanguageNode getParent() {
			return parent;
		}
		
		public TableKey getTableKey() {
			return tableKey;
		}
		
		public Set<MatchableKey> getKeys() {
			return keys;
		}
		
		public abstract List<ColumnKey> getColumns();

		public abstract boolean isComplete();
		
		public abstract void setComplete();
		
		public abstract ListSet<Part> getParts();
		
		public abstract Part copy();

		public abstract ListSet<PlanningConstraint> convertAll(SchemaContext sc);
		
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
		
		public OredParts(LanguageNode parent, TableKey tk, Collection<Part> p, Set<MatchableKey> onKeys) {
			super(parent,tk, onKeys);
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
		public ListSet<PlanningConstraint> convertAll(SchemaContext sc) {
			HashMap<MatchableKey,ConstraintCollection> byKey = new HashMap<MatchableKey,ConstraintCollection>();
			for(Part p : parts) {
				ListSet<PlanningConstraint> sub = p.convertAll(sc);
				for(PlanningConstraint c : sub) {
					ConstraintCollection cc = byKey.get(c.getKey(sc));
					if (cc == null) {
						cc = new ConstraintCollection();
						byKey.put(c.getKey(sc), cc);
					}
					cc.add(c);
				}
			}
			ListSet<PlanningConstraint> out = new ListSet<PlanningConstraint>();
			for(ConstraintCollection cc : byKey.values())
				out.add(cc);
			return out;
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
			return new AndedParts(newp, getTableKey(), newparts, isComplete(), keys);
		}
	}
	
	public static class EqualityPart extends Part {
		
		protected ColumnInstance column;
		protected ConstantExpression constant;
		protected boolean complete;
		protected SchemaContext context;
		
		public EqualityPart(SchemaContext sc, LanguageNode owning, ColumnInstance ci, ConstantExpression litex, Set<MatchableKey> onKeys) {
			this(sc, owning, ci, litex, false,onKeys);
		}
		
		private EqualityPart(SchemaContext sc, LanguageNode owning, ColumnInstance ci, ConstantExpression litex, boolean comp, Set<MatchableKey> onKeys) {
			super(owning,ci.getColumnKey().getTableKey(),onKeys);
			column = ci;
			constant = litex;
			complete = comp;
			context = sc;
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
		public ListSet<PlanningConstraint> convertAll(SchemaContext sc) {
			ListSet<PlanningConstraint> out = new ListSet<PlanningConstraint>();
			ListOfPairs<PEColumn,ConstantExpression> values = new ListOfPairs<PEColumn,ConstantExpression>();
			values.add(column.getPEColumn(),constant);
			for(MatchableKey mk : getKeys()) {
				List<PEColumn> keyCols = mk.getColumns(context);
				if (keyCols.size() > 1) continue;
				out.add(mk.buildEmptyConstraint(sc, getTableKey(), values));
			}
			return out;
		}
		
		@Override
		public Part copy() {
			FunctionCall nfc = (FunctionCall)getParent().copy(null);
			ColumnInstance ci = (ColumnInstance) nfc.getParametersEdge().get(0);
			ConstantExpression litex = (ConstantExpression) nfc.getParametersEdge().get(1);
			return new EqualityPart(context, nfc, ci, litex,complete,keys);
		}
	}
	
	public static class AndedParts extends Part {
		
		protected ListSet<Part> parts;
		protected boolean complete;

		public AndedParts(LanguageNode owning, TableKey tk, List<Part> inparts, Set<MatchableKey> onKeys) {
			this(owning,tk,inparts,false, onKeys);
			((ExpressionNode)owning).setGrouped();
		}
		
		private AndedParts(LanguageNode owning, TableKey tk, List<Part> inparts, boolean comp, Set<MatchableKey> onKeys) {
			super(owning, tk, onKeys);
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
		public ListSet<PlanningConstraint> convertAll(SchemaContext sc) {
			ListSet<PlanningConstraint> out = new ListSet<PlanningConstraint>();
			ListOfPairs<PEColumn,ConstantExpression> values = new ListOfPairs<PEColumn,ConstantExpression>();
			for(Part p : parts) {
				EqualityPart ep = (EqualityPart) p;
				values.add(ep.getColumn().getPEColumn(),ep.getLiteral());
			}
			for(MatchableKey mk : getKeys()) {
				out.add(mk.buildEmptyConstraint(sc, getTableKey(), values));
			}
			return out;
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
			return new AndedParts(newp, getTableKey(), newparts, complete, keys);
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
