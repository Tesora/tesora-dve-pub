// OS_STATUS: public
package com.tesora.dve.sql.statement.dml;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.NameAlias;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.SchemaMapper;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

// switch all composition to use DMLStatementUtils -
// this forces us to track derived information
public class DMLStatementUtils {

	public static SelectStatement composeMulti(SchemaContext sc, List<SelectStatement> inputs) throws PEException {
		ListSet<PEStorageGroup> groups = new ListSet<PEStorageGroup>();
		SchemaMapper fm = null;
		for(SelectStatement s : inputs) {
			if (!s.getOrderBys().isEmpty())
				throw new PEException("Composition of order bys not supported.");
			if (!s.getGroupBys().isEmpty())
				throw new PEException("Composition of group bys not supported.");
			if (s.getLimit() != null)
				throw new PEException("Composition of limits not supported");
			if (s.getMapper() == null)
				throw new PEException("Composition of nontracked statements.  Missing schema mapper.");
			if (fm == null) fm = s.getMapper();
			else if (!fm.hasSameRoot(s.getMapper()))
				throw new PEException("Composition of statements with differing roots.  Unable to preserve column/table mapping.");
			groups.add(s.getSingleGroup(sc));
		}
		// validate groups - it's enough if they are all subsets of the first group
		for(int i = 1; i < groups.size(); i++) {
			if (!groups.get(0).isSubsetOf(sc, groups.get(i)))
				throw new PEException("Composition of stmts on different groups.  Found group " + groups.get(0) + " and " + groups.get(1));				
		}
		
		
		List<DMLStatement> sources = new ArrayList<DMLStatement>();
		List<SelectStatement> copies = new ArrayList<SelectStatement>();
		ArrayList<ExpressionNode> projection = new ArrayList<ExpressionNode>();
		ArrayList<ExpressionNode> wcs = new ArrayList<ExpressionNode>();
		ArrayList<FromTableReference> tabs = new ArrayList<FromTableReference>();
		AliasInformation combined = new AliasInformation();
		CopyContext fcc = null;
		for(SelectStatement s : inputs) {
			sources.add(s);
			SelectStatement c = CopyVisitor.copy(s);
			if (fcc == null) fcc = c.getMapper().getCopyContext();
			else fcc = CopyContext.compose(fcc, c.getMapper().getCopyContext());
			copies.add(c);
			projection.addAll(c.getProjection());
			wcs.addAll(ExpressionUtils.decomposeAndClause(c.getWhereClause()));
			tabs.addAll(c.getTables());
			combined.take(c.getAliases());
		}
		ExpressionNode nwc = null;
		if (!wcs.isEmpty()) {
			if (wcs.size() == 1)
				nwc = wcs.get(0);
			else
				nwc = ExpressionUtils.buildAnd(wcs);
		}
		SelectStatement out = new SelectStatement(combined)
		.setTables(tabs).setProjection(projection).setWhereClause(nwc);
		for(SelectStatement s : copies)
			out.getDerivedInfo().take(s.getDerivedInfo());
		SchemaMapper uberMapper = new SchemaMapper(sources, out, fcc);
		out.setMapper(uberMapper);
		return out;
	}
	
	public static SelectStatement compose(SchemaContext sc, SelectStatement left, SelectStatement right) throws PEException {
		// we don't handle order bys, limits, group bys - make sure they are not present
		if (!left.getOrderBys().isEmpty() || !right.getOrderBys().isEmpty()) 
			throw new PEException("Composition of order bys not supported.");
		if (!left.getGroupBys().isEmpty() || !right.getGroupBys().isEmpty())
			throw new PEException("Composition of group bys not supported.");
		if ((left.getLimit() != null && !left.getLimit().hasLimitOne(sc) ||
				(right.getLimit() != null && !right.getLimit().hasLimitOne(sc))))
			throw new PEException("Composition of limits not supported");
		SchemaMapper lm = left.getMapper();
		SchemaMapper rm = right.getMapper();
		if (lm == null || rm == null) 
			throw new PEException("Composition of nontracked statements.  Missing schema mapper.");
		
		if (!lm.hasSameRoot(rm)) {
			throw new PEException("Composition of statements with differing roots.  Unable to preserve column/table mapping.");
		}
		// as a failsafe - make sure that the two stmts are on the same storage group - most of the time we have done
		// something like TempTable.buildSelect on both sides.
		PEStorageGroup leftGroup = left.getSingleGroup(sc);
		PEStorageGroup rightGroup = right.getSingleGroup(sc);
		if (!leftGroup.isSubsetOf(sc, rightGroup))
			throw new PEException("Composition of stmts on different groups.  Found group " + leftGroup + " and " + rightGroup);
		
		
		// this really can't be a destructive thing, so we're going to make a couple of copies and fix up 
		// afterwards.  if this was destructive, then if these select statements were used for something else there would be an error.
		
		SelectStatement lc = CopyVisitor.copy(left);
		SelectStatement rc = CopyVisitor.copy(right);
		// might want to use left and right copy context here - more correct since lc and rc aren't returned
		CopyContext fcc = CopyContext.compose(lc.getMapper().getCopyContext(), rc.getMapper().getCopyContext());
		
		ArrayList<ExpressionNode> projection = new ArrayList<ExpressionNode>();
		projection.addAll(lc.getProjection());
		projection.addAll(rc.getProjection());
		ExpressionNode nwc = null;
		ArrayList<ExpressionNode> wcs = new ArrayList<ExpressionNode>();
		// decompose where clauses back down to ands, then add in
		ListSet<ExpressionNode> leftWC = ExpressionUtils.decomposeAndClause(lc.getWhereClause());
		ListSet<ExpressionNode> rightWC = ExpressionUtils.decomposeAndClause(rc.getWhereClause());
		wcs.addAll(leftWC);
		wcs.addAll(rightWC);
		if (!wcs.isEmpty()) {
			if (wcs.size() == 1)
				nwc = wcs.get(0);
			else
				nwc = ExpressionUtils.buildAnd(wcs);
		}
		// this doesn't do the right thing for the tables, but the caller is free to do whatever they want with it.
		ArrayList<FromTableReference> tabs = new ArrayList<FromTableReference>();
		tabs.addAll(lc.getTables());
		tabs.addAll(rc.getTables());
		AliasInformation combined = new AliasInformation();
		combined.take(lc.getAliases());
		combined.take(rc.getAliases());
		SelectStatement out = new SelectStatement(combined)
			.setTables(tabs).setProjection(projection).setWhereClause(nwc);
		out.getDerivedInfo().take(lc.getDerivedInfo());
		out.getDerivedInfo().take(rc.getDerivedInfo());
		ArrayList<DMLStatement> sources = new ArrayList<DMLStatement>();
		sources.add(left);
		sources.add(right);
		SchemaMapper uberMapper = new SchemaMapper(sources, out, fcc);
		out.setMapper(uberMapper);
		return out;
	}
	
	// we do this for the purposes of redistribution, so get all of the columns
	public static SelectStatement convertToSelect(SchemaContext sc, UpdateStatement us) throws PEException {
		UpdateStatement copy = CopyVisitor.copy(us);
		ListSet<ExpressionNode> projs = new ListSet<ExpressionNode>();
		TableInstance mainTable = us.getBaseTables().get(0);
		for(PEColumn c : mainTable.getAbstractTable().getColumns(sc)) {
			projs.add(new ColumnInstance(c,mainTable));
		}
		// we should generate new aliases here
		SelectStatement ss = new SelectStatement(new AliasInformation())
			.setTables(copy.getTables())
			.setProjection(projs)
			.setWhereClause(copy.getWhereClause());
		ss.setOrderBy(copy.getOrderBys());
		ss.setLimit(copy.getLimit());
		ss.getDerivedInfo().take(copy.getDerivedInfo());
		SchemaMapper mapper = new SchemaMapper(copy.getMapper().getOriginals(), ss, copy.getMapper().getCopyContext());
		ss.setMapper(mapper);
		return ss;
	}
	
	public static DeleteStatement convertToDelete(SchemaContext sc, UpdateStatement us) throws PEException {
		if (us.getLimit() != null)
			throw new PEException("Unable to convert update with limit to delete");
		UpdateStatement copy = CopyVisitor.copy(us);
		DeleteStatement ds = new DeleteStatement().setTruncate(false);
		ds.setTables(copy.getTables())
		  .setWhereClause(copy.getWhereClause());
		ds.getDerivedInfo().take(copy.getDerivedInfo());
		SchemaMapper mapper = new SchemaMapper(copy.getMapper().getOriginals(), ds, copy.getMapper().getCopyContext());
		ds.setMapper(mapper);
		return ds;
	}
	
	public static UpdateStatement convertToUpdate(SelectStatement ss, UpdateStatement guide) throws PEException {
		SelectStatement copy = CopyVisitor.copy(ss);
		List<ExpressionNode> mappedSetExpressions = copy.getMapper().copyForward(guide.getUpdateExpressions());
		UpdateStatement us = new UpdateStatement()
				.setUpdateExpressions(mappedSetExpressions)
				.setLimit(copy.getLimit())
				.setOrderBys(copy.getOrderBys());
		us.setTables(copy.getTables())
		  .setWhereClause(copy.getWhereClause());
								
		SchemaMapper mapper = new SchemaMapper(copy.getMapper().getOriginals(), us, copy.getMapper().getCopyContext());
		us.setMapper(mapper);
		return us;
	}
	
	public static SelectStatement convertDeleteToSelect(DeleteStatement copy, MultiMap<TableInstance,Column<?>> requiredColumns) {
		List<ExpressionNode> proj = new ArrayList<ExpressionNode>();
		for(TableInstance ti : requiredColumns.keySet()) {
			final TableInstance fti = ti;
			proj.addAll(Functional.apply(requiredColumns.get(ti), new UnaryFunction<ExpressionNode, Column<?>>() {

				@Override
				public ExpressionNode evaluate(Column<?> object) {
					return new ExpressionAlias(new ColumnInstance(object, fti), new NameAlias(object.getName().getUnqualified()), false);
				}
				
			}));
		}
		SelectStatement ss = new SelectStatement(new AliasInformation())
			.setTables(copy.getTables()).setProjection(proj).setWhereClause(copy.getWhereClause());
		ss.setOrderBy(copy.getOrderBys());
		ss.setLimit(copy.getLimit());
		ss.getDerivedInfo().take(copy.getDerivedInfo());
		return ss;
	}
}
