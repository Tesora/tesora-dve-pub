// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.IndexType;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.TempTableDeclHints;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ActualLiteralExpression;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TempTableInstance;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.modifiers.TypeModifier;
import com.tesora.dve.sql.schema.modifiers.TypeModifierKind;
import com.tesora.dve.sql.schema.types.BasicType;
import com.tesora.dve.sql.schema.types.TempColumnType;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.transform.SchemaMapper;
import com.tesora.dve.sql.transform.TableInstanceCollector;
import com.tesora.dve.sql.util.ListSet;

public final class TempTable extends PETable {
    static final TempColumnType NULL_COLUMN_TYPE = new TempColumnType(
            BasicType.buildType("SMALLINT", 0,
                    Collections.singletonList(new TypeModifier(TypeModifierKind.UNSIGNED))).normalize()
    );

	// the select this was based on
	protected ProjectingStatement sourceStatement;
	// forwarding information.  we have a few types of forwarding:
	// column to column
	// table to table
	// expression to column
	private Map<ColumnKey, PEColumn> forwardColumns;
	private Map<TableKey, TableKey> forwardTables;
	private Map<ExpressionKey, PEColumn> forwardExpressions;

	// indexed by input projection - value is output column
	private PEColumn[] columnForOffset;
	
	// we use an index into the connection values for the name
	private int index;
	
	private long rowCount;
	// total number of keys added, used to generate unique key names
	private int totalAddedKeys;
	
	private TempTableDeclHints declHint = new TempTableDeclHints();
	private boolean explicitlyDeclared = false;	
	
	private TempTable(SchemaContext pc, int nameIndex, List<PEColumn> cols, ProjectingStatement source, 
			DistributionVector distVect,
			PEStorageGroup storageGroup,
			PEDatabase ofDatabase,
			Map<ColumnKey,PEColumn> columnForwarding,
			PEColumn[] colForOff,
			Set<TableInstance> sourceTables, // all forward to this table
			Map<ExpressionKey, PEColumn> forwardExprs,
            List<PEColumn> nullColumns,
            long rowCount) throws PEException {
		super(pc, null, cols, distVect, storageGroup, ofDatabase);
		index = nameIndex;
		sourceStatement = source;
		forwardColumns = columnForwarding;
		forwardExpressions = forwardExprs;
		columnForOffset = colForOff;
		totalAddedKeys = 0;
		this.rowCount = rowCount;		
        TableKey tk = new TableKey(this, 0);
		forwardTables = new HashMap<TableKey, TableKey>();
		for(TableInstance ti : sourceTables)
			forwardTables.put(ti.getTableKey(), tk);
		// record any hints
		for(PEColumn c : cols) {
			String cn = c.getName().getUnquotedName().get();
			if (c.getType().isZeroFill()) 
				declHint.addZeroFilled(cn);
			if (c.getType().getCollation() != null) 
				declHint.addCollation(cn, c.getType().getCollation().getUnquotedName().get());
			if (c.getType().getCharset() != null) 
				declHint.addCharset(cn, c.getType().getCharset().getUnquotedName().get());			
		}

        //add better than nothing hint for any null columns
        if (nullColumns == null)
            nullColumns = new ArrayList<PEColumn>();
        this.forceDefinitions(pc, nullColumns );

		// also, if the table is distributed on explicit columns - add index hints on those columns
		if (distVect.usesColumns(pc)) {
			addKey(pc,distVect.getColumns(pc));
		}
		if (distVect.isRandom() && storageGroup.isTempGroup()) {
			throw new SchemaException(Pass.PLANNER, "Invalid target group random temp table: dynamic group");
		}
	}
	
	private void addKey(SchemaContext sc, List<PEColumn> onCols) {
		List<PEKeyColumn> kc = new ArrayList<PEKeyColumn>();
		for(PEColumn pec : onCols) {
			if (pec.getType().getBaseType() == null || pec.getType().isStringType() || pec.getType().isBinaryType())
				continue;
			if (kc.size() >= Singletons.require(HostService.class).getDBNative().getMaxNumColsInIndex()) {
				break;
			}
			kc.add(new PEKeyColumn(pec,null,-1L));
		}
		// we can do on-the-fly filtering here.  we seek to preserve the following:
		// [1] there are no duplicate keys
		// [2] if we have prefix keys we toss them in favor of longer keys
		
		if (!kc.isEmpty()) {
			PEKey pek = new PEKey(new UnqualifiedName("tk" + (++totalAddedKeys)),IndexType.BTREE,kc,null);
			// search existing keys for a prefix
			List<PEKey> toRemove = new ArrayList<PEKey>();
			for(PEKey e : getKeys(sc)) {
				// e is longer than pek - so pek is redundant.
				if (PEKey.samePrefix(pek, e))
					return;
				if (PEKey.samePrefix(e, pek)) {
					// pek makes e redundant.
					toRemove.add(e);
				}
			}
			addKey(sc,pek,false);
			for(PEKey rkey : toRemove) {
				getKeys(sc).remove(rkey);
			}
		}
	}
	
	public void addConstraint(SchemaContext sc, ConstraintType type, List<PEColumn> onCols) {
		final List<PEKeyColumn> kc = new ArrayList<PEKeyColumn>();
		for (final PEColumn pec : onCols) {
			kc.add(new PEKeyColumn(pec, null, -1L));
		}

		final PEKey pek = new PEKey(new UnqualifiedName("tk" + (++totalAddedKeys)), IndexType.BTREE, kc, null);
		pek.setConstraint(type);
		addKey(sc, pek, false);
	}

	public PEColumn getNewColumn(ColumnInstance oldColumn) {
		PEColumn out = forwardColumns.get(oldColumn.getColumnKey());
		if (out == null)
			throw new SchemaException(Pass.REWRITER, "Unable to find new column in temp table " + getName().get() + " for old column " + oldColumn);
		return out;
	}
		
	@Override
	public boolean isTempTable() {
		return true;
	}

	@Override
	public long getTableSizeEstimate(SchemaContext sc) {
		return rowCount;
	}
	
	@Override
	public boolean hasCardinalityInfo(SchemaContext sc) {
		return rowCount > -1;
	}
	
	public boolean isExplicitlyDeclared() {
		return explicitlyDeclared;
	}
	
	@Override
	public Name getName(SchemaContext sc) {
		return sc._getValues().getTempTableName(index);
	}
	
	@Override
	public String toString() {
		return getName(SchemaContext.threadContext.get()).get();
	}

	public void noteJoinedColumns(SchemaContext sc, List<PEColumn> pec) {
		ListSet<PEColumn> uniqued = new ListSet<PEColumn>(pec);
		addKey(sc,uniqued);
	}

	public TempTableDeclHints getHints(SchemaContext sc) {
		// if we have keys we have to add them to the hints
		final List<PEKey> keys = getKeys(sc);
		if (!keys.isEmpty()) {
			int acc = 0;
			// the unique constraints must be declared for correctness.
			// the other keys are for performance, not correctness.
			HashSet<PEKey> handled = new HashSet<PEKey>();
			List<PEKey> uniques = getUniqueKeys(sc);
			for(PEKey pek : uniques) {
				if (declHint.addUniqueKey(pek.buildHint())) {
					acc += pek.getIndexSize();
					handled.add(pek);
				}
			}
			for(PEKey pek : keys) {
				if (handled.add(pek)) {
					int peksize = pek.getIndexSize();
					if (((acc + peksize) <= 1000) && declHint.addIndex(pek.buildHint())) {
						acc += peksize;
					}
				}
			}
		}

		return declHint;
		
	}
	
    protected void forceDefinitions(SchemaContext sc, Collection<PEColumn> columns) {
        if (columns.size() > 0){
            SchemaContext mutable = SchemaContext.makeMutableIndependentContext(sc);
            mutable.setValues(sc._getValues());
            for(PEColumn p : columns) {
                UserColumn uc = p.getPersistent(mutable);
                declHint.addOverrideDecl(p.getName().getUnquotedName().get(), uc);
            }
        }
    }

    //forces all defined column definitions
	public void forceDefinitions(SchemaContext sc) {
        forceDefinitions( sc, getColumns(sc) );
	}
	
	@Override
	public void setFrozen() {
		sourceStatement = null;
		forwardColumns = null;
		forwardExpressions = null;
		forwardTables = null;
	}
	
	@Override
	public Name getName() {
		throw new SchemaException(Pass.PLANNER, "Temp table requires context");
	}
	
	// used in show table status support
	@SuppressWarnings({ "unchecked" })
	public static TempTable buildAdHoc(SchemaContext sc, PEDatabase db, List<PEColumn> newColumns, 
			Model model, List<PEColumn> distCols, PEStorageGroup group,
			boolean explicit) throws PEException {
		DistributionVector dv = new DistributionVector(sc, distCols, model, true);
		TempTable tt = new TempTable(sc,
				sc.getValueManager().allocateTempTableName(sc),
				newColumns, null, dv, group, db, Collections.EMPTY_MAP, null, Collections.EMPTY_SET, Collections.EMPTY_MAP, Collections.EMPTY_LIST, -1);
		tt.explicitlyDeclared = explicit;
		return tt;
	}
	
	// we always need the src stmt, dv col offsets, the model, the storage group and the row estimate
	// but the invisible cols, vector range - not so much.
	
	public static TempTable build(SchemaContext sc, ProjectingStatement in,
			TempTableCreateOptions options) throws PEException {
		return buildFromSelect(sc,in,options.getDistVectColumns(),options.getInvisibleColumns(),
				options.getModel(),options.getRange(),
				options.getGroup(), options.getRowcount());
	}
	
	@SuppressWarnings("unchecked")
	public static TempTable buildFromSelect(SchemaContext sc, ProjectingStatement in, 
			List<Integer> dvcolOffsets,
			List<Integer> invisibleCols,
			Model model, VectorRange rangeDist,
			PEStorageGroup storageGroup,
			long rowEstimate) throws PEException {
		List<PEColumn> cols = new ArrayList<PEColumn>();
        List<PEColumn> nulledColumns = new ArrayList<PEColumn>();
		ListSet<TableInstance> sourceTables = TableInstanceCollector.getInstances(in);
		final HashMap<ColumnKey, PEColumn> forwardCols = new HashMap<ColumnKey, PEColumn>();
		HashMap<ExpressionKey, PEColumn> forwardExprs = new HashMap<ExpressionKey, PEColumn>();
		List<PEColumn> mappingOffsets = new ArrayList<PEColumn>();
		HashSet<Integer> invisible = new HashSet<Integer>((invisibleCols == null ? Collections.EMPTY_LIST : invisibleCols));

		ListSet<Integer> offsets = new ListSet<Integer>(dvcolOffsets == null ? Collections.EMPTY_LIST : dvcolOffsets);
		Map<Integer, PEColumn> dvcols = new HashMap<Integer, PEColumn>();
		
		List<ExpressionNode> projection = in.getProjections().get(0);
		
		for(int i = 0; i < projection.size(); i++) {
			ExpressionNode e = projection.get(i);
			ExpressionAlias ea = (ExpressionAlias)e;
			ExpressionNode targ = ea.getTarget();
			ColumnInstance ci = null;
			boolean invisibleColumn = invisible.contains(i);
			if (targ instanceof ColumnInstance) {
				ci = (ColumnInstance) targ;
			}

			PEColumn nc = null;
			if (ci != null && !invisibleColumn)
				nc = forwardCols.get(ci.getColumnKey());
			if (nc == null) {
                if ((targ instanceof ActualLiteralExpression) && ((ActualLiteralExpression)targ).isNullLiteral()){
                    //Fix for PE-881.
                    //The NullLiteralColumnTransformFactory usually relocates null expressions, since we don't know what column type is expected by the next step.
                    //Hopefully we have a UNION, which aggressively casts the output, rather than complain about mismatched column types.
                    nc = ea.buildTempColumn(sc,NULL_COLUMN_TYPE );
                    nulledColumns.add(nc);
                } else {
				    nc = ea.buildTempColumn(sc);
                }
				cols.add(nc);
			}
			mappingOffsets.add(nc);
			ExpressionKey eak = new ExpressionKey(targ);
			forwardExprs.put(eak, nc);
			if (ci != null && !invisibleColumn) {
				forwardCols.put(ci.getColumnKey(), nc);
			}
			if (offsets.contains(i)) {
				dvcols.put(i,nc);
			}
		}
		Model dvModel = null;
		if (rangeDist != null)
			dvModel = Model.RANGE;
		else
			dvModel = model;
		List<PEColumn> mappedDVCols = new ArrayList<PEColumn>();
		for(Integer i : offsets) 
			mappedDVCols.add(dvcols.get(i));
		DistributionVector dv = null;
		if (rangeDist != null)
			dv = new RangeDistributionVector(sc, mappedDVCols, true, rangeDist);
		else
			dv = new DistributionVector(sc, mappedDVCols, dvModel, true);
		TempTable tt = new TempTable(sc,
				sc.getValueManager().allocateTempTableName(sc),
				cols, in, dv, storageGroup, (PEDatabase)in.getDatabase(sc),
				forwardCols,
				(PEColumn[])mappingOffsets.toArray(new PEColumn[0]),
				sourceTables,
				forwardExprs, nulledColumns, rowEstimate);
		return tt;
	}
	
	// the source select is the source; the new select is treated as a copy.
	public SelectStatement buildSelect(SchemaContext pc) throws PEException {
		CopyContext cc = new CopyContext("TempTable.buildSelect");
		TableInstance ti = new TempTableInstance(pc, this);
		// set up the copy context with the column and table mappings
		for(TableKey tk : forwardTables.keySet()) {
			cc.put(tk, ti.getTableKey());
		}
		for(Map.Entry<ColumnKey, PEColumn> me : forwardColumns.entrySet()) {
			cc.put(me.getKey(), new ColumnKey(ti.getTableKey(), me.getValue()));
		}
		for(Map.Entry<ExpressionKey, PEColumn> me : forwardExpressions.entrySet()) {
			cc.put(me.getKey(), new ColumnKey(ti.getTableKey(), me.getValue()));
		}
		ArrayList<ExpressionNode> proj = new ArrayList<ExpressionNode>();
		HashSet<String> aliases = new HashSet<String>();
		// we're reconstructing the original projection - use the offsets instead
		if (columnForOffset == null) {
			for(PEColumn c : getColumns(pc)) {
				ExpressionNode repl = buildProjection(c,ti,null, aliases);
				proj.add(repl);				
			}
		} else {
			List<ExpressionNode> origProj = sourceStatement.getProjections().get(0);
			for(int i = 0; i < columnForOffset.length; i++) {
				PEColumn c = columnForOffset[i];
				ExpressionNode orig = origProj.get(i);
				ExpressionNode repl = buildProjection(c,ti,orig,aliases);
				proj.add(repl);
			}
		}
		AliasInformation ai = new AliasInformation();
		ai.addAliases(aliases);
		SelectStatement out = new SelectStatement(ai)
			.setTables(ti).setProjection(proj);
		out.getDerivedInfo().addLocalTable(ti.getTableKey());
		if (sourceStatement != null) {
			SchemaMapper mapper = new SchemaMapper(Collections.singleton((DMLStatement)sourceStatement), out, cc);
			out.setMapper(mapper);
		}
		return out;
	}

	private ExpressionNode buildProjection(PEColumn c, TableInstance ti, ExpressionNode original, Set<String> aliases) {
		ExpressionNode proj = c.buildProjection(ti);
		if (original instanceof ExpressionAlias) {
			ExpressionAlias ea = (ExpressionAlias) original;
			aliases.add(ea.getAlias().get());
			if (!ea.isSynthetic()) {
				return new ExpressionAlias(proj, ea.getAlias(), false);
			}
		}
		return proj;
	}
	
	// for updates we need to build out the catalog entities, but temp tables don't exist in the catalog
	@Override
	protected UserTable lookup(SchemaContext pc) throws PEException {
		return null;
	}
	
	@Override
	protected UserTable createEmptyNew(SchemaContext pc) throws PEException {
		String persistName = Singletons.require(HostService.class).getDBNative().getEmitter().getPersistentName(pc, this);
		UserDatabase pdb = getPEDatabase(pc).persistTree(pc); 
		DistributionModel dm = getDistributionVector(pc).persistTree(pc);
		UserTable ut = pc.getCatalog().createTempTable(pdb, persistName, dm);
		pc.getSaveContext().add(this,ut);
		return ut;
	}
		
	@Override
	protected boolean isTemporary() {
		return true;
	}	
}
