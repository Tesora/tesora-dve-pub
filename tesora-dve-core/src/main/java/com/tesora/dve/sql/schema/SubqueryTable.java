// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.WildcardTable;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.types.TempColumnType;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.util.ListSet;

public class SubqueryTable extends PETable {
	
	private boolean hasCardInfo;
	
	private SubqueryTable(SchemaContext pc, Name name, List<PEColumn> cols,
			DistributionVector distVect, PEStorageGroup group, PEDatabase pdb,
			boolean cardInfo) {
		super(pc, name, cols, distVect, group, pdb);
		hasCardInfo = cardInfo;
	}

	@Override
	public boolean isVirtualTable() {
		return true;
	}
	
	@Override
	public boolean hasCardinalityInfo(SchemaContext sc) {
		return hasCardInfo;
	}
	
	@SuppressWarnings("unchecked")
	public static SubqueryTable make(SchemaContext sc, DMLStatement in) {
		List<PEColumn> newColumns = new ArrayList<PEColumn>();
		List<ExpressionNode> projection = null;
		ProjectingStatement ps = (ProjectingStatement) in;
		projection = ps.getProjections().get(0);
		for(int i = 0; i < projection.size(); i++) {
			ExpressionNode en = projection.get(i);
			if (en instanceof ExpressionAlias) {
				ExpressionAlias ea = (ExpressionAlias) en;
				newColumns.add(ea.buildTempColumn(sc));
			} else if (en instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) en;
				newColumns.add(new TempColumn(sc,ci.getColumn().getName(),ci.getColumn().getType()));
			} else if ((en instanceof LiteralExpression) || (en instanceof FunctionCall)) {
				StringBuilder buf = new StringBuilder();
                Singletons.require(HostService.class).getDBNative().getEmitter().emitExpression(sc, en, buf);
				Name name = new UnqualifiedName(buf.toString());
				newColumns.add(new TempColumn(sc,name.getQuotedName(),TempColumnType.TEMP_TYPE));
			} else if (en instanceof WildcardTable) {
				WildcardTable wct = (WildcardTable) en;
				Table<?> theTable = wct.getTableInstance().getTable();
				for(Column<?> c : theTable.getColumns(sc)) {
					newColumns.add(new TempColumn(sc,c.getName(),c.getType()));
				}
			} else {
				throw new SchemaException(Pass.SECOND, "Unable to build name for virtual table");
			}
		}
		UnqualifiedName unq = new UnqualifiedName("vtab" + sc.getNextTable());
		PEDatabase pdb = (PEDatabase) in.getDatabase(sc);
		PEStorageGroup pg = null;
		List<PEStorageGroup> groups = in.getStorageGroups(sc);
		if (groups.size() > 1) {
			pg = virtualTableStorageGroup;
		} else {
			pg = groups.get(0);
		}
		DistributionVector dvect = new DistributionVector(sc,Collections.EMPTY_LIST, Model.RANDOM);
		ListSet<TableKey> allTabs = ps.getAllTableKeys();
		boolean cardInfo = true;
		for(TableKey tk : allTabs) {
			if (!tk.getAbstractTable().hasCardinalityInfo(sc)) {
				cardInfo = false;
				break;
			}
		}
		return new SubqueryTable(sc,unq,newColumns,dvect,pg,pdb,cardInfo);
	}

	private static final PEStorageGroup virtualTableStorageGroup = new PEStorageGroup() {
		
		@Override
		public StorageGroup getPersistent(SchemaContext sc) {
			return null;
		}

		@Override
		public PersistentGroup persistTree(SchemaContext sc) throws PEException {
			return null;
		}

		@Override
		public PEPersistentGroup anySite(SchemaContext sc) throws PEException {
			return null;
		}

		@Override
		public boolean comparable(SchemaContext sc, PEStorageGroup storage) {
			return false;
		}

		@Override
		public boolean isSubsetOf(SchemaContext sc, PEStorageGroup storage) {
			return false;
		}

		@Override
		public boolean isSingleSiteGroup() {
			return false;
		}

		@Override
		public void setCost(int score) throws PEException {
		}

		@Override
		public boolean isTempGroup() {
			return true;
		}

		@Override
		public PEStorageGroup getPEStorageGroup(SchemaContext sc) {
			return null;
		}

		@Override
		public StorageGroup getScheduledGroup(SchemaContext sc) {
			return null;
		}
	};
}
