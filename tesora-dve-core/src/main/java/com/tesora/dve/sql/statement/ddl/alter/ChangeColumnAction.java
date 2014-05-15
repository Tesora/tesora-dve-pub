// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl.alter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.Pair;

public class ChangeColumnAction extends AbstractAlterColumnAction {

	private static final String AFTER_MODIFIER = "AFTER";

	protected PEColumn oldDefinition;
	protected PEColumn newDefinition;
	protected Pair<String, Name> firstOrAfterSpec;
	
	public ChangeColumnAction(PEColumn oldDef, PEColumn newDef, Pair<String, Name> firstOrAfterSpec) {
		oldDefinition = oldDef;
		newDefinition = newDef;
		newDefinition.normalize();
		this.firstOrAfterSpec = firstOrAfterSpec;
	}

	@Override
	public void refresh(SchemaContext sc, PETable pet) {
		super.refresh(sc, pet);
		// the old definition is relative to backing - reload it now if we have to
		oldDefinition = oldDefinition.getIn(sc, pet);
	}

	
	public PEColumn getOldDefinition() {
		return oldDefinition;
	}
	
	public PEColumn getNewDefinition() {
		return newDefinition;
	}
	
	public Pair<String, Name> getFirstOrAfterSpec() {
		return firstOrAfterSpec;
	}

	@Override
	public AlterTableAction alterTable(SchemaContext sc, PETable tab) {
		PEColumn origDef = oldDefinition.getIn(sc, tab);

		if ((firstOrAfterSpec != null)) {
			final PEColumn insertAtColumn = getInsertAtPositionFromPositionSpec(sc, tab, firstOrAfterSpec);
			if ((insertAtColumn == null) || !insertAtColumn.equals(origDef)) {
				moveColumnsToPosition(sc, tab, Collections.singletonList(origDef), insertAtColumn);
			}
		}

		origDef.take(sc, newDefinition);
		origDef.normalize();

		return null;
	}

	@Override
	public AlterTableAction adapt(SchemaContext sc, PETable actual) {
		return new ChangeColumnAction(oldDefinition, newDefinition, firstOrAfterSpec);
	}

	@Override
	public String isValid(SchemaContext sc, PETable tab) {
		final Name tableName = tab.getName().getUnquotedName();
		if ((firstOrAfterSpec != null) && (firstOrAfterSpec.getSecond() != null)) {
			final Name afterColumnName = firstOrAfterSpec.getSecond();
			final PEColumn afterColumn = tab.lookup(sc, afterColumnName);
			/*
			 * Check if the AFTER column exists and is not same as the modified
			 * one.
			 */
			if ((afterColumn == null) || (afterColumn.equals(oldDefinition))) {
				return "Unknown column '" + afterColumnName.getSQL() + "' in '" + tableName.getSQL() + "'";
			}
		}

		PEColumn extant = oldDefinition.getIn(sc, tab);
		if (extant == null) {
			return "Unknown column '" + oldDefinition.getName().getSQL() + "' in '" + tableName.getSQL() + "'";
		}

		return null;
	}

	@Override
	public boolean isNoop(SchemaContext sc, PETable tab) {
		// assume never for now - really we should check new def against tab
		return false;
	}

	@Override
	public Action getActionKind() {
		return Action.ALTER;
	}

	@Override
	public List<PEColumn> getColumns() {
		return Collections.singletonList(oldDefinition);
	}

	protected void moveColumnsToPosition(final SchemaContext sc, final PETable table, final List<PEColumn> columns, final PEColumn insertAtColumn) {
		final List<PEColumn> tableColumns = new ArrayList<PEColumn>(table.getColumns(sc));

		tableColumns.removeAll(columns);

		if (insertAtColumn != null) {
			tableColumns.addAll(tableColumns.indexOf(insertAtColumn), columns);
		} else {
			tableColumns.addAll(columns);
		}

		table.removeColumns(sc, table.getColumns(sc));

		table.addColumns(sc, tableColumns);
	}

	/**
	 * Get column at the insertion position from [FIRST | AFTER col_name]
	 * clause, or NULL if not specified.
	 */
	private PEColumn getInsertAtPositionFromPositionSpec(final SchemaContext sc, final PETable table, final Pair<String, Name> firstOrAfterSpec) {
		final List<PEColumn> tableColumns = table.getColumns(sc);

		if (tableColumns.isEmpty()) {
			return null;
		}
		
		if (AFTER_MODIFIER.equalsIgnoreCase(firstOrAfterSpec.getFirst())) {
			final PEColumn afterColumn = table.lookup(sc, firstOrAfterSpec.getSecond().getUnqualified().getUnquotedName().get());
			final int insertAtIndex = afterColumn.getPosition() + 1;
			return (insertAtIndex < tableColumns.size()) ? tableColumns.get(insertAtIndex) : null;
		}

		return tableColumns.get(0);
	}
	
}
