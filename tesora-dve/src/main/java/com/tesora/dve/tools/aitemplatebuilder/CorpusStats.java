package com.tesora.dve.tools.aitemplatebuilder;

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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats.ForeignRelationship;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats.TableColumn;
import com.tesora.dve.tools.analyzer.stats.EquijoinInfo;
import com.tesora.dve.tools.analyzer.stats.StatementAnalysis;
import com.tesora.dve.tools.analyzer.stats.StatsVisitor;

public class CorpusStats implements StatsVisitor {

	public static enum StatementType {
		SELECT("SELECT"),
		INSERT("INSERT"),
		UPDATE("UPDATE"),
		DELETE("DELETE"),
		JOIN("JOIN"),
		ORDERBY("ORDERBY");

		private final String name;

		private StatementType(final String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}
	}

	public static abstract class Relationship {

		public static class RelationshipSpecification {

			private static final String FK_SQL = "FOREIGN KEY";

			public static final RelationshipSpecification FK_SPECIFICATION = new RelationshipSpecification();
			private final JoinSpecification joinType;

			private RelationshipSpecification() {
				this.joinType = null;
			}

			public RelationshipSpecification(final JoinSpecification joinType) {
				this.joinType = joinType;
			}

			public boolean isJoin() {
				return (this.joinType != null);
			}

			public boolean isInnerJoin() {
				return this.isJoin() && this.joinType.isInnerJoin();
			}

			public boolean isCrossJoin() {
				return this.isJoin() && this.joinType.isCrossJoin();
			}

			public boolean isOuterJoin() {
				return this.isJoin() && this.joinType.isOuterJoin();
			}

			public boolean isLeftOuterJoin() {
				return this.isJoin() && this.joinType.isLeftOuterJoin();
			}

			public boolean isRightOuterJoin() {
				return this.isJoin() && this.joinType.isRightOuterJoin();
			}

			public boolean isFullOuterJoin() {
				return this.isJoin() && this.joinType.isFullOuterJoin();
			}

			@Override
			public int hashCode() {
				if (this.isJoin()) {
					return this.joinType.hashCode();
				}

				return FK_SQL.hashCode();
			}

			@Override
			public boolean equals(Object other) {
				if (other == this) {
					return true;
				} else if (other == null) {
					return false;
				} else if (!(other instanceof RelationshipSpecification)) {
					return false;
				}

				final RelationshipSpecification otherSpecification = (RelationshipSpecification) other;

				return ObjectUtils.equals(this.joinType, otherSpecification.joinType);
			}

			@Override
			public String toString() {
				if (this.isJoin()) {
					return this.joinType.getSQL().concat(" JOIN");
				}

				return FK_SQL;
			}

		}

		public abstract TableStats getLHS();

		public abstract TableStats getRHS();

		public abstract Set<TableColumn> getLeftColumns();

		public abstract Set<TableColumn> getRightColumns();

		public abstract Set<Pair<TableColumn, TableColumn>> getColumnPairs();

		public abstract long getFrequency();

		public abstract RelationshipSpecification getType();

		/**
		 * Check if the relationship has range compatible collocatable types.
		 */
		public boolean isRangeCompatible() {
			final Set<Pair<TableColumn, TableColumn>> columnPairs = this.getColumnPairs();
			for (final Pair<TableColumn, TableColumn> columnPair : columnPairs) {
				final Type leftType = columnPair.getFirst().getType();
				final Type rightType = columnPair.getSecond().getType();
				if (!CommonRange.isRangeCompatible(leftType, rightType)) {
					return false;
				}
			}

			return true;
		}

		public boolean involves(final TableStats table) {
			return this.getLHS().equals(table) || this.getRHS().equals(table);
		}

		@Override
		public boolean equals(Object other) {
			if (other == null) {
				return false;
			} else if (this == other) {
				return true;
			} else if (!(other instanceof Relationship)) {
				return false;
			}

			final Relationship otherRelationship = (Relationship) other;
			return this.getType().equals(otherRelationship.getType()) && this.getColumnPairs().equals(otherRelationship.getColumnPairs());
		}

		@Override
		public int hashCode() {
			final RelationshipSpecification type = this.getType();
			final Set<Pair<TableColumn, TableColumn>> columnPairs = this.getColumnPairs();

			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((type != null) ? type.hashCode() : 0);
			result = (prime * result) + ((columnPairs != null) ? columnPairs.hashCode() : 0);
			return result;
		}
	}

	public final class TableStats implements Comparable<TableStats> {

		/**
		 * Same as com.tesora.dve.sql.schema.Column<T>, but includes the parent
		 * table in
		 * column comparisons. If you want to compare only on name and type
		 * retrieve the enclosed com.tesora.dve.sql.schema.Column<T> instance.
		 */
		public final class TableColumn {

			private final com.tesora.dve.sql.schema.Column<?> column;

			private TableColumn(final com.tesora.dve.sql.schema.Column<?> column) {
				this.column = column;
			}

			public com.tesora.dve.sql.schema.Column<?> getColumnInstance() {
				return this.column;
			}

			public Name getName() {
				return this.column.getName();
			}

			public Type getType() {
				return this.column.getType();
			}

			@Override
			public boolean equals(Object other) {
				if (other == null) {
					return false;
				} else if (this == other) {
					return true;
				} else if (!(other instanceof TableColumn)) {
					return false;
				}

				@SuppressWarnings("unchecked")
				final TableColumn otherColumn = (TableColumn) other;
				if (this.getColumnInstance().getTable().equals(otherColumn.getColumnInstance().getTable())) {
					return this.getColumnInstance().equals(otherColumn.getColumnInstance());
				}

				return false;
			}

			@Override
			public int hashCode() {
				final Pair<Table<?>, Column<?>> pair = new Pair<Table<?>, Column<?>>(
						this.getColumnInstance().getTable(),
						this.getColumnInstance());
				return pair.hashCode();
			}

			@Override
			public String toString() {
				return TableStats.this.getFullTableName().concat(".").concat(this.getColumnInstance().getName().get());
			}

		}

		public final class ForeignRelationship extends Relationship {
			private final TableStats targetTable;
			private final Set<TableColumn> foreignColumns = new LinkedHashSet<TableColumn>();
			private final Set<TableColumn> targetColumns = new LinkedHashSet<TableColumn>();
			final Set<Pair<TableColumn, TableColumn>> columnPairs = new HashSet<Pair<TableColumn, TableColumn>>();
			private final String signature;

			private <T extends Column<?>> ForeignRelationship(final TableStats targetTable, final List<T> foreignColumns, final List<T> targetColumns) {
				assert (foreignColumns.size() == targetColumns.size());

				this.targetTable = targetTable;

				final Iterator<T> foreignColumnIt = foreignColumns.iterator();
				final Iterator<T> targetColumnIt = targetColumns.iterator();
				while (foreignColumnIt.hasNext()) {
					final TableColumn foreignColumn = TableStats.this.new TableColumn(foreignColumnIt.next());
					final TableColumn targetColumn = targetTable.new TableColumn(targetColumnIt.next());

					this.foreignColumns.add(foreignColumn);
					this.targetColumns.add(targetColumn);
					this.columnPairs.add(new Pair<TableColumn, TableColumn>(foreignColumn, targetColumn));
				}

				this.signature = getSignature();
			}

			public Set<TableColumn> getForeignColumns() {
				return getLeftColumns();
			}

			public Set<TableColumn> getTargetColumns() {
				return getRightColumns();
			}

			@Override
			public TableStats getLHS() {
				return TableStats.this;
			}

			@Override
			public TableStats getRHS() {
				return this.targetTable;
			}

			@Override
			public Set<TableColumn> getLeftColumns() {
				return Collections.unmodifiableSet(this.foreignColumns);
			}

			@Override
			public Set<TableColumn> getRightColumns() {
				return Collections.unmodifiableSet(this.targetColumns);
			}

			@Override
			public Set<Pair<TableColumn, TableColumn>> getColumnPairs() {
				return this.columnPairs;
			}

			@Override
			public long getFrequency() {
				return 1;
			}

			@Override
			public RelationshipSpecification getType() {
				return RelationshipSpecification.FK_SPECIFICATION;
			}

			@Override
			public String toString() {
				final StringBuilder value = new StringBuilder();
				value.append("[").append(this.signature).append("]");
				return value.toString();
			}

			private String getSignature() {
				final StringBuilder signature = new StringBuilder();
				signature.append(RelationshipSpecification.FK_SPECIFICATION).append(" ")
						.append(this.getLHS().getFullTableName()).append(" (").append(StringUtils.join(this.getForeignColumns(), ", "))
						.append(") REFERENCES ")
						.append(this.getRHS().getFullTableName()).append(" (").append(StringUtils.join(this.getTargetColumns(), ", "))
						.append(")");
				return signature.toString();
			}

		}

		private final String schemaName;
		private final String tableName;
		private final QualifiedName fullName;
		private final Map<StatementType, Long> statementStats = new HashMap<StatementType, Long>();
		private final Map<TableColumn, Long> identColumnStats = new HashMap<TableColumn, Long>();
		private final Map<TableColumn, Long> groupByColumnStats = new HashMap<TableColumn, Long>();
		private final Set<TableColumn> columns = new HashSet<TableColumn>();
		private final Set<ForeignRelationship> forwardRelationships = new HashSet<ForeignRelationship>();
		private final Set<ForeignRelationship> backwardRelationships = new HashSet<ForeignRelationship>();
		private long cardinality;

		private TemplateItem distributionModel;

		public TableStats(final String databaseName, final String tableName, final long cardinality) {
			this.schemaName = databaseName;
			this.tableName = tableName;
			this.fullName = toQualifiedName(databaseName, tableName);
			this.cardinality = cardinality;
		}

		public String getSchemaName() {
			return this.schemaName;
		}

		public String getTableName() {
			return this.tableName;
		}

		public QualifiedName getQualifiedTableName() {
			return this.fullName;
		}

		public String getFullTableName() {
			return this.fullName.getUnquotedName().get();
		}

		public TemplateItem getTableDistributionModel() {
			return this.distributionModel;
		}

		public void setTableDistributionModel(final TemplateItem model) {
			this.distributionModel = model;
		}

		public boolean hasStatements() {
			for (final StatementType type : StatementType.values()) {
				if (this.getStatementCounts(type) > 0) {
					return true;
				}
			}

			return false;
		}

		public long getTableCardinality() {
			return this.cardinality;
		}

		/**
		 * Extrapolate table cardinality "futureExtrapolationConstant" number of
		 * epochs in the future. One epoch is the time coverage of data
		 * available for analysis.
		 */
		public long getPredictedFutureCardinality() {
			return predictFutureTableCardinality(this);
		}

		/**
		 * Count all SELECT, INSERT, UPDATE and DELETE statements on this table.
		 */
		public long getTotalStatementCount() {
			long total = 0;
			for (final StatementType type : Arrays.asList(
					StatementType.SELECT,
					StatementType.INSERT,
					StatementType.UPDATE,
					StatementType.DELETE)) {
				total += this.getStatementCounts(type);
			}

			return total;
		}

		public long getStatementCounts(final StatementType type) {
			final Long count = this.statementStats.get(type);
			if (count == null) {
				return 0;
			}
			return count;
		}

		public Map<TableColumn, Long> getIdentColumns() {
			return Collections.unmodifiableMap(this.identColumnStats);
		}

		public boolean hasIdentColumns() {
			return !this.identColumnStats.isEmpty();
		}

		public Map<TableColumn, Long> getGroupByColumns() {
			return Collections.unmodifiableMap(this.groupByColumnStats);
		}

		public boolean hasGroupByColumns() {
			return !this.groupByColumnStats.isEmpty();
		}

		public Set<TableColumn> getTableColumns() {
			return Collections.unmodifiableSet(this.columns);
		}

		public Set<TableStats> getReferencingForeignTables() {
			return getUniqueForeignTables(this.backwardRelationships);
		}

		public Set<TableStats> getReferencedForeignTables() {
			return getUniqueForeignTables(this.forwardRelationships);
		}

		public Set<Set<TableColumn>> getUniqueForeignColumnGroups() {
			return getUniqueRelationshipColumnGroups(this.forwardRelationships);
		}

		public Set<Set<TableColumn>> getUniqueTargetColumnGroups() {
			return getUniqueRelationshipColumnGroups(this.backwardRelationships);
		}

		public Set<ForeignRelationship> getForwardRelationships() {
			return Collections.unmodifiableSet(this.forwardRelationships);
		}

		public Set<ForeignRelationship> getBackwardRelationships() {
			return Collections.unmodifiableSet(this.backwardRelationships);
		}

		public void addForwardRelationships(final ForeignRelationship relationship) {
			this.forwardRelationships.add(relationship);
		}

		public void addBackwardRelationships(final ForeignRelationship relationship) {
			this.backwardRelationships.add(relationship);
		}

		public boolean hasForeignRelationship() {
			return !this.forwardRelationships.isEmpty() || !this.backwardRelationships.isEmpty();
		}

		protected void bumpCount(final StatementType type, final int increment) {
			Long count = this.statementStats.get(type);
			if (count == null) {
				count = new Long(increment);
			} else {
				count += increment;
			}
			this.statementStats.put(type, count);
		}

		protected void bumpCount(final TableColumn identColumn, final int increment, final Map<TableColumn, Long> columnStats) {
			Long count = columnStats.get(identColumn);
			if (count == null) {
				count = new Long(increment);
			} else {
				count += increment;
			}
			columnStats.put(identColumn, count);
		}

		protected void setTableCardinality(final long cardinality) {
			this.cardinality = cardinality;
		}

		@Override
		public boolean equals(Object other) {
			if (other == null) {
				return false;
			} else if (this == other) {
				return true;
			} else if (!(other instanceof TableStats)) {
				return false;
			}

			return this.fullName.equals(((TableStats) other).fullName);
		}

		@Override
		public int hashCode() {
			return this.fullName.hashCode();
		}

		@Override
		public int compareTo(TableStats other) {
			return this.fullName.getUnquotedName().get().compareTo(other.fullName.getUnquotedName().get());
		}

		@Override
		public String toString() {
			final StringBuilder value = new StringBuilder();
			final List<String> tableProperties = new ArrayList<String>();
			tableProperties.add("C:" + String.valueOf(this.getTableCardinality()));
			for (final StatementType type : StatementType.values()) {
				tableProperties.add(type.getName().substring(0, 1).concat(":").concat(String.valueOf(this.getStatementCounts(type)).concat("x")));
			}
			return value.append("[").append(this.getFullTableName()).append(": ").append(StringUtils.join(tableProperties, ", ")).append("]").toString();
		}

		private Set<Set<TableColumn>> getUniqueRelationshipColumnGroups(final Set<ForeignRelationship> relationships) {
			final Set<Set<TableColumn>> columns = new HashSet<Set<TableColumn>>();
			for (final ForeignRelationship item : relationships) {
				final Set<TableColumn> group = new HashSet<TableColumn>();
				for (final Pair<TableColumn, TableColumn> pair : item.getColumnPairs()) {
					group.add(pair.getFirst());
				}
				columns.add(group);
			}

			return columns;
		}

		private Set<TableStats> getUniqueForeignTables(final Set<ForeignRelationship> relationships) {
			final Set<TableStats> tables = new HashSet<TableStats>();
			for (final ForeignRelationship item : relationships) {
				tables.add(item.getRHS());
			}

			return tables;
		}
	}

	private static QualifiedName toQualifiedName(final String... parts) {
		final List<UnqualifiedName> nameParts = new ArrayList<UnqualifiedName>();
		for (final String item : parts) {
			nameParts.add(new UnqualifiedName(item));
		}
		return new QualifiedName(nameParts);
	}

	public final class JoinStats extends Relationship {

		private final Pair<TableColumn, TableColumn> joinColumns;
		private final TableStats lhs;
		private final TableStats rhs;
		private final RelationshipSpecification type;
		private long count;
		private final String signature;

		private <T extends Column<?>> JoinStats(final JoinSpecification joinType, final Pair<T, T> joinColumns, final long count, final SchemaContext context) {
			final Column<?> leftColumn = joinColumns.getFirst();
			final Column<?> rightColumn = joinColumns.getSecond();
			this.lhs = getStats(leftColumn.getTable(), context);
			this.rhs = getStats(rightColumn.getTable(), context);
			this.joinColumns = new Pair<TableColumn, TableColumn>(this.lhs.new TableColumn(leftColumn), this.rhs.new TableColumn(rightColumn));
			this.type = new RelationshipSpecification(joinType);
			this.count = count;
			this.signature = getSignature();
		}

		private JoinStats(final JoinStats other, final JoinSpecification joinType) {
			this.joinColumns = other.joinColumns;
			this.lhs = other.lhs;
			this.rhs = other.rhs;
			this.type = new RelationshipSpecification(joinType);
			this.count = other.count;
			this.signature = getSignature();
		}

		@Override
		public TableStats getLHS() {
			return this.lhs;
		}

		@Override
		public TableStats getRHS() {
			return this.rhs;
		}

		@Override
		public Set<TableColumn> getLeftColumns() {
			return Collections.<TableColumn> singleton(this.joinColumns.getFirst());
		}

		@Override
		public Set<TableColumn> getRightColumns() {
			return Collections.<TableColumn> singleton(this.joinColumns.getSecond());
		}

		@Override
		public Set<Pair<TableColumn, TableColumn>> getColumnPairs() {
			return Collections.singleton(this.joinColumns);
		}

		@Override
		public long getFrequency() {
			return this.count;
		}

		@Override
		public RelationshipSpecification getType() {
			return this.type;
		}

		public void bumpJoinCount(final long increment) {
			this.count += increment;
		}

		public boolean isFkCompatible() {

			/*
			 * This join is not involved in a foreign key relationship and
			 * therefore should always be safe to collocate.
			 */
			if (!this.lhs.hasForeignRelationship() && !this.rhs.hasForeignRelationship()) {
				return true;
			}

			/*
			 * This join is involved in a foreign key relationship.
			 * 
			 * If only one side is involved, that side has to to be joined
			 * on any of the involved columns.
			 * 
			 * If both sides are involved the join has to exactly follow the
			 * relationship between the two tables.
			 */
			if (this.lhs.hasForeignRelationship() && !this.rhs.hasForeignRelationship()) { // Only LHS involved.
				final Set<Set<TableColumn>> leftForeignColumns = this.lhs.getUniqueForeignColumnGroups();
				final Set<Set<TableColumn>> leftTargetColumns = this.lhs.getUniqueTargetColumnGroups();

				return (leftForeignColumns.contains(this.getLeftColumns()) || leftTargetColumns.contains(this.getLeftColumns()));

			} else if (!this.lhs.hasForeignRelationship() && this.rhs.hasForeignRelationship()) { // Only RHS involved.
				final Set<Set<TableColumn>> rigthForeignColumns = this.rhs.getUniqueForeignColumnGroups();
				final Set<Set<TableColumn>> rigthTargetColumns = this.rhs.getUniqueTargetColumnGroups();

				return (rigthForeignColumns.contains(this.getRightColumns()) || rigthTargetColumns.contains(this.getRightColumns()));
			} else { // Both sides involved.

				/* It should be enough to only check relationships on one side. */
				final Set<ForeignRelationship> relationships = new HashSet<ForeignRelationship>();
				relationships.addAll(this.lhs.getForwardRelationships());
				relationships.addAll(this.lhs.getBackwardRelationships());

				final Set<Pair<TableColumn, TableColumn>> joinColumnPairs = this.getColumnPairs();
				for (final ForeignRelationship relationship : relationships) {
					final Set<Pair<TableColumn, TableColumn>> relationshipColumnPairs = relationship.getColumnPairs();
					final Set<Pair<TableColumn, TableColumn>> relationshipColumnPairsReversed = new HashSet<Pair<TableColumn, TableColumn>>();

					for (final Pair<TableColumn, TableColumn> pair : relationshipColumnPairs) {
						relationshipColumnPairsReversed.add(pair.getReverse());
					}

					if (joinColumnPairs.equals(relationshipColumnPairs) || joinColumnPairs.equals(relationshipColumnPairsReversed)) {
						return true;
					}
				}

				return false;
			}
		}

		public JoinStats cloneAsInnerJoin() {
			if (!this.type.isInnerJoin()) {
				return new JoinStats(this, JoinSpecification.INNER_JOIN);
			}

			return this;
		}

		@Override
		public String toString() {
			final StringBuilder value = new StringBuilder();
			value.append("[").append(this.signature).append("; F:").append(this.getFrequency()).append("x]");
			return value.toString();
		}

		private String getSignature() {
			final StringBuilder signature = new StringBuilder();
			signature.append(this.getLHS().getFullTableName()).append(" ").append(this.type).append(" ").append(this.getRHS().getFullTableName())
					.append(" ON ").append(this.getLeftColumn()).append(" = ").append(this.getRightColumn());
			return signature.toString();
		}

		private TableColumn getLeftColumn() {
			return this.joinColumns.getFirst();
		}

		private TableColumn getRightColumn() {
			return this.joinColumns.getSecond();
		}
	}

	private final String corpusName;
	private final int corpusScaleFactor;
	private final SortedMap<QualifiedName, TableStats> corpusStats = new TreeMap<QualifiedName, TableStats>(new Comparator<QualifiedName>() {
		@Override
		public int compare(QualifiedName a, QualifiedName b) {
			return a.getUnquotedName().get().compareTo(b.getUnquotedName().get());
		}
	});
	private final Set<JoinStats> joins = new HashSet<JoinStats>();
	private SchemaContext currentContext;

	public CorpusStats(final String corpusName, final int corpusScaleFactor) {
		this.corpusName = corpusName;
		this.corpusScaleFactor = corpusScaleFactor;
	}

	public String getCorpusName() {
		return this.corpusName;
	}

	public int getSize() {
		return this.getStatistics().size();
	}

	/**
	 * Return the % of tables with statements.
	 */
	public double getCorpusCoverage() {
		int tablesWithStatements = 0;
		for (final TableStats table : this.getStatistics()) {
			if (table.hasStatements()) {
				++tablesWithStatements;
			}
		}

		return FuzzyLinguisticVariable.toPercent(tablesWithStatements, this.getSize());
	}

	public void addTable(final TableStats table) {
		final QualifiedName fullTableName = table.getQualifiedTableName();
		this.corpusStats.put(fullTableName, table);
	}

	public Collection<TableStats> getStatistics() {
		return Collections.unmodifiableCollection(corpusStats.values());
	}

	public Set<JoinStats> getJoinsStatistics() {
		return Collections.unmodifiableSet(this.joins);
	}

	@Override
	public void beginStmt(StatementAnalysis<?> s) {
		this.currentContext = s.getSchemaContext();
	}

	@Override
	public void onIdentColumn(Column<?> c, int freq) {
		final TableStats table = getStats(c.getTable(), currentContext);
		table.bumpCount(convertToPEColumn(c), freq, table.identColumnStats);
	}

	@Override
	public void onJoin(EquijoinInfo joinInfo, int freq) {
		final TableStats lhs = getStats(joinInfo.getLHS(), currentContext);
		final TableStats rhs = getStats(joinInfo.getRHS(), currentContext);
		lhs.bumpCount(StatementType.JOIN, freq);
		rhs.bumpCount(StatementType.JOIN, freq);

		final JoinSpecification joinType = joinInfo.getType();
		final ListOfPairs<PEColumn, PEColumn> joinOnColumnPairs = joinInfo.getEquijoins();
		for (final Pair<PEColumn, PEColumn> columnPair : joinOnColumnPairs) {
			final JoinStats join = new JoinStats(joinType, columnPair, freq, currentContext);
			if (joins.contains(join)) {
				for (final JoinStats item : joins) {
					if (item.equals(join)) {
						item.bumpJoinCount(join.getFrequency());
					}
				}
			} else {
				joins.add(join);
			}
		}
	}

	@Override
	public void onUpdate(PEColumn c, int freq) {
		// not implemented
	}

	@Override
	public void onInsertValues(PETable tab, int ntuples, int freq) {
		getStats(tab, currentContext).bumpCount(StatementType.INSERT, freq);
	}

	@Override
	public void onSelect(List<Table<?>> tables, int freq) {
		bumpStatementCounts(tables, StatementType.SELECT, freq);

	}

	@Override
	public void onUpdate(List<PETable> tables, int freq) {
		bumpStatementCounts(tables, StatementType.UPDATE, freq);
	}

	@Override
	public void onDelete(List<PETable> tables, int freq) {
		bumpStatementCounts(tables, StatementType.DELETE, freq);

	}

	@Override
	public void onInsertIntoSelect(PETable table, int freq) {
		getStats(table, currentContext).bumpCount(StatementType.INSERT, freq);

	}

	@Override
	public void onUnion(int freq) {
		// not implemented

	}

	@Override
	public void onGroupBy(Column<?> c, int freq) {
		final TableStats table = getStats(c.getTable(), currentContext);
		table.bumpCount(convertToPEColumn(c), freq, table.groupByColumnStats);
	}

	@Override
	public void onOrderBy(PETable table, int freq) {
		getStats(table, currentContext).bumpCount(StatementType.ORDERBY, freq);
	}

	@Override
	public void endStmt(StatementAnalysis<?> s) {
		// noop
	}

	@Override
	public String toString() {
		return "[" + this.getCorpusName() + ": C:" + this.getSize() + "]";
	}

	public void resolveTableColumns(final Table<?> table, final SchemaContext context) {
		final TableStats tableStats = getStats(table, context);
		if (tableStats.columns.isEmpty()) {
			for (final Column<?> column : table.getColumns(context)) {
				tableStats.columns.add(tableStats.new TableColumn(column));
			}
		}
	}

	public void resolveTableForeignKeys(final PETable table, final SchemaContext context) {
		final List<PEForeignKey> keys = table.getForeignKeys(context);
		final TableStats thisTable = getStats(table, context);
		for (final PEForeignKey key : keys) {
			final TableStats targetTable = getStats(key.getTargetTable(context), context);
			final List<PEColumn> foreignColumns = key.getColumns(context);
			final List<PEColumn> targetColumns = key.getTargetColumns(context);
			thisTable.addForwardRelationships(thisTable.new ForeignRelationship(targetTable, foreignColumns, targetColumns));
			targetTable.addBackwardRelationships(targetTable.new ForeignRelationship(thisTable, targetColumns, foreignColumns));
		}
	}

	private <T extends Table<?>> void bumpStatementCounts(final List<T> tables, final StatementType statementType, final int increment) {
		for (final T table : tables) {
			getStats(table, currentContext).bumpCount(statementType, increment);
		}
	}

	private TableStats getStats(final Table<?> table, final SchemaContext context) {
		final String databaseName = table.getDatabase(context).getName().get();
		final String tableName = table.getName().get();
		final QualifiedName fullTableName = toQualifiedName(databaseName, tableName);
		TableStats stats = this.corpusStats.get(fullTableName);
		if (stats == null) { // Table not mentioned in the static report? Silently create one and proceed...
			stats = new TableStats(databaseName, tableName, 0);
			this.corpusStats.put(fullTableName, stats);
		}
		return stats;
	}

	private TableColumn convertToPEColumn(final Column<?> c) {
		final Table<?> parentTable = c.getTable();
		final TableStats tableStats = getStats(parentTable, currentContext);
		if (parentTable instanceof PETable) {
			for (final Column<?> column : parentTable.getColumns(currentContext)) {
				if (column.equals(c)) {
					return tableStats.new TableColumn(column);
				}
			}
		}
		return tableStats.new TableColumn(c);
	}

	private long predictFutureTableCardinality(final TableStats table) {
		final long initialCardinality = table.getTableCardinality();
		final long growthRate = Math.max(
				table.getStatementCounts(StatementType.INSERT)
						- table.getStatementCounts(StatementType.DELETE), 0);

		return initialCardinality + (growthRate * this.corpusScaleFactor);
	}
}
