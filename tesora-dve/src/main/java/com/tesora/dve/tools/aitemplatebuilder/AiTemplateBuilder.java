// OS_STATUS: public
package com.tesora.dve.tools.aitemplatebuilder;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.common.MathUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.template.TemplateBuilder;
import com.tesora.dve.sql.template.jaxb.Template;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.JoinStats;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.Relationship;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.Relationship.RelationshipSpecification;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats.ForeignRelationship;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats.TableColumn;

public final class AiTemplateBuilder {

	private static final Logger logger = Logger.getLogger(AiTemplateBuilder.class);
	private static final double CORPUS_COVERAGE_THRESHOLD_PC = 60.0;
	private static final int NUMBER_DISPLAY_PRECISION = 2;
	private static int TABLE_NAME_MIN_PREFIX_LENGTH = 4;
	private static String TABLE_NAME_WILDCARD = ".*";

	private static final class Ranges {

		private static final class RangeSizeSorter implements Comparator<CommonRange> {
			@Override
			public int compare(CommonRange a, CommonRange b) {
				return a.getSize() - b.getSize();
			}
		}

		private static final RangeSizeSorter RANGE_SIZE_SORTER = new RangeSizeSorter();

		private final List<CommonRange> ranges = new ArrayList<CommonRange>();
		private final boolean isSafeMode;

		final List<Long> frequencies = new ArrayList<Long>();
		final List<Long> cardinalities = new ArrayList<Long>();

		public Ranges(final boolean isSafeMode) {
			this.isSafeMode = isSafeMode;
		}

		public boolean isEmpty() {
			return this.frequencies.isEmpty();
		}

		public List<Long> getPredictedFutureJoinCardinalities() {
			return Collections.unmodifiableList(this.cardinalities);
		}

		public List<Long> getJoinFrequencies() {
			return Collections.unmodifiableList(this.frequencies);
		}

		public void generateAllPossibleCombinations() throws PEException {
			flattenJoins();
			diversify();
		}

		/**
		 * OUTER JOINs can be merged with compatible INNER JOINs provided that
		 * they are not joins to Broadcast that trigger redistribution - those
		 * have to be handled separately.
		 */
		private void flattenJoins() {
			for (final CommonRange range : this.ranges) {
				range.flattenJoins();
			}
		}

		/**
		 * For all overlapping ranges add their versions without the overlaps.
		 */
		private void diversify() throws PEException {
			final List<CommonRange> diversifiedRanges = new ArrayList<CommonRange>();
			for (final CommonRange range : this.ranges) {
				for (final CommonRange overlappingRange : getOverlapping(this.ranges, range)) {
					if (overlappingRange != range) {
						if (range.getSize() > 1) {
							final CommonRange copy = new CommonRange(range);
							for (final TableStats table : overlappingRange.getTables()) {
								copy.remove(table);
							}
							if (copy.getSize() > 0) {
								diversifiedRanges.add(copy);
							}
						}
					}
				}
			}

			this.ranges.addAll(diversifiedRanges);
		}

		private void addJoin(final Relationship join) throws PEException {
			if (join.isRangeCompatible()) {
				this.frequencies.add(join.getFrequency());
				this.cardinalities.add(join.getLHS().getPredictedFutureCardinality() + join.getRHS().getPredictedFutureCardinality());

				final List<CommonRange> affectedRanges = addJoinToSuitableRanges(join);
				if (affectedRanges.isEmpty()) {
					final CommonRange range = new CommonRange(join, this.isSafeMode);

					/*
					 * Add suitable joins from other ranges.
					 */
					for (final CommonRange other : this.ranges) {
						other.mergeInto(range);
					}

					this.ranges.add(range);

				} else {

					/*
					 * Two-way merge all affected ranges with their overlaps.
					 */
					final Set<CommonRange> uniqueMergedRanges = new HashSet<CommonRange>();
					for (final CommonRange range : affectedRanges) {
						uniqueMergedRanges.addAll(doTwoWayMergeOnOverlappingRanges(range));
					}

					/*
					 * Check for and remove completely overlapping ranges.
					 */
					this.ranges.removeAll(findAllSubranges(new ArrayList<CommonRange>(uniqueMergedRanges)));
				}
			}
		}

		private List<CommonRange> doTwoWayMergeOnOverlappingRanges(final CommonRange range) throws PEException {
			final List<CommonRange> overlappingRanges = getOverlapping(this.ranges, range);
			for (final CommonRange overlappingRange : overlappingRanges) {
				if (overlappingRange != range) {
					range.mergeInto(overlappingRange);
					overlappingRange.mergeInto(range);
				}
			}

			return overlappingRanges;
		}

		private Set<CommonRange> findAllSubranges(final List<CommonRange> ranges) {
			final int numRanges = ranges.size();
			final Set<CommonRange> subranges = new HashSet<CommonRange>(numRanges);
			for (int i = 0; i < (numRanges - 1); ++i) {
				/* Keep the merged ranges sorted by size. */
				Collections.sort(ranges, RANGE_SIZE_SORTER);
				final CommonRange range = ranges.get(i);
				for (int j = i + 1; j < numRanges; ++j) {
					final CommonRange next = ranges.get(j);
					if (range.isSubrangeOf(next)) {
						subranges.add(range);
						break;
					}
				}
			}

			return subranges;
		}

		private List<CommonRange> addJoinToSuitableRanges(final Relationship join) throws PEException {
			final List<CommonRange> affectedRanges = new ArrayList<CommonRange>();
			for (final CommonRange range : this.ranges) {
				if (range.isSuitableFor(join)) {
					range.addJoin(join);
					affectedRanges.add(range);
				}
			}
			return affectedRanges;
		}

		private void evaluate(final Set<Long> uniqueJoinFrequency, final SortedSet<Long> sortedJoinCardinalities) {
			for (final CommonRange range : this.ranges) {
				range.evaluate(uniqueJoinFrequency, sortedJoinCardinalities);
			}
		}

		private List<CommonRange> getSortedCommonRanges() {
			final List<CommonRange> sortedRanges = new ArrayList<CommonRange>(this.ranges);
			Collections.sort(sortedRanges, FuzzyLinguisticVariable.getScoreComparator());
			return sortedRanges;
		}

		private static List<CommonRange> getOverlapping(final List<CommonRange> ranges, final CommonRange range) {
			final List<CommonRange> overlappingRanges = new ArrayList<CommonRange>();
			for (final CommonRange item : ranges) {
				if (item.hasIntersectionWith(range)) {
					overlappingRanges.add(item);
				}
			}

			return overlappingRanges;
		}

	}

	public static List<Template> buildAllBroadcastTemplates(final List<String> databases) {
		return buildSingleModelTemplates(databases, Broadcast.SINGLETON_TEMPLATE_ITEM);
	}

	public static List<Template> buildAllRandomTemplates(final List<String> databases) {
		return buildSingleModelTemplates(databases, Random.SINGLETON_TEMPLATE_ITEM);
	}

	private static List<Template> buildSingleModelTemplates(final List<String> databases, final TemplateItem distributionModel) {
		final List<Template> templates = new ArrayList<Template>();
		for (final String database : databases) {
			templates.add(buildSingleModelTemplate(database, distributionModel));
		}

		return templates;
	}

	private static Template buildSingleModelTemplate(final String databaseName, final TemplateItem distributionModel) {
		final TemplateBuilder builder = new TemplateBuilder(databaseName);
		builder.withTable(TABLE_NAME_WILDCARD, distributionModel.getTemplateItemName());

		return builder.toTemplate();
	}

	private static String getWelcomeMessage(final List<String> databases, final Long broadcastCardinalityCutoff, final boolean followForeignKeys,
			final boolean isSafeMode) {
		final StringBuilder welcomeMessage = new StringBuilder();
		welcomeMessage.append("Generating templates for '" + StringUtils.join(databases, "', '") + "'.");
		welcomeMessage.append(" Broadcast cardinality cutoff: ").append((broadcastCardinalityCutoff != null) ? broadcastCardinalityCutoff : "automatic");
		welcomeMessage.append(" Following FKs: ").append(followForeignKeys);
		welcomeMessage.append(" Safe mode: ").append(isSafeMode);
		welcomeMessage.append("...");

		return welcomeMessage.toString();
	}

	private static String getTableNameAndDistributionModel(final TableStats table) {
		return table.toString().concat(": ").concat(table.getTableDistributionModel().toString());
	}

	/**
	 * Get bonus % for a given column.
	 * Bonus AI columns which are generally safe and ideal for the Range
	 * distribution.
	 * Also slightly bonus integral types which are often good candidates for
	 * Ranging.
	 * 
	 * In the "Safe Mode" only AI columns get non-zero score.
	 */
	public static float getBonusForColumn(final TableColumn column,
			final boolean isSafeMode) {
		if (isSafeMode) {
			if (isAutoIncrement(column)) {
				return 1.0f;
			}
			return 0.0f;
		}

		float bonus = 1.0f;
		if (isAutoIncrement(column)) {
			bonus += 0.15f;
		} else if (column.getType().isIntegralType()) {
			bonus += 0.05f;
		}
		return bonus;
	}

	public static float getBonusForColumns(final Set<TableColumn> columns,
			final boolean isSafeMode) {
		float bonus = 0.0f;
		for (final TableColumn column : columns) {
			bonus = Math.max(bonus, getBonusForColumn(column, isSafeMode));
		}

		return bonus;
	}

	public static boolean isAutoIncrement(final TableColumn column) {
		final Column<?> columnInstance = column.getColumnInstance();
		return ((columnInstance instanceof PEColumn) && ((PEColumn) columnInstance).isAutoIncrement());
	}

	public static boolean hasAutoIncrement(final Set<TableColumn> columns) {
		for (final TableColumn column : columns) {
			if (isAutoIncrement(column)) {
				return true;
			}
		}

		return false;
	}

	public static boolean hasRangeCompatible(final Set<TableColumn> columns) {
		for (final TableColumn column : columns) {
			if (column.getType().isAcceptableRangeType()) {
				return true;
			}
		}

		return false;
	}

	public static boolean isRangeToRangeRelationship(final Relationship relationship) {
		final TemplateItem lhsModel = relationship.getLHS().getTableDistributionModel();
		final TemplateItem rhsModel = relationship.getRHS().getTableDistributionModel();
		return ((lhsModel instanceof Range) && (rhsModel instanceof Range));
	}

	/**
	 * Although collocated, OUTER JOINs to Broadcast require redistribution
	 * if the Broadcast table is the first table in the join.
	 */
	public static boolean isJoinToBroadcastAndRequiresRedist(final Relationship join) {
		final RelationshipSpecification type = join.getType();
		if (type.isOuterJoin()) {
			final TemplateItem lhsModel = join.getLHS().getTableDistributionModel();
			final TemplateItem rhsModel = join.getRHS().getTableDistributionModel();
			if (type.isLeftOuterJoin()) {
				return ((lhsModel instanceof Broadcast) && (rhsModel instanceof Range));
			} else if (type.isRightOuterJoin()) {
				return ((rhsModel instanceof Broadcast) && (lhsModel instanceof Range));
			}
		}

		return false;
	}

	public static boolean isFkCompatibleJoin(final JoinStats join) {

		/*
		 * If this is a join to Broadcast verify that it can be collocated
		 * with FK relationships pointing into this table (@see PE-1504).
		 */
		if (isJoinToBroadcastAndRequiresRedist(join)) {
			final RelationshipSpecification type = join.getType();
			final TableStats rangeSide = (type.isLeftOuterJoin()) ? join.getRHS() : join.getLHS();

			final Set<Set<TableColumn>> targetColumns = rangeSide.getUniqueTargetColumnGroups();
			if (!targetColumns.isEmpty()) {
				final Set<TableColumn> rangeSideJoinColumns = (type.isLeftOuterJoin()) ? join.getRightColumns() : join.getLeftColumns();
				return (join.isFkCompatible() && targetColumns.contains(rangeSideJoinColumns));
			}
		}

		return join.isFkCompatible();
	}

	private static TemplateRangeItem findRangeForTable(final Set<? extends TemplateRangeItem> ranges,
			final TableStats table) {
		for (final TemplateRangeItem range : ranges) {
			if (range.contains(table)) {
				return range;
			}
		}
		return null;
	}

	private static boolean hasRangeForTable(final Set<? extends TemplateRangeItem> ranges, final TableStats table) {
		return (findRangeForTable(ranges, table) != null);
	}

	private final CorpusStats schemaStats;
	private final PrintStream outputStream;
	private final Collection<TableStats> tableStatistics;
	private final Set<JoinStats> joinStatistics;
	private boolean enableWildcards;

	public AiTemplateBuilder(final CorpusStats schemaStats, final PrintStream outputStream) throws PEException {
		if ((schemaStats == null) || (outputStream == null)) {
			throw new IllegalArgumentException();
		}

		this.schemaStats = schemaStats;
		this.outputStream = outputStream;
		this.tableStatistics = schemaStats.getStatistics();
		this.joinStatistics = schemaStats.getJoinsStatistics();

		if (this.tableStatistics.isEmpty()) {
			throw new PEException("The schema contains no tables.");
		}
	}

	public void setWildcardsEnabled(final boolean enableWildcards) {
		this.enableWildcards = enableWildcards;
	}

	public List<Template> buildBroadcastCutoffTemplates(final List<String> databases, final long broadcastCardinalityCutoff) {
		final List<Template> templates = new ArrayList<Template>();
		for (final String database : databases) {
			templates.add(getTemplate(database, this.tableStatistics, broadcastCardinalityCutoff));
		}

		return templates;
	}

	public List<Template> buildTemplates(final List<String> databases, final Long broadcastCardinalityCutoff, final boolean followForeignKeys,
			final boolean isSafeMode) throws Exception {
		logger.debug(getWelcomeMessage(databases, broadcastCardinalityCutoff, followForeignKeys, isSafeMode));

		checkCorpusCoverage();

		logger.debug("Identifying candidate distribution models...");
		if (broadcastCardinalityCutoff != null) {
			identifyCandidateModels(this.tableStatistics, broadcastCardinalityCutoff);
		} else {
			identifyCandidateModels(this.tableStatistics);
		}

		logger.debug("Identifying distribution ranges...");
		final Set<? extends TemplateRangeItem> ranges = identifyBestRanges(this.tableStatistics,
				this.joinStatistics, followForeignKeys, isSafeMode);

		final List<Template> templates = new ArrayList<Template>();
		for (final String database : databases) {
			templates.add(getTemplate(database, this.tableStatistics, ranges));
		}

		return templates;
	}

	private Template getTemplate(final String databaseName, final Collection<TableStats> tables, final long broadcastCardinalityCutoff) {
		final SortedSet<TableStats> databaseTables = new TreeSet<TableStats>();
		for (final TableStats table : tables) {
			if (databaseName.equals(table.getSchemaName())) {
				setCardinalityBasedDistributionModel(table, broadcastCardinalityCutoff, Broadcast.SINGLETON_TEMPLATE_ITEM, Random.SINGLETON_TEMPLATE_ITEM);
				logger.debug(getTableNameAndDistributionModel(table));
				databaseTables.add(table);
			}
		}

		final String commonNamePrefix = getCommonTableNamePrefix(databaseTables);
		final TemplateBuilder builder = new TemplateBuilder(databaseName);

		/* Append table items. */
		for (final TableStats table : databaseTables) {
			final String tableName = replaceTableNamePrefix(table.getTableName(), commonNamePrefix);
			final TemplateItem distributionModel = table.getTableDistributionModel();
			builder.withTable(tableName, distributionModel.getTemplateItemName());
		}

		return builder.toTemplate();
	}

	/**
	 * Set table distribution model based on its predicted cardinality.
	 */
	private void setCardinalityBasedDistributionModel(final TableStats table, final long broadcastCardinalityCutoff, final TemplateItem smallTableModel,
			final TemplateItem largeTableModel) {
		if (table.getPredictedFutureCardinality() > broadcastCardinalityCutoff) {
			table.setTableDistributionModel(largeTableModel);
		} else {
			table.setTableDistributionModel(smallTableModel);
		}
	}

	private Template getTemplate(final String databaseName,
			final Collection<TableStats> tables,
			final Set<? extends TemplateRangeItem> ranges) throws PEException {
		logger.debug("Building a template for '" + databaseName + "'...");

		final SortedSet<TableStats> databaseTables = new TreeSet<TableStats>();
		final SortedSet<TemplateRangeItem> databaseRanges = new TreeSet<TemplateRangeItem>(
				new Comparator<TemplateRangeItem>() {
					@Override
					public int compare(TemplateRangeItem a, TemplateRangeItem b) {
						return a.getTemplateItemName().compareTo(b.getTemplateItemName());
					}
				});
		for (final TableStats table : tables) {
			if (databaseName.equals(table.getSchemaName())) {
				final TemplateItem distributionModel = table.getTableDistributionModel();
				databaseTables.add(table);
				if (distributionModel instanceof Range) {
					databaseRanges.add(findRangeForTable(ranges, table));
				}
			}
		}

		final String commonNamePrefix = getCommonTableNamePrefix(databaseTables);

		final TemplateBuilder builder = new TemplateBuilder(databaseName);

		/* Append range declarations. */
		for (final TemplateRangeItem range : databaseRanges) {
			final String rangeName = removeTableNamePrefix(range.getTemplateItemName(), commonNamePrefix);
			builder.withRequirement(builder.toCreateRangeStatement(rangeName, "#sg#",
					range.getUniqueColumnTypes()));
		}

		/* Append table items. */
		for (final TableStats table : databaseTables) {
			final String tableName = replaceTableNamePrefix(table.getTableName(), commonNamePrefix);
			final TemplateItem distributionModel = table.getTableDistributionModel();
			if (distributionModel instanceof Range) {
				final TemplateRangeItem tableRange = findRangeForTable(databaseRanges, table);
				final String rangeName = removeTableNamePrefix(tableRange.getTemplateItemName(), commonNamePrefix);
				final String[] rangeColumnNames = getColumnNames(tableRange.getRangeColumnsFor(table)).toArray(new String[] {});
				builder.withRangeTable(tableName, rangeName, rangeColumnNames);
			} else {
				builder.withTable(tableName, distributionModel.getTemplateItemName());
			}
		}

		return builder.toTemplate();
	}

	private void checkCorpusCoverage() {
		final double pcTablesWithStatements = MathUtils.round(this.schemaStats.getCorpusCoverage(), NUMBER_DISPLAY_PRECISION);

		if (pcTablesWithStatements < CORPUS_COVERAGE_THRESHOLD_PC) {
			final String warning = "WARNING: Corpus coverage of " + this.schemaStats + " is only " + pcTablesWithStatements + "%!";
			this.outputStream.println(warning);
			logger.debug(warning);
		} else {
			logger.debug("Corpus coverage of " + this.schemaStats + " is " + pcTablesWithStatements + "%.");
		}
	}

	private void identifyCandidateModels(final Collection<TableStats> tables) throws Exception {
		final SortedSet<Long> sortedCardinalities = new TreeSet<Long>();
		for (final TableStats table : tables) {
			sortedCardinalities.add(table.getPredictedFutureCardinality());
		}

		for (final TableStats table : tables) {
			final StringBuilder progress = new StringBuilder();
			progress.append(table).append(": ");

			final List<FuzzyLinguisticVariable> modelsSortedByScore = FuzzyLinguisticVariable
					.evaluateDistributionModels(
							new Broadcast(table, sortedCardinalities),
							new Range(table, sortedCardinalities));
			logger.debug(progress.append(StringUtils.join(modelsSortedByScore, ", ")));

			table.setTableDistributionModel(Collections.max(modelsSortedByScore,
					FuzzyLinguisticVariable.getScoreComparator()));
		}
	}

	private void identifyCandidateModels(final Collection<TableStats> tables, final long broadcastCardinalityCutoff) throws Exception {
		for (final TableStats table : tables) {
			setCardinalityBasedDistributionModel(table, broadcastCardinalityCutoff, Broadcast.SINGLETON_TEMPLATE_ITEM, Range.SINGLETON_TEMPLATE_ITEM);
			logger.debug(getTableNameAndDistributionModel(table));
		}
	}

	/*
	 * Try to identify the best ranges based on the following filter order.
	 * 1. JOIN
	 * 2. WHERE
	 * 3. GROUP BY
	 */
	private Set<? extends TemplateRangeItem> identifyBestRanges(
			final Collection<TableStats> tables, final Set<JoinStats> joins,
			boolean followForeignKeys, final boolean isSafeMode) throws PEException {
		logger.debug("Processing join statistics...");

		final Ranges rangeToRangeRanges = new Ranges(isSafeMode);
		Set<TemplateRangeItem> topRangeToRangeTopRanges = null;
		if (followForeignKeys) {

			/* Handle special foreign relationship cases. */
			resolveForeignCollocationConflicts(tables);

			/* Add joins compatible with the FK relationships. */
			for (final JoinStats join : joins) {
				if (isFkCompatibleJoin(join)) {
					addRangeToRangeRelationship(join, rangeToRangeRanges);
				}
			}

			rangeToRangeRanges.generateAllPossibleCombinations();

			/* Add FK relationships. */
			for (final TableStats table : tables) {
				for (final ForeignRelationship relationship : table.getForwardRelationships()) {
					addRangeToRangeRelationship(relationship, rangeToRangeRanges);
				}
			}

			topRangeToRangeTopRanges = getTopRanges(rangeToRangeRanges);

			// All Range tables with FK and without a range -> Broadcast.
			for (final TableStats table : tables) {
				if ((table.getTableDistributionModel() instanceof Range) && !table.getBackwardRelationships().isEmpty()
						&& !hasRangeForTable(topRangeToRangeTopRanges, table)) {
					table.setTableDistributionModel(Broadcast.SINGLETON_TEMPLATE_ITEM);
				}
			}

		} else {
			for (final JoinStats join : joins) {
				addRangeToRangeRelationship(join, rangeToRangeRanges);
			}

			rangeToRangeRanges.generateAllPossibleCombinations();

			topRangeToRangeTopRanges = getTopRanges(rangeToRangeRanges);
		}

		logger.debug("Processing orphaned range tables...");
		for (final TableStats table : tables) {
			final TemplateItem model = table.getTableDistributionModel();

			/*
			 * This Range candidate has no range.
			 * Either its proposed range was not identified as optimal
			 * or it is only being joined to Broadcast tables.
			 */
			if ((model instanceof Range) && !hasRangeForTable(topRangeToRangeTopRanges, table)) {

				final StringBuilder logMessage = new StringBuilder();
				logMessage.append(table).append(": ").append(model).append(" (");

				/*
				 * Try to range on OUTER JOIN columns if the to a Broadcast
				 * table and would require redistribution.
				 */
				TemplateRangeItem newRange = PrivateRange.fromOuterJoinColumns(table, joins, topRangeToRangeTopRanges, isSafeMode);
				if (newRange != null) {
					topRangeToRangeTopRanges.add(newRange);
					logMessage.append("ranged on OUTER JOIN column(s): ").append(toStringOfDelimitedColumnNames(newRange.getRangeColumnsFor(table)));
				}

				/*
				 * Try to Range on identity columns instead.
				 */
				if (newRange == null) {
					if (table.hasIdentColumns() || table.hasGroupByColumns()) {
						newRange = PrivateRange.fromWhereColumns(table, topRangeToRangeTopRanges,
								isSafeMode);
						if (newRange == null) {
							newRange = PrivateRange.fromGroupByColumns(table, topRangeToRangeTopRanges, isSafeMode);
						}
						if (newRange != null) {
							topRangeToRangeTopRanges.add(newRange);
							logMessage.append("constant filter column ").append(toStringOfDelimitedColumnNames(newRange.getRangeColumnsFor(table)))
									.append(" found");
						}
					}
				}

				/*
				 * Look for AI columns and similar columns used in existing
				 * ranges.
				 */
				if (newRange == null) {
					newRange = PrivateRange.fromAllColumns(table, topRangeToRangeTopRanges,
							isSafeMode);
					if (newRange != null) {
						final Set<TableColumn> columnSet = newRange
								.getRangeColumnsFor(table);
						if (hasAutoIncrement(columnSet)) {
							topRangeToRangeTopRanges.add(newRange);
							logMessage.append("AI range column ").append(toStringOfDelimitedColumnNames(newRange.getRangeColumnsFor(table))).append(" found");
						} else {
							boolean found = false;
							for (final TemplateRangeItem range : topRangeToRangeTopRanges) {
								if (range.hasCommonColumn(columnSet)) {

									topRangeToRangeTopRanges.add(newRange);
									found = true;
									logMessage.append("foreign range column ").append(toStringOfDelimitedColumnNames(newRange.getRangeColumnsFor(table)))
											.append(" found");

									break;
								}
							}

							if (!found) {
								newRange = null;
							}
						}
					}
				}

				/* Fall back to Random. */
				if (newRange == null) {
					table.setTableDistributionModel(Random.SINGLETON_TEMPLATE_ITEM);
					logMessage.append("no suitable range columns found");
				}

				logger.debug(logMessage.append(")"));
			}
		}

		return topRangeToRangeTopRanges;
	}

	/**
	 * Joins to Broadcast tables will always be collocated.
	 * INNER JOINs to Broadcast tables should not trigger redistribution.
	 * OUTER JOINs with the first table being Broadcast tables will trigger
	 * redistribution of the second (Range) table unless it is ranged on the
	 * joined column. This is to prevent getting duplicate rows from the first
	 * (Broadcast) table.
	 */
	private void addRangeToRangeRelationship(final Relationship relationship, final Ranges rangeToRangeRanges)
			throws PEException {
		if (isRangeToRangeRelationship(relationship) || isJoinToBroadcastAndRequiresRedist(relationship)) {
			rangeToRangeRanges.addJoin(relationship);
		}
	}

	private Set<TemplateRangeItem> getTopRanges(final Ranges ranges) {
		if (!ranges.isEmpty()) {
			final SortedSet<Long> sortedJoinCardinalities = new TreeSet<Long>(ranges.getPredictedFutureJoinCardinalities());
			final Set<Long> uniqueJoinFrequencies = new HashSet<Long>(ranges.getJoinFrequencies());
			ranges.evaluate(uniqueJoinFrequencies, sortedJoinCardinalities);
		}

		/*
		 * Retain only the highest score ranges,
		 * removing the lower score alternatives.
		 */
		final List<CommonRange> sortedRanges = ranges.getSortedCommonRanges();
		final Set<TemplateRangeItem> topRanges = new TreeSet<TemplateRangeItem>(new Comparator<TemplateRangeItem>() {
			@Override
			public int compare(TemplateRangeItem a, TemplateRangeItem b) {
				return a.getTemplateItemName().compareTo(b.getTemplateItemName());
			}
		});
		while (!sortedRanges.isEmpty()) {
			final CommonRange top = Collections.max(sortedRanges, FuzzyLinguisticVariable.getScoreComparator());
			if (top.getScore() > 0.0) {
				topRanges.add(top);
				final StringBuilder progress = new StringBuilder();
				progress.append("[Range Group]: ").append(System.getProperty("line.separator"));
				final List<CommonRange> overlappingRanges = Ranges.getOverlapping(sortedRanges, top);
				sortedRanges.removeAll(overlappingRanges);
				for (final CommonRange groupRange : overlappingRanges) {
					progress.append(groupRange).append(System.getProperty("line.separator"));
				}
				logger.debug(progress.toString());
			} else {

				/*
				 * All ranges have zero score.
				 * Most likely because we are in the safe mode
				 * considering only auto-increment columns.
				 * Move on and try to find private ranges for the tables
				 * based on other (ident) columns or fall back to Random.
				 */
				sortedRanges.clear();
			}
		}

		return topRanges;
	}

	/**
	 * Handles collocation special cases.
	 * 
	 * 1. A table is being referenced on a single column group.
	 * 
	 * a) The table is referenced by two or more unique column groups from a
	 * single table.
	 * The solution which preserves collocation is to make the
	 * table and all the tables it points at Broadcast.
	 * 
	 * b) The table has unique foreign and target column groups. In other words,
	 * the table is being pointed at and points on two or more unique column
	 * groups.
	 * The only solution is making the pointed tables
	 * and all their descendants Broadcast.
	 * 
	 * 2. A table is being referenced on two or more unique column groups. The
	 * only solution in this case is making the table and all the tables it
	 * points at Broadcast.
	 * 
	 * 
	 * NOTE: Basic one-to-one collocation cases:
	 * 
	 * a) Range -> Broadcast: always collocated
	 * b) Range -> Range: collocated only if in the same range.
	 * c) Broadcast -> Range: make the referenced table Broadcast (a), or
	 * colocate the two tables on the same range (b).
	 * 
	 * NOTE: Same rules hold for self-referencing relationships (table with a
	 * foreign key into itself).
	 */
	private void resolveForeignCollocationConflicts(final Collection<TableStats> tables) throws PEException {

		/*
		 * Make sure there are no Broadcast -> Range relationships (c) by making
		 * both tables Range.
		 */
		for (final TableStats table : tables) {
			if (table.getTableDistributionModel() instanceof Range) {
				for (final TableStats childTable : table.getReferencingForeignTables()) {
					if (childTable.getTableDistributionModel() instanceof Broadcast) {
						makeBackwardTableTreeRange(childTable);
					}
				}
			}
		}

		/*
		 * Now, we should have only Range -> Broadcast (a) and Range -> Range
		 * (b) relationships.
		 */

		/* Resolve the special cases. */
		for (final TableStats table : tables) {

			/*
			 * Here we handle only the Range -> Range (b) case.
			 * Range -> Broadcast (a) is always collocated and we never change
			 * the parent table's model to anything other than Broadcast.
			 */
			if (table.getTableDistributionModel() instanceof Range) {
				final Set<Set<TableColumn>> uniqueTargetColumnGroups = table
						.getUniqueTargetColumnGroups();

				if (uniqueTargetColumnGroups.size() == 1) { // Case (1)

					/* Case (1a) */
					final Set<ForeignRelationship> backwardRelationships = table.getBackwardRelationships();
					for (@SuppressWarnings("unused")
					final Set<TableColumn> targetColumnGroup : uniqueTargetColumnGroups) {
						final Set<TableStats> visitedReferencingTables = new HashSet<TableStats>();
						for (final ForeignRelationship relationship : backwardRelationships) {
							final TableStats targetTable = relationship.getRHS();
							if (!visitedReferencingTables.add(targetTable)) {
								makeForwardTableTreeBroadcast(table);
								break;
							}
						}
					}

					if (!(table.getTableDistributionModel() instanceof Range)) {
						continue; // The case already resolved from above.
					}

					/* Case (1b) */
					final Set<ForeignRelationship> forwardRelationships = table.getForwardRelationships();
					for (final Set<TableColumn> targetColumnGroup : uniqueTargetColumnGroups) {
						for (final ForeignRelationship relationship : forwardRelationships) {
							if (!targetColumnGroup.equals(relationship.getForeignColumns())) {
								final TableStats targetTable = relationship.getRHS();
								makeForwardTableTreeBroadcast(targetTable);
							}
						}
					}

				} else if (uniqueTargetColumnGroups.size() > 1) { // Case (2)
					makeForwardTableTreeBroadcast(table);
				}
			}
		}
	}

	private void makeBackwardTableTreeRange(final TableStats root)
			throws PEException {
		for (final TableStats table : root.getReferencingForeignTables()) {
			if (!(table.getTableDistributionModel() instanceof Range)) {
				/* Change model first, then recurse to avoid cycles. */
				table.setTableDistributionModel(Range.SINGLETON_TEMPLATE_ITEM);
				makeBackwardTableTreeRange(table);
			}
		}
		root.setTableDistributionModel(Range.SINGLETON_TEMPLATE_ITEM);
	}

	private void makeForwardTableTreeBroadcast(final TableStats root)
			throws PEException {
		for (final TableStats table : root.getReferencedForeignTables()) {
			if (!(table.getTableDistributionModel() instanceof Broadcast)) {
				/* Change model first, then recurse to avoid cycles. */
				table.setTableDistributionModel(Broadcast.SINGLETON_TEMPLATE_ITEM);
				makeForwardTableTreeBroadcast(table);
			}
		}
		root.setTableDistributionModel(Broadcast.SINGLETON_TEMPLATE_ITEM);
	}

	private String toStringOfDelimitedColumnNames(final Set<TableColumn> columns) {
		return "'" + StringUtils.join(getColumnNames(columns), "', '") + "'";
	}

	private List<String> getColumnNames(final Set<TableColumn> columns) {
		final List<String> names = new ArrayList<String>();
		for (final TableColumn column : columns) {
			names.add(column.getName().getUnquotedName().get());
		}

		return names;
	}

	private String getCommonTableNamePrefix(final SortedSet<TableStats> tables) {
		final String firstName = tables.first().getTableName();
		final String lastName = tables.last().getTableName();

		final int minLength = Math.min(firstName.length(), lastName.length());
		for (int i = 0; i < minLength; ++i) {
			if ((i > (TABLE_NAME_MIN_PREFIX_LENGTH - 1)) && (firstName.charAt(i) != lastName.charAt(i))) {
				return firstName.substring(0, i);
			}
		}

		return firstName.substring(0, minLength);
	}

	private String removeTableNamePrefix(final String value, final String prefix) {
		return replaceTableNamePrefix(value, prefix, "");
	}

	private String replaceTableNamePrefix(final String value, final String prefix) {
		return replaceTableNamePrefix(value, prefix, TABLE_NAME_WILDCARD);
	}

	private String replaceTableNamePrefix(final String value, final String prefix, final String replacement) {
		// Exclude prefixes shorter than 3 chars.
		if (this.enableWildcards && !value.equals(prefix) && (prefix.length() > TABLE_NAME_MIN_PREFIX_LENGTH)) {
			return value.replaceFirst(prefix, replacement);
		}

		return value;
	}
}