// OS_STATUS: public
package com.tesora.dve.tools.analyzer.stats;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.util.Pair;

public class BasicStatsVisitor implements StatsVisitor {

	private static final String i1 = "  ";
	private static final String i2 = i1 + i1;
	private static final String i3 = i2 + i1;

	private final HashMap<Table<?>, TableStats> stats = new HashMap<Table<?>, TableStats>();

	private final Stack<StatementAnalysis<?>> path = new Stack<StatementAnalysis<?>>();

	public BasicStatsVisitor() {
	}

	private TableStats getStats(Table<?> pet) {
		TableStats ts = stats.get(pet);
		if (ts == null) {
			final StatementAnalysis<?> top = path.peek();
			ts = new TableStats(pet, pet.getDatabase(top.getSchemaContext()));
			stats.put(pet, ts);
		}
		return ts;
	}

	public void display(PrintStream ps) {
		// we sort this first by join counts, and then by ident column counts
		final MultiMap<Integer, TableStats> byJoinCount = new MultiMap<Integer, TableStats>();

		final CountingMap<Database<?>> totalReads = new CountingMap<Database<?>>();
		final CountingMap<Database<?>> totalWrites = new CountingMap<Database<?>>();
		final CountingMap<Database<?>> totalJoins = new CountingMap<Database<?>>();
		for (final TableStats ts : stats.values()) {
			byJoinCount.put(ts.getTotalJoins(), ts);
			totalReads.put(ts.db, ts.getTotalSelects());
			totalWrites.put(ts.db, ts.getTotalInserts()
					+ ts.getTotalUpdates()
					+ ts.getTotalDeletes());
			totalJoins.put(ts.db, ts.getTotalJoins());
		}

		displayGlobalDatabaseStats(totalReads, totalWrites, totalJoins, ps);

		final List<Integer> jcs = new ArrayList<Integer>(byJoinCount.keySet());
		Collections.sort(jcs);
		Collections.reverse(jcs);
		for (final Integer i : jcs) {
			final Collection<TableStats> sub = byJoinCount.get(i);
			if ((sub == null) || sub.isEmpty()) {
				continue;
			}
			for (final TableStats ts : sub) {
				ts.display(ps);
			}
		}
	}

	private void displayGlobalDatabaseStats(
			final CountingMap<Database<?>> reads,
			final CountingMap<Database<?>> writes,
			final CountingMap<Database<?>> joins,
			final PrintStream ps) {

		final SortedSet<Database<?>> databases = new TreeSet<Database<?>>(
				new Comparator<Database<?>>() {
					@Override
					public int compare(Database<?> a, Database<?> b) {
						final String aName = a.getName().get();
						final String bName = b.getName().get();
						return aName.compareTo(bName);
					}
				});
		databases.addAll(reads.keySet());
		databases.addAll(writes.keySet());
		databases.addAll(joins.keySet());

		ps.println("Global corpus statistics");
		for (final Database<?> database : databases) {
			final String databaseName = database.getName().getSQL();
			ps.println(i1 + "Database " + databaseName);
			ps.println(i2 + "Reads: " + getPrimitiveValue(reads.get(database)));
			ps.println(i2 + "Writes: " + getPrimitiveValue(writes.get(database)));
			ps.println(i2 + "Joins: " + getPrimitiveValue(joins.get(database)));
		}

	}

	private int getPrimitiveValue(final Integer value) {
		return (value != null) ? value.intValue() : 0;
	}

	@Override
	public void onIdentColumn(Column<?> c, int freq) {
		getStats(c.getTable()).takeIdent(c, freq);
	}

	@Override
	public void onJoin(EquijoinInfo joinInfo, int freq) {
		getStats(joinInfo.getLHS()).takeJoin(joinInfo, freq);
		getStats(joinInfo.getRHS()).takeJoin(joinInfo, freq);
	}

	@Override
	public void onUpdate(PEColumn pec, int freq) {
		getStats(pec.getTable()).takeUpdate(pec, freq);
	}

	@Override
	public void onDelete(List<PETable> tabs, int freq) {
		for (final PETable pet : tabs) {
			getStats(pet).takeDelete(freq);
		}
	}

	@Override
	public void onInsertValues(PETable tab, int ntuples, int freq) {
		getStats(tab).takeInsert(ntuples, freq);
	}

	@Override
	public void onGroupBy(Column<?> c, int freq) {
		getStats(c.getTable()).takeGroupBy(c, freq);
	}

	@Override
	public void onOrderBy(PETable tab, int freq) {
		// not implemented
	}

	@Override
	public void onSelect(List<Table<?>> tabs, int freq) {
		for (final Table<?> pet : tabs) {
			getStats(pet).takeSelect(freq);
		}
	}

	@Override
	public void onUpdate(List<PETable> tables, int freq) {
		for (final PETable pet : tables) {
			getStats(pet).takeUpdate(freq);
		}
	}

	@Override
	public void onInsertIntoSelect(PETable table, int freq) {
		getStats(table).takeInsertIntoSelect(freq);
	}

	@Override
	public void onUnion(int freq) {
		// not implemented

	}

	@Override
	public void beginStmt(StatementAnalysis<?> s) {
		path.push(s);
	}

	@Override
	public void endStmt(StatementAnalysis<?> s) {
		path.pop();
	}

	private static class TableStats {

		private final CountingMap<Column<?>> identColumns;
		private final CountingMap<EquijoinInfo> joinInfo;
		private final CountingMap<Column<?>> updateColumns;
		private final Table<?> table;
		private final Database<?> db;
		private int deletes;
		private int selects;
		private int insertIntoSelects;
		private int updates;
		private final CountingMap<Integer> insertTuples;
		private final CountingMap<Column<?>> groupBys;

		public TableStats(Table<?> tab, Database<?> db) {
			identColumns = new CountingMap<Column<?>>();
			joinInfo = new CountingMap<EquijoinInfo>();
			updateColumns = new CountingMap<Column<?>>();
			deletes = 0;
			selects = 0;
			insertIntoSelects = 0;
			updates = 0;
			insertTuples = new CountingMap<Integer>();
			groupBys = new CountingMap<Column<?>>();
			table = tab;
			this.db = db;
		}

		public void takeIdent(Column<?> p, int freq) {
			identColumns.put(p, freq);
		}

		public void takeJoin(EquijoinInfo eji, int freq) {
			joinInfo.put(eji, freq);
		}

		public void takeGroupBy(Column<?> p, int freq) {
			groupBys.put(p, freq);
		}

		public void takeUpdate(Column<?> p, int freq) {
			updateColumns.put(p, freq);
		}

		public void takeUpdate(int freq) {
			updates += freq;
		}

		public void takeDelete(int freq) {
			deletes += freq;
		}

		public void takeSelect(int freq) {
			selects += freq;
		}

		public void takeInsertIntoSelect(int freq) {
			insertIntoSelects += freq;
		}

		public void takeInsert(int ntuples, int freq) {
			insertTuples.put(new Integer(ntuples), freq);
		}

		public int getTotalSelects() {
			return selects;
		}

		public int getTotalInserts() {
			return insertTuples.getValueTotal() + insertIntoSelects;
		}

		public int getTotalUpdates() {
			return updates;
		}

		public int getTotalDeletes() {
			return deletes;
		}

		public int getTotalJoins() {
			return joinInfo.getValueTotal();
		}

		public void display(PrintStream ps) {
			ps.println("Table " + table.getName().getSQL() + " of database " + db.getName().getSQL());
			if (!identColumns.isEmpty()) {
				ps.println(i1 + "Ident columns:");
			}
			for (final Pair<Integer, Column<?>> p : identColumns.getDescendingCountOrder()) {
				final StringBuilder buf = new StringBuilder();
				if (p.getSecond() instanceof PEColumn) {
					Singletons.require(HostService.class).getDBNative().getEmitter().emitDeclaration(((PEColumn) p.getSecond()).getType(),
							(PEColumn) p.getSecond(), buf, false);
				}
				ps.println(i2 + "[" + p.getFirst() + "]: " + p.getSecond().getName().getSQL() + " " + buf.toString());
			}
			if (!joinInfo.isEmpty()) {
				ps.println(i1 + "Joins:");
			}
			for (final Pair<Integer, EquijoinInfo> p : joinInfo.getDescendingCountOrder()) {
				ps.println(i2 + "[" + p.getFirst() + "]: "
						+ p.getSecond().getLDB().getName().getSQL() + "." + p.getSecond().getLHS().getName().getSQL()
						+ " " + p.getSecond().getType().getSQL() + " "
						+ p.getSecond().getRDB().getName().getSQL() + "." + p.getSecond().getRHS().getName().getSQL());
				for (final Pair<PEColumn, PEColumn> jc : p.getSecond().getEquijoins()) {
					ps.println(i3 + jc.getFirst().getName().getSQL() + " = " + jc.getSecond().getName().getSQL());
				}
			}
			if (!updateColumns.isEmpty()) {
				ps.println(i1 + "Updated columns:");
				for (final Pair<Integer, Column<?>> p : updateColumns.getDescendingCountOrder()) {
					ps.println(i2 + "[" + p.getFirst() + "]: " + p.getSecond().getName().getSQL());
				}
			}
			if (!groupBys.isEmpty()) {
				ps.println(i1 + "Grouped by columns:");
				for (final Pair<Integer, Column<?>> p : groupBys.getDescendingCountOrder()) {
					ps.println(i2 + "[" + p.getFirst() + "]: " + p.getSecond().getName().getSQL());
				}
			}
			if (!insertTuples.isEmpty()) {
				ps.println(i1 + "Insert sizes (number of tuples):");
				for (final Pair<Integer, Integer> p : insertTuples.getDescendingCountOrder()) {
					ps.println(i2 + "[" + p.getFirst() + "]: " + p.getSecond());
				}
			}
			if (selects > 0) {
				ps.println(i1 + "Found " + selects + " selects");
			}
			if (updates > 0) {
				ps.println(i1 + "Found " + updates + " updates");
			}
			if (deletes > 0) {
				ps.println(i1 + "Found " + deletes + " deletes");
			}
			if (insertIntoSelects > 0) {
				ps.println(i1 + "Found " + insertIntoSelects + " insert into selects");
			}
		}
	}

	private static class CountingMap<K> extends LinkedHashMap<K, Integer> {

		private static final long serialVersionUID = 1L;

		public CountingMap() {
			super();
		}

		public void put(K key, int freq) {
			Integer e = get(key);
			if (e == null) {
				e = new Integer(freq);
			} else {
				e = new Integer(e.intValue() + freq);
			}
			put(key, e);
		}

		public int getValueTotal() {
			int acc = 0;
			final Collection<Integer> vals = values();
			for (final Integer i : vals) {
				acc += i.intValue();
			}
			return acc;
		}

		public List<Pair<Integer, K>> getDescendingCountOrder() {
			final MultiMap<Integer, K> byOrder = new MultiMap<Integer, K>();
			for (final Map.Entry<K, Integer> m : entrySet()) {
				byOrder.put(m.getValue(), m.getKey());
			}
			final List<Integer> cards = new ArrayList<Integer>(byOrder.keySet());
			Collections.sort(cards);
			Collections.reverse(cards);
			final ArrayList<Pair<Integer, K>> out = new ArrayList<Pair<Integer, K>>();
			for (final Integer i : cards) {
				for (final K k : byOrder.get(i)) {
					out.add(new Pair<Integer, K>(i, k));
				}
			}
			return out;
		}
	}
}
