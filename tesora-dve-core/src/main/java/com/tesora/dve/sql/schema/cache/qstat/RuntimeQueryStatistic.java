package com.tesora.dve.sql.schema.cache.qstat;

import java.util.LinkedList;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.PersistentQueryStatistic;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.StructuralUtils;
import com.tesora.dve.sql.schema.UnqualifiedName;

// has a cache key, but not a name.
public class RuntimeQueryStatistic extends Persistable<RuntimeQueryStatistic, PersistentQueryStatistic> {

	private final SchemaEdge<PEDatabase> db;
	// this intentionally restricts these stats to tables only.
	// no ctas, no temp tables, no views.
	private final TableCacheKey[] tables;
	// the stylized query.
	private final String query;
	
	// transient constructor - i.e. we just decided to create one
	public RuntimeQueryStatistic(SchemaContext sc, PETable[] onTabs, String stylizedQuery) {
		super(buildQueryCacheKey(onTabs,stylizedQuery));
		db = StructuralUtils.buildEdge(sc, onTabs[0].getPEDatabase(sc), false);
		this.tables = new TableCacheKey[onTabs.length];
		for(int i = 0; i < onTabs.length; i++)
			tables[i] = (TableCacheKey) onTabs[i].getCacheKey();
		this.query = stylizedQuery;
	}

	public RuntimeQueryStatistic(SchemaContext sc, PersistentQueryStatistic pqs) {
		super(buildQueryCacheKey(sc,pqs));
		db = StructuralUtils.buildEdge(sc,PEDatabase.load(pqs.getUserDatabase(), sc),true);
		this.tables = toCacheTabs(sc,db.get(sc),pqs.getTables());
		this.query = pqs.getQuery();
	}

	// accessors
	public PEDatabase getDatabase(SchemaContext sc) {
		return db.get(sc);
	}

	public TableCacheKey[] getTables() {
		return tables;
	}
		
	public String getQuery() {
		return this.query;
	}
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return PersistentQueryStatistic.class;
	}

	@Override
	protected int getID(PersistentQueryStatistic p) {
		return p.getId();
	}

	@Override
	protected PersistentQueryStatistic lookup(SchemaContext sc)
			throws PEException {
		return null;
	}

	@Override
	protected PersistentQueryStatistic createEmptyNew(SchemaContext sc)
			throws PEException {
		PersistentQueryStatistic pqs = new PersistentQueryStatistic(db.get(sc).getPersistent(sc),
				toPersistentTabs(tables),
				query,
				0,
				0);
		updateExisting(sc,pqs);
		return pqs;
	}

	private static String toPersistentTabs(TableCacheKey[] tabs) {
		StringBuilder buf = new StringBuilder();
		for(int i = 0; i < tabs.length; i++) {
			if (i > 0) buf.append(",");
			buf.append(tabs[i].getTableName().getUnqualified().getUnquotedName().get());
		}
		return buf.toString();
	}
	
	private static TableCacheKey[] toCacheTabs(SchemaContext sc, PEDatabase pdb, String tabs) {
		String[] bits = tabs.split(",");
		TableCacheKey[] out = new TableCacheKey[bits.length];
		for(int i = 0; i < bits.length; i++) {
			out[i] = PEAbstractTable.getTableKey(pdb, new UnqualifiedName(bits[i]));
		}
		return out;
	}
	
	@Override
	protected void populateNew(SchemaContext sc, PersistentQueryStatistic p)
			throws PEException {
	}

	@Override
	protected void updateExisting(SchemaContext sc, PersistentQueryStatistic p) throws PEException {
	}

	
	@Override
	protected Persistable<RuntimeQueryStatistic, PersistentQueryStatistic> load(
			SchemaContext sc, PersistentQueryStatistic p) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String getDiffTag() {
		return "QStat";
	}

	public static RuntimeQueryCacheKey buildQueryCacheKey(PETable[] tabs, String q) {
		SchemaCacheKey<PEDatabase> dck = tabs[0].getDatabaseCacheKey();
		TableCacheKey[] tcks = new TableCacheKey[tabs.length];
		for(int i = 0; i < tabs.length; i++)
			tcks[i] = (TableCacheKey) tabs[i].getCacheKey();
		return new RuntimeQueryCacheKey(dck,tcks,q);
	}
	
	public static RuntimeQueryCacheKey buildQueryCacheKey(SchemaContext sc, PersistentQueryStatistic pqs) {
		PEDatabase pdb = PEDatabase.load(pqs.getUserDatabase(), sc);
		SchemaCacheKey<PEDatabase> pck = (SchemaCacheKey<PEDatabase>)pdb.getCacheKey(); 
		TableCacheKey[] tcks = toCacheTabs(sc,pdb,pqs.getTables());
		return new RuntimeQueryCacheKey(pck,tcks,pqs.getQuery());
	}
	
	public static class RuntimeQueryCacheKey extends SchemaCacheKey<RuntimeQueryStatistic> {

		// minimal amount of information to load
		// may switch this to an array of ids, where the first is the dbid
		private final SchemaCacheKey<PEDatabase> db;
		private final TableCacheKey[] tabs;
		private final String query;
		
		public RuntimeQueryCacheKey(SchemaCacheKey<PEDatabase> dck, TableCacheKey[] tabs, String q) {
			super();
			this.db = dck;
			this.tabs = tabs;
			this.query = q;
		}
		
		// in practice we will use a different lookup, I think
		@Override
		public int hashCode() {
			int acc = addIntHash(initHash(RuntimeQueryStatistic.class,query.hashCode()),db.hashCode());
			for(TableCacheKey tck : tabs)
				acc = addIntHash(acc,tck.hashCode());
			return acc;
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof RuntimeQueryCacheKey) {
				RuntimeQueryCacheKey oth = (RuntimeQueryCacheKey) o;
				if (!db.equals(oth.db)) return false;
				if (tabs.length != oth.tabs.length) return false;
				if (!query.equals(oth.query)) return false;
				for(int i = 0; i < tabs.length; i++)
					if (!tabs[i].equals(oth.tabs[i])) return false;
				return true;
			}
			return false;
		}

		@Override
		public RuntimeQueryStatistic load(SchemaContext sc) {
			throw new RuntimeException("not quite yet");
		}

		@Override
		public String toString() {
			StringBuilder buf = new StringBuilder();
			buf.append("RuntimeQStat{").append(query).append("}[").append(db.toString()).append("](");
			for(int i = 0; i < tabs.length; i++) {
				if (i > 0) buf.append(",");
				buf.append(tabs[i].toString());
			}
			buf.append(")");
			return buf.toString();
		}
		
		public CacheSegment getCacheSegment() {
			return CacheSegment.QSTATS;
		}

		
	}
	
	/*
		private CurrentStatisticUnit current;
		
		public Accumulator() {
			this.current = new CurrentStatisticUnit();
		}
		
		
		public void onMeasurement(long rows) {
			current.onMeasurement(rows);
		}
		
		public long getCurrentAvg() {
			return (currentHistoricalAvg.get() + current.getAvgRows())/2;
		}
		
		public void update(PersistentQueryStatistic pqs) {
			long calls = 0;
			// we always update with the current avg
			for(HistoricalStatisticUnit i : acc)
				calls += i.getCalls();
			calls += current.getCalls();
			pqs.updateMeasurement(getCurrentAvg(), calls);
		}
	}
	*/
	
}
