// OS_STATUS: public
package com.tesora.dve.distribution;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.StorageGroupGeneration;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.mysql.common.DBTypeBasedUtils;
import com.tesora.dve.db.mysql.common.DataTypeValueFunc;
import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.groupmanager.GroupManager;
import com.tesora.dve.groupmanager.GroupTopicPublisher;
import com.tesora.dve.groupmanager.PurgeDistributionRangeCache;
import com.tesora.dve.locking.ClusterLock;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.worker.MysqlTextResultCollector;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

@Entity
@DiscriminatorValue(RangeDistributionModel.MODEL_NAME)
public class RangeDistributionModel extends DistributionModel {
	
	static Cache<Integer, Integer> rangeForTableCache = CacheBuilder.newBuilder().maximumSize(1000).build();
	
	static Cache<Integer, DistributionRange> rangeCache = CacheBuilder.newBuilder().build();

	private abstract class BinaryFunction<O, I> {
		public BinaryFunction() {}

		public abstract O evaluate(I o1, I o2) throws PEException;
	}
	
	private static final long serialVersionUID = 1L;
	public final static String MODEL_NAME = "Range";
    public static final String GENERATION_LOCKNAME = "PE.Model.Range";

	public final static RangeDistributionModel SINGLETON = new RangeDistributionModel(); 

	public RangeDistributionModel() {
		super(MODEL_NAME);
	}

	@Override
	public MappingSolution mapKeyForInsert(CatalogDAO c, StorageGroup sg, IKeyValue key) throws PEException {
		return mapKeyToGeneration(c, key);
	}
	
	@Override
	public MappingSolution mapKeyForUpdate(CatalogDAO c, StorageGroup sg, IKeyValue key) throws PEException {
		return mapKeyToGeneration(c, key);
	}

	@Override
	public MappingSolution mapKeyForQuery(CatalogDAO c, StorageGroup sg, IKeyValue key, DistKeyOpType op) throws PEException {
		return mapKeyToGeneration(c, key);
	}

	private static DistributionRange findDistributionRange(final CatalogDAO c, final IKeyValue key) throws PEException {
		Integer distRangeID = null;
		DistributionRange dr = null;
		try {
			distRangeID = rangeForTableCache.get(key.getUserTableId(), 
					new Callable<Integer>() {
						@Override
						public Integer call() throws Exception {
							DistributionRange distRange = c.findRangeForTableByName(
									key.getQualifiedTableName(),
									key.getUserTableId());
							return distRange.getId();
						}
			});
		} catch (ExecutionException e) {
			throw new PEException("Cannot map key to range for table " + key.getQualifiedTableName(), e);
		}
		if (distRangeID == null)
			throw new PEException("No range found for table " + key.getQualifiedTableName());
		final int distid = distRangeID; 
		try {
			dr = rangeCache.get(distid, 
					new Callable<DistributionRange>() {
						@Override
						public synchronized DistributionRange call() throws Exception {
							DistributionRange distRange = c.findByKey(DistributionRange.class, distid);
							distRange.fullyLoad();
							return distRange;
						}
			});
		} catch (ExecutionException e) {
			throw new PEException("Cannot load range for id " + distid, e);
		}
		return dr;
	}
	
	MappingSolution mapKeyToGeneration(final CatalogDAO c, final IKeyValue key) throws PEException {
		return findDistributionRange(c,key).mapKeyToGeneration(key);
	}

	@Override
	public void prepareGenerationAddition(SSConnection ssCon, WorkerGroup wg, UserTable ut, StorageGroupGeneration newGen) throws PEException {
		// Compute the range of the generation as the superset of the ranges
		// of all the tables that use the range (they should all be the same,
		// but in case the user has defined disjoint ranges we need to
		// pick the superset).
		DistributionRange dr = ssCon.getCatalogDAO().findRangeForTable(ut);

        ClusterLock genLock = GroupManager.getCoordinationServices().getClusterLock(GENERATION_LOCKNAME);
        boolean didReadUnlock = false;
        try{
            genLock.sharedUnlock(ssCon,"preparing generation, upgrading read lock");
            didReadUnlock = true;
        } catch (IllegalMonitorStateException imse){
            //we probably didn't have the lock.
        }
		genLock.exclusiveLock(ssCon,"preparing new generation");
		try {
			StorageGroupGeneration rangeGeneration = ut.getPersistentGroup().getLastGen();
			GenerationKeyRange genKeyRange = dr.getRangeForGeneration(rangeGeneration);

            Singletons.require(GroupTopicPublisher.class).publish(new PurgeDistributionRangeCache(dr.getId()));

			KeyValue dvStart = getRangeKey(ssCon, dr, wg, ut, "ASC", new BinaryFunction<KeyValue, KeyValue>() {
				@Override
				public KeyValue evaluate(KeyValue o1, KeyValue o2) throws PEException {
					return (o1.compare(o2) < 0) ? o1 : o2;
				}
			});
			if (dvStart != null) {
				KeyValue dvEnd = getRangeKey(ssCon, dr, wg, ut, "DESC", new BinaryFunction<KeyValue, KeyValue>() {
					@Override
					public KeyValue evaluate(KeyValue o1, KeyValue o2) throws PEException {
						return (o1.compare(o2) > 0) ? o1 : o2;
					}
				});
				if (genKeyRange == null) {
					genKeyRange = new GenerationKeyRange(dr, rangeGeneration, dvStart, dvEnd);
					dr.addRangeGeneration(genKeyRange);
				} else {
					genKeyRange.mergeRange(dvStart, dvEnd);
				}
			}
		} finally {
			genLock.exclusiveUnlock(ssCon,"releasing gen lock exclusive");
            if (didReadUnlock)
                genLock.sharedLock(ssCon,"downgrading gen lock to share");
		}
	}

	private KeyValue getRangeKey(SSConnection ssCon, DistributionRange dr, WorkerGroup wg, UserTable ut, final String order,
			BinaryFunction<KeyValue, KeyValue> chooser) throws PEException {
		KeyValue keyValue = ut.getDistValue();
		KeyValue retValue = null;

		String orderColList = Functional.apply(keyValue.keySet(), new UnaryFunction<String, String>() {
			@Override
			public String evaluate(String object) {
				return object + " " + order;
			}
		}).toString();

		String query = "SELECT " + keyValue.orderedKeyNames()
				+ " FROM " + ut.getNameAsIdentifier()
				+ " ORDER BY " + orderColList.substring(1, orderColList.length() - 1)
				+ " LIMIT 1";

		MysqlTextResultCollector results = new MysqlTextResultCollector(true);
		WorkerRequest req = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(),
				new SQLCommand(query)).onDatabase(ut.getDatabase());
		wg.execute(MappingSolution.AllWorkers, req, results);
		if (!results.hasResults()) {
			throw new PENotFoundException("Could not get Key Ranges");
		}
		Map<String, String> nativeTypeComparatorMap = 
				DistributionRange.decodeSignatureIntoNativeTypeComparatorMap(dr.getSignature());
		ColumnSet columnSet = results.getColumnSet();
		for (List<String> row : results.getRowData()) {
			keyValue = ut.getDistValue();
			for (int i = 0; i < keyValue.size(); ++i) {
				ColumnMetadata columnMetadata = columnSet.getColumn(i+1);
				DataTypeValueFunc typeReader = DBTypeBasedUtils.getMysqlTypeFunc(MyFieldType.fromByte(
						columnMetadata.getNativeTypeId()), columnMetadata.getSize(), columnMetadata.getNativeTypeFlags());
				Object value = typeReader.convertStringToObject(row.get(i), columnMetadata);
				ColumnDatum cd = keyValue.get(columnMetadata.getAliasName());
				cd.setValue(value);
				if (nativeTypeComparatorMap.size() > 0) {
					// avoid a lookup if we can
					String nativeType = columnMetadata.getFullNativeTypeName().toLowerCase();
					if (nativeTypeComparatorMap.containsKey(nativeType)) {
						cd.setComparatorClassName(nativeTypeComparatorMap.get(nativeType));
					}
				}
			}
			retValue = (retValue == null) ? keyValue : chooser.evaluate(keyValue, retValue);
		}
		
		return retValue;
	}
	
	public static void clearCache() {
		rangeCache.invalidateAll();
		rangeForTableCache.invalidateAll();
	}
	
	public static void clearCacheEntry(int distributionRangeId) {
		rangeCache.invalidate(distributionRangeId);
	}

	@Override
	public void onUpdate() {
		// do nothing
	}

	@Override
	public void onDrop() {
		// do nothing
	}
}
