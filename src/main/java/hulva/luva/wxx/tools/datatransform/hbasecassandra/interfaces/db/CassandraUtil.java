package hulva.luva.wxx.tools.datatransform.hbasecassandra.interfaces.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.exceptions.DbException;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.log.MonitorLog;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.log.MonitorLog.MonitorOperationType;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.utils.RandomUtils;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * Cassandra封装查询方法
 * 
 * @author Fritz.F.yan
 */
public class CassandraUtil extends DatabaseInterface {

	protected Cluster cluster;
	protected Keyspace keyspace;
	protected String columnFamily = null;
	protected int ttl = 0;

	@Override
	public String name() {
		return "CASSANDRA";
	}

	public CassandraUtil(String cassandraHosts, String clusterName, String keyspacesName, String columnFamily, int ttl,
			DatabaseWatcher waitch) {
		this.columnFamily = columnFamily;
		this.waitch = waitch;
		CassandraHostConfigurator configurator = new CassandraHostConfigurator(cassandraHosts);
		this.cluster = HFactory.getOrCreateCluster(clusterName, configurator);
		this.keyspace = HFactory.createKeyspace(keyspacesName, cluster);
		this.ttl = ttl;
	}

	// added by Luva 20170505
	public Keyspace getKeyspace() {
		return this.keyspace;
	}

	@Override
	public boolean exist(String key) throws DbException {
		long time = System.currentTimeMillis();
		try {
			LOGGER.debug("exist by cassandra:" + key);
			SliceQuery<String, String, String> q = HFactory.createSliceQuery(keyspace, StringSerializer.get(),
					StringSerializer.get(), StringSerializer.get());
			q.setColumnFamily(columnFamily);
			q.setKey(key);
			q.setRange("", "", false, 1);
			QueryResult<ColumnSlice<String, String>> result = q.execute();
			ColumnSlice<String, String> values = result.get();
			time = System.currentTimeMillis() - time;
			MonitorLog.cassandraLog(MonitorOperationType.READ, time);
			if (values != null && values.getColumnByName(VersionColumn) != null) {
				return true;
			}
			return false;
		} catch (Exception e) {
			onException(e);
			throw new DbException("exist error:" + key, e);
		}
	}

	@Override
	public void iterator(WitchData witch) throws DbException {
		if (witch == null)
			return;
		try {
			RangeSlicesQuery<String, String, String> query = HFactory.createRangeSlicesQuery(keyspace,
					StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
			query.setColumnFamily(columnFamily);
			query.setRange(null, null, false, Integer.MAX_VALUE);
			query.setRowCount(100);
			boolean flag = false;
			String start = null;
			do {
				int count = 0;
				query.setKeys(start, null);
				OrderedRows<String, String, String> result = query.execute().get();
				Iterator<Row<String, String, String>> rows = result.iterator();
				while (rows.hasNext()) {
					Row<String, String, String> row = rows.next();
					ColumnSlice<String, String> values = row.getColumnSlice();
					if (null == values || values.getColumns() == null || values.getColumns().size() == 0) {
						return;
					}
					Map<String, String> map = new HashMap<String, String>();
					for (HColumn<String, String> column : values.getColumns()) {
						String name = column.getName();
						String value = column.getValue();
						map.put(name, value);
					}
					count++;
					witch.next(row.getKey(), map);
					start = row.getKey();
				}
				if (count >= 95) {
					flag = true;
				}
			} while (flag);
		} catch (Exception e) {
			onException(e);
			throw new DbException("iterator data error", e);
		}
	}

	public Map<String, String> get(String key) throws DbException {
		if (key == null)
			return null;
		long time = System.currentTimeMillis();
		try {
			LOGGER.debug("get by cassandra:" + key);
			SliceQuery<String, String, String> q = HFactory.createSliceQuery(keyspace, StringSerializer.get(),
					StringSerializer.get(), StringSerializer.get());
			q.setColumnFamily(columnFamily);
			q.setKey(key);
			q.setRange(null, null, false, 100);
			QueryResult<ColumnSlice<String, String>> result = q.execute();
			ColumnSlice<String, String> values = result.get();
			time = System.currentTimeMillis() - time;
			MonitorLog.cassandraLog(MonitorOperationType.READ, time);
			if (null == values || values.getColumns() == null || values.getColumns().size() == 0) {
				return null;
			}
			Map<String, String> map = new HashMap<String, String>();
			for (HColumn<String, String> column : values.getColumns()) {
				String name = column.getName();
				String value = column.getValue();
				map.put(name, value);
			}
			return map;
		} catch (Exception e) {
			onException(e);
			throw new DbException("get data error:" + key, e);
		}
	}

	/**
	 * 保存对象
	 */
	public void put(String key, Map<String, String> map) throws DbException {
		long time = System.currentTimeMillis();
		try {
			LOGGER.debug("put by cassandra:" + key);
			Mutator<String> m = HFactory.createMutator(keyspace, StringSerializer.get());
			m.addDeletion(key, columnFamily);
			long clock = HFactory.createClock();
			for (String name : map.keySet()) {
				String value = map.get(name);
				if (value != null && ttl > 0) {
					m.addInsertion(key, columnFamily, HFactory.createColumn(name, value, clock, ttl,
							StringSerializer.get(), StringSerializer.get()));
				} else if (value != null) {
					m.addInsertion(key, columnFamily,
							HFactory.createColumn(name, value, clock, StringSerializer.get(), StringSerializer.get()));
				} else {
					m.addDeletion(key, columnFamily, name, StringSerializer.get(), clock);
				}
			}
			m.execute();
			time = System.currentTimeMillis() - time;
			MonitorLog.cassandraLog(MonitorOperationType.WRITE, time);
		} catch (Exception e) {
			onException(e);
			throw new DbException("put data error:" + key, e);
		}
	}

	@Override
	public void put(String key, Map<String, String> map, int ttl) throws DbException {
		long time = System.currentTimeMillis();
		try {
			LOGGER.debug("put by cassandra:" + key);
			Mutator<String> m = HFactory.createMutator(keyspace, StringSerializer.get());
			m.addDeletion(key, columnFamily);
			long clock = HFactory.createClock();
			for (String name : map.keySet()) {
				String value = map.get(name);
				if (value != null && ttl > 0) {
					m.addInsertion(key, columnFamily, HFactory.createColumn(name, value, clock, ttl,
							StringSerializer.get(), StringSerializer.get()));
				} else if (value != null) {
					m.addInsertion(key, columnFamily,
							HFactory.createColumn(name, value, clock, StringSerializer.get(), StringSerializer.get()));
				} else {
					m.addDeletion(key, columnFamily, name, StringSerializer.get(), clock);
				}
			}
			m.execute();
			time = System.currentTimeMillis() - time;
			MonitorLog.cassandraLog(MonitorOperationType.WRITE, time);
		} catch (Exception e) {
			onException(e);
			throw new DbException("put data error:" + key, e);
		}
	}

	public void delete(String key) throws DbException {
		long time = System.currentTimeMillis();
		try {
			LOGGER.debug("delete by cassandra:" + key);
			Mutator<String> m = HFactory.createMutator(keyspace, StringSerializer.get());
			m.addDeletion(key, columnFamily);
			m.execute();
			time = System.currentTimeMillis() - time;
			MonitorLog.cassandraLog(MonitorOperationType.WRITE, time);
		} catch (Exception e) {
			onException(e);
			throw new DbException("delete data error:" + key, e);
		}
	}

	@Override
	public String get(String key, String field) throws DbException {
		long time = System.currentTimeMillis();
		try {
			LOGGER.debug("get field by cassandra:" + key);
			SliceQuery<String, String, String> q = HFactory.createSliceQuery(keyspace, StringSerializer.get(),
					StringSerializer.get(), StringSerializer.get());
			q.setColumnFamily(columnFamily);
			q.setColumnNames(field);
			q.setKey(key);
			QueryResult<ColumnSlice<String, String>> result = q.execute();
			ColumnSlice<String, String> values = result.get();
			time = System.currentTimeMillis() - time;
			MonitorLog.cassandraLog(MonitorOperationType.READ, time);
			if (null == values || values.getColumns() == null || values.getColumns().size() == 0) {
				return null;
			}
			HColumn<String, String> column = values.getColumnByName(field);
			return column.getValue();
		} catch (Exception e) {
			onException(e);
			throw new DbException("get field data error:" + key + ":" + field, e);
		}
	}

	@Override
	public String randomKey() throws DbException {
		try {
			RangeSlicesQuery<String, String, String> q = HFactory.createRangeSlicesQuery(keyspace,
					StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
			q.setColumnFamily(columnFamily);
			q.setReturnKeysOnly();
			q.setRowCount(1);
			q.setRange(RandomUtils.nextString(1), "", false, 1);
			QueryResult<OrderedRows<String, String, String>> results = q.execute();
			OrderedRows<String, String, String> rows = results.get();
			return rows.peekLast().getKey();
		} catch (Exception e) {
			onException(e);
			throw new DbException("get random key error", e);
		}
	}

	@Override
	public void close() throws IOException {
		HFactory.shutdownCluster(cluster);
	}

	@Override
	public boolean ping() {
		try {
			SliceQuery<String, String, String> q = HFactory.createSliceQuery(keyspace, StringSerializer.get(),
					StringSerializer.get(), StringSerializer.get());
			q.setColumnFamily(columnFamily);
			q.setKey("null");
			q.setRange("", "", false, 1);
			q.execute();
			return true;
		} catch (Exception e) {
			onException(e);
			return false;
		}
	}

}
