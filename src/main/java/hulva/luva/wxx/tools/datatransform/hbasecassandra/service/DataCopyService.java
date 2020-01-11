package hulva.luva.wxx.tools.datatransform.hbasecassandra.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.exceptions.DbException;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.interfaces.DatabaseFactory;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.interfaces.db.CassandraUtil;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.interfaces.db.DatabaseInterface;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.interfaces.db.HbaseUtil;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.model.CassandraInfo;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.model.HBaseInfo;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

@Service
public class DataCopyService {

	// 是否可以继续copy数据
	public static boolean ISCOPYING = false;
	public static long CURRENTROW = 0;
	public static String RUNNINGFUNCTION = "No Running Function";
	public static String JOBSTATUS = "No Running Job";

	private static int TTL = 2592000; // original global TTL

	private static Logger logger = Logger.getLogger(DataCopyService.class);

	private static DatabaseInterface sourceDb;

	private static DatabaseInterface destinationDb;

	private final static AtomicBoolean ENABLED = new AtomicBoolean(true);

	public int setTTL(int newTTL) {
		TTL = newTTL;
		return TTL;
	}

	public int getTTL() {
		return TTL;
	}

	private DatabaseInterface initHBase(HBaseInfo hbaseInfo) throws NumberFormatException, DbException {
		return DatabaseFactory.buildHBase(hbaseInfo.getHbaseZKQuorum(), Integer.parseInt(hbaseInfo.getHbaseZKPropertyClientPort()),
				hbaseInfo.getHbaseTable(), hbaseInfo.getHbaseColumnFamily(), TTL, null);
	}

	private DatabaseInterface initCassandra(CassandraInfo cassandraInfo) throws DbException {
		return DatabaseFactory.buildCassandra(cassandraInfo.getCassandraHosts(), cassandraInfo.getCassandraClusterName(),
				cassandraInfo.getCassandraKeyspace(), cassandraInfo.getCassandraColumnFamily(), TTL, null);
	}

	public void doCopyHbase2Hbase(HBaseInfo source, HBaseInfo destination) {
		try {
			sourceDb = initHBase(source);
			destinationDb = initHBase(destination);

			RUNNINGFUNCTION = "doCopyHbase2Hbase";
			JOBSTATUS = "Running";
			logger.info("--------------- Start to copy hbase to hbase ---------------");
			scanHBaseAndHandleResult(source, (HbaseUtil) sourceDb, destinationDb);
			logger.info("--------------- Copy hbase to hbase is finish, total count:" + CURRENTROW + " ---------------");
			JOBSTATUS = "Success";
		} catch (Exception e) {
			JOBSTATUS = "Failed";
			logger.error("Copy data from HBase to HBase error.", e);
		} finally {
			ISCOPYING = false;
			try {
				sourceDb.close();
				destinationDb.close();
			} catch (IOException e) {
				logger.error("Close hbase error.", e);
			}
		}
	}

	public void doCopyHbase2Cassandra(HBaseInfo source, CassandraInfo destination) {
		try {
			sourceDb = initHBase(source);
			destinationDb = initCassandra(destination);

			RUNNINGFUNCTION = "doCopyHbase2Cassandra";
			JOBSTATUS = "Running";
			logger.info("--------------- Start to copy hbase to cassandra ---------------");
			scanHBaseAndHandleResult(source, (HbaseUtil) sourceDb, destinationDb);
			logger.info("--------------- Copy hbase to cassandra is finish, total count:" + CURRENTROW + " ---------------");
			JOBSTATUS = "Success";
		} catch (Exception e) {
			JOBSTATUS = "Failed";
			logger.error("Copy data from HBase to Cassandra error.", e);
		} finally {
			ISCOPYING = false;
			try {
				sourceDb.close();
				destinationDb.close();
			} catch (IOException e) {
				logger.error("Close hbase and cassandra error.", e);
			}
		}
	}

	public void doCopyCassandra2Hbase(CassandraInfo source, HBaseInfo destination) {
		try {
			sourceDb = initCassandra(source);
			destinationDb = initHBase(destination);

			RUNNINGFUNCTION = "doCopyCassandra2Hbase";
			JOBSTATUS = "Running";
			logger.info("--------------- Start to copy cassandra to hbase ---------------");
			scanCassandraAndHandleResult(source, (CassandraUtil) sourceDb, destinationDb);
			logger.info("--------------- Copy data from HBase to Cassandra is finish, total count:" + CURRENTROW + " ---------------");
			JOBSTATUS = "Success";
		} catch (Exception e) {
			JOBSTATUS = "Failed";
			logger.error("Copy Cassandra to HBase error.", e);
		} finally {
			ISCOPYING = false;
			try {
				sourceDb.close();
				destinationDb.close();
			} catch (IOException e) {
				logger.error("Close hbase and cassandra error.", e);
			}
		}
	}

	public void doCopyCassandra2Cassandra(CassandraInfo source, CassandraInfo destination) {
		try {
			sourceDb = initCassandra(source);
			destinationDb = initCassandra(destination);

			RUNNINGFUNCTION = "doCopyCassandra2Cassandra";
			JOBSTATUS = "Running";
			logger.info("--------------- Start to copy cassandra to cassandra ---------------");
			scanCassandraAndHandleResult(source, (CassandraUtil) sourceDb, destinationDb);
			logger.info("--------------- Copy data from Cassandra to Cassandra is finish, total count:" + CURRENTROW + " ---------------");
			JOBSTATUS = "Success";
		} catch (Exception e) {
			JOBSTATUS = "Failed";
			logger.error("Copy Cassandra to Cassandra error.", e);
		} finally {
			ISCOPYING = false;
			try {
				sourceDb.close();
				destinationDb.close();
			} catch (IOException e) {
				logger.error("Close cassandra error.", e);
			}
		}
	}

	private void scanHBaseAndHandleResult(HBaseInfo source, HbaseUtil sourceHBase, DatabaseInterface db) throws Exception {
		HTable table = (HTable) sourceHBase.getTable();
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(source.getHbaseColumnFamily()));

		ResultScanner scanner;
		scanner = table.getScanner(scan);
		long n = 0;
		Map<String, String> map = null;
		List<Cell> cells = null;
		for (Result result = scanner.next(); ENABLED.get() && result != null; result = scanner.next()) {
			cells = result.listCells();
			map = new HashMap<String, String>();
			if (cells != null && 0 != cells.size()) {
				for (Cell cell : cells) {
					map.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}
			if (save(Bytes.toString(result.getRow()), map, db)) {
				CURRENTROW = ++n;
			}
		}
	}

	private void scanCassandraAndHandleResult(CassandraInfo source, CassandraUtil sourceCassandra, DatabaseInterface db) throws Exception {
		Keyspace keySpace = sourceCassandra.getKeyspace();
		int rowCount = 100;
		RangeSlicesQuery<String, String, String> query = HFactory.createRangeSlicesQuery(keySpace, StringSerializer.get(),
				StringSerializer.get(), StringSerializer.get());
		query.setColumnFamily(source.getCassandraColumnFamily());
		query.setKeys(null, null);
		query.setRange(null, null, false, Integer.MAX_VALUE);
		query.setRowCount(rowCount);
		boolean more = true;
		long n = 0;
		while (ENABLED.get() && more) {
			QueryResult<OrderedRows<String, String, String>> result = query.execute();
			OrderedRows<String, String, String> orderedRows = result.get();
			if (orderedRows == null) {
				continue;
			}
			List<Row<String, String, String>> rowList = orderedRows.getList();
			if (rowList.size() < rowCount) {
				more = false;
			} else {
				query.setKeys(orderedRows.peekLast().getKey(), null);
			}
			if (rowList != null && rowList.size() > 0) {
				for (Row<String, String, String> row : rowList) {
					Map<String, String> map = new HashMap<String, String>();
					List<HColumn<String, String>> columns = row.getColumnSlice().getColumns();
					if (0 != columns.size()) {
						for (HColumn<String, String> column : columns) {
							map.put(column.getName(), column.getValue());
						}
					}
					if (save(row.getKey(), map, db)) {
						CURRENTROW = ++n;
					}
				}
			}
		}
	}

	/**
	 * 写入成功返回 true
	 * 
	 * @param key
	 * @param newData
	 * @param db      数据要写入的DB()
	 * @return
	 * @throws DbException
	 */
	private boolean save(String key, Map<String, String> newData, DatabaseInterface db) throws DbException {
		Map<String, String> oldData = db.get(key);
		if (oldData != null) {
			String nv = newData.get("V");
			String ov = oldData.get("V");
			if (nv != null && ov != null && Long.valueOf(nv) > Long.valueOf(ov)) {
				logger.info(newData);
				long remainedTTL = TTL - (System.currentTimeMillis() - Long.valueOf(nv));
				db.put(key, newData, (int) (remainedTTL / 1000));
				return true;
			}
		} else {
			if (newData.size() > 0) {
				logger.info(newData);
				db.put(key, newData);
				return true;
			}
		}
		return false;
	}
	
	public void disable() {
		ENABLED.set(false);
	}
	
	public void enable() {
		ENABLED.set(true);
	}
	
	public boolean enableStatus() {
		return ENABLED.get();
	}
}
