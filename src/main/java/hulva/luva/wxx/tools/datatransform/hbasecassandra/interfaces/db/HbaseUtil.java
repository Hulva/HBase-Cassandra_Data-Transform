package hulva.luva.wxx.tools.datatransform.hbasecassandra.interfaces.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.exceptions.DbException;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.log.MonitorLog;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.log.MonitorLog.MonitorOperationType;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.utils.RandomUtils;


public class HbaseUtil extends DatabaseInterface {
	static Log LOGGER = LogFactory.getLog(HbaseUtil.class);

	static Integer hbase_zookeeper_pool = 50;
	String table;
	String family;
	int ttl;
	protected Connection connection = null;

	static {
		/**
		 * 此代码为了防止 windows下 不设置 hadoop.home.dir 会报错 但设不设值都不影响程序执行
		 */
		String osName = System.getProperty("os.name");
		if (osName.startsWith("Windows")) {
			try {
				String hadoop = HbaseUtil.class.getResource("/hadoop").getPath();
				if (hadoop == null || hadoop.indexOf("!") != -1) {
					String userdir = System.getProperty("user.dir");
					hadoop = "/" + userdir + "/config/hadoop";
				}
				hadoop = java.net.URLDecoder.decode(hadoop, "utf-8");
				System.setProperty("hadoop.home.dir", hadoop);
			} catch (Exception e) {
				LOGGER.error("set hadoopHome error", e);
			}
		}
	}

	@Override
	public String name() {
		return "HBASE";
	}

	public HbaseUtil(String zookeeper, int port, String table, String family, DatabaseWatcher waitch)
			throws IOException {
		this(zookeeper, port, table, family, 0, waitch);
	}

	public HbaseUtil(String zookeeper, int port, String table, String family, int ttl, DatabaseWatcher waitch)
			throws IOException {
		this.table = table;
		this.family = family;
		this.waitch = waitch;
		this.ttl = ttl;
		Configuration HBASE_CONFIG = HBaseConfiguration.create();
		HBASE_CONFIG.set("hbase.zookeeper.quorum", zookeeper);
		HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", port + "");
		HBASE_CONFIG.setInt("hbase.htable.threads.max", hbase_zookeeper_pool);
		this.connection = ConnectionFactory.createConnection(HBASE_CONFIG);
		LOGGER.info("HBase Connection");
	}

	/**
	 * 关闭到表的连接
	 */
	protected void closeTable(Table table) {
		try {
			if (table != null) {
				table.close();
			}
		} catch (Exception e) {
			LOGGER.error("HBase close exception", e);
		}
	}

	// 将getTable()由protected改为了public --Luva 20170428
	public Table getTable() throws DbException {
		try {
			return connection.getTable(TableName.valueOf(table));
		} catch (Exception e) {
			onException(e);
			throw new DbException("HBase getTable exception", e);
		}
	}

	@Override
	public void iterator(WitchData witch) throws DbException {
		if (waitch == null) {
			return;
		}
		Table table = getTable();
		try {
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes(family));
			scan.setCaching(100);
			ResultScanner scanner = table.getScanner(scan);
			Result result = null;
			while ((result = scanner.next()) != null) {
				String key = Bytes.toString(result.getRow());
				if (!result.isEmpty()) {
					List<Cell> cells = result.listCells();
					Map<String, String> returns = new HashMap<String, String>();
					for (Cell cell : cells) {
						String name = Bytes.toString(CellUtil.cloneQualifier(cell));
						String value = Bytes.toString(CellUtil.cloneValue(cell));
						returns.put(name, value);
					}
					witch.next(key, returns);
				}
			}
		} catch (Exception e) {
			throw new DbException("iterator data error", e);
		} finally {
			closeTable(table);
		}
	}

	@Override
	public boolean exist(String key) throws DbException {
		long time = System.currentTimeMillis();
		Table table = getTable();
		try {
			LOGGER.debug("exist by hbase:" + key);
			Get get = new Get(Bytes.toBytes(key));
			get.addFamily(Bytes.toBytes(family));
			Result result = table.get(get);
			time = System.currentTimeMillis() - time;
			MonitorLog.hbaseLog(MonitorOperationType.READ, time);
			if (result.getExists() == false || result.isEmpty()) {
				return false;
			}
			return true;
		} catch (Exception e) {
			onException(e);
			throw new DbException("exist error:" + key, e);
		} finally {
			closeTable(table);
		}
	}

	public void put(String key, Map<String, String> map) throws DbException {
		put(key, map, this.ttl);
	}

	@Override
	public void put(String key, Map<String, String> map, int ttl) throws DbException {
		long time = System.currentTimeMillis();
		Table table = getTable();
		try {
			LOGGER.debug("put by hbase:" + key);
			Put put = new Put(Bytes.toBytes(key), time);
			if (ttl != 0) {
				long safeTTL = ttl; // 当ttl 的值接近int的最大值时，将其*1000将会得到一个负值
				put.setTTL(safeTTL * 1000);
			}
			for (String name : map.keySet()) {
				String value = map.get(name);
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(name), Bytes.toBytes(value));
			}
			table.delete(new Delete(Bytes.toBytes(key), time - 1));
			table.put(put);
			time = System.currentTimeMillis() - time;
			MonitorLog.hbaseLog(MonitorOperationType.WRITE, time);
		} catch (Exception e) {
			onException(e);
			throw new DbException("put data error:" + key, e);
		} finally {
			closeTable(table);
		}
	}

	@Override
	public String get(String key, String field) throws DbException {
		long time = System.currentTimeMillis();
		Table table = getTable();
		try {
			LOGGER.debug("get field by hbase:" + key);
			Get get = new Get(Bytes.toBytes(key));
			get.addFamily(Bytes.toBytes(family));
			Result result = table.get(get);
			time = System.currentTimeMillis() - time;
			MonitorLog.hbaseLog(MonitorOperationType.READ, time);
			if (result.getExists() == false || result.isEmpty()) {
				return null;
			}
			return Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(field)));
		} catch (Exception e) {
			onException(e);
			throw new DbException("get field data error:" + key + ":" + field, e);
		} finally {
			closeTable(table);
		}
	}

	@Override
	public String randomKey() throws DbException {
		long time = System.currentTimeMillis();
		Table table = getTable();
		try {
			LOGGER.debug("get by hbase random key");
			Scan scan = new Scan(RandomUtils.nextString(1).getBytes());
			ResultScanner scanner = table.getScanner(scan);
			time = System.currentTimeMillis() - time;
			MonitorLog.hbaseLog(MonitorOperationType.READ, time);
			Result result = scanner.next();
			if (result == null || result.isEmpty()) {
				return null;
			}
			return Bytes.toString(result.getRow());
		} catch (Exception e) {
			onException(e);
			throw new DbException("get data error by random", e);
		} finally {
			closeTable(table);
		}
	}

	public Map<String, String> get(String key) throws DbException {
		if (key == null) {
			return null;
		}
		long time = System.currentTimeMillis();
		Table table = getTable();
		try {
			LOGGER.debug("get by hbase:" + key);
			Get get = new Get(Bytes.toBytes(key));
			get.addFamily(Bytes.toBytes(family));
			Result result = table.get(get);
			time = System.currentTimeMillis() - time;
			MonitorLog.hbaseLog(MonitorOperationType.READ, time);
			if (result.isEmpty()) {
				return null;
			}
			List<Cell> cells = result.listCells();
			Map<String, String> returns = new HashMap<String, String>();
			for (Cell cell : cells) {
				String name = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				returns.put(name, value);
			}
			return returns;
		} catch (Exception e) {
			onException(e);
			throw new DbException("get data error:" + key, e);
		} finally {
			closeTable(table);
		}
	}

	@Override
	public void delete(String key) throws DbException {
		long time = System.currentTimeMillis();
		Table table = getTable();
		try {
			LOGGER.debug("delete by hbase:" + key);
			Delete delete = new Delete(Bytes.toBytes(key));
			table.delete(delete);
			time = System.currentTimeMillis() - time;
			MonitorLog.hbaseLog(MonitorOperationType.WRITE, time);
		} catch (Exception e) {
			onException(e);
			throw new DbException("delete error:" + key, e);
		} finally {
			closeTable(table);
		}
	}

	@Override
	public void close() throws IOException {
		try {
			if (connection != null) {
				connection.close();
				LOGGER.info("Shutdown HBase Connection");
			}
		} catch (Exception e) {
			LOGGER.error("HBase close exception", e);
		}
	}

	@Override
	public boolean ping() {
		try {
			Table t = connection.getTable(TableName.valueOf(table));
			t.get(new Get(Bytes.toBytes("null")));
			return true;
		} catch (Exception e) {
			onException(e);
			return false;
		}
	}

}