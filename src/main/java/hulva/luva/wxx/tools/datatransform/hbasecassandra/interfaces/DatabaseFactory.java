package hulva.luva.wxx.tools.datatransform.hbasecassandra.interfaces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.exceptions.DbException;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.interfaces.db.CassandraUtil;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.interfaces.db.DatabaseInterface;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.interfaces.db.DatabaseWatcher;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.interfaces.db.HbaseUtil;

public class DatabaseFactory {
	private static final Logger logger = LoggerFactory.getLogger(DatabaseFactory.class);

	public static boolean cassandra_status;
	public static boolean hbase_status;
	public static boolean redis_status;
	
	static DatabaseInterface hbase;
	static DatabaseInterface cassandra;
	static DatabaseInterface redis;
	
	/**
	 * 创建HBase
	 */
	public static DatabaseInterface buildHBase(String zookeeper, int port, String table,String family, int ttl,DatabaseWatcher watcher) throws DbException{
		logger.info("Create HBase connection, Zookeeper:"+zookeeper+", Port:"+port+", Table:"+table+", Family:"+family);
		try {
			hbase = new HbaseUtil(zookeeper, port, table, family, ttl, watcher);
			return hbase;
		} catch (Exception e) {
			throw new DbException("connect hbase error", e);
		}
	}
	
	/**
	 * 创建Cassandra数据库连接
	 */
	public static DatabaseInterface buildCassandra(String cassandraHosts, String clusterName, String keyspacesName, String columnFamily, int ttl, DatabaseWatcher watcher) throws DbException{
		logger.info("Create Cassandra connection, Hosts:"+cassandraHosts+", ClusterName:"+clusterName+", KeySpaces:"+keyspacesName+", Family:"+columnFamily);
		try {
			cassandra = new CassandraUtil(cassandraHosts, clusterName, keyspacesName, columnFamily, ttl , watcher);
			return cassandra;
		} catch (Exception e) {
			throw new DbException("connect cassandra error", e);
		}
	}
	
}
