package hulva.luva.wxx.tools.datatransform.hbasecassandra;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import hulva.luva.wxx.tools.datatransform.hbasecassandra.model.CassandraInfo;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.model.HBaseInfo;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.service.DataCopyService;

@RunWith(SpringJUnit4ClassRunner.class)
@ComponentScan(basePackages = "hulva.luva.wxx.tools.datatransform.hbasecassandra.**")
public class DataServiceTest {

	private static final String hbaseZKQuorum = null;

	private static final String hbaseZKPropertyClientPort = null;

	private static final String hbaseTable = null;

	private static final String hbaseColumnFamily = null;

	private static final String cassandraHosts = null;

	private static final String cassandraClusterName = null;

	private static final String cassandraKeyspace = null;

	private static final String cassandraColumnFamily = null;

	@Autowired
	private DataCopyService service;

	CassandraInfo cassandraInfo = null;
	HBaseInfo hbaseInfo = null;

	public void initDbInfo() {
		hbaseInfo = new HBaseInfo(hbaseZKQuorum, hbaseZKPropertyClientPort, hbaseTable, hbaseColumnFamily);
		cassandraInfo = new CassandraInfo(cassandraHosts, cassandraClusterName, cassandraKeyspace,
				cassandraColumnFamily);
	}

	@Test
	public void testCassandra2HBase() {
		service.doCopyCassandra2Hbase(cassandraInfo, hbaseInfo);
	}

	@Test
	public void testHBase2Cassandra() {
		service.doCopyHbase2Cassandra(hbaseInfo, cassandraInfo);
	}

}
