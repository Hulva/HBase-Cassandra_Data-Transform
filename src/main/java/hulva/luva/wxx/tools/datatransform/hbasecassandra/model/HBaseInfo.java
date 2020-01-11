package hulva.luva.wxx.tools.datatransform.hbasecassandra.model;

/**
 * @author Hulva Luva.H from ECBD
 * @date 2017年5月6日
 * @description
 *
 */
public class HBaseInfo extends DBInfo {
	private String hbaseZKQuorum;
	private String hbaseZKPropertyClientPort;
	private String hbaseTable;
	private String hbaseColumnFamily;

	public HBaseInfo() {
		this.dbName = "HBase";
	}

	public HBaseInfo(String hbaseZKQuorum, String hbaseZKPropertyClientPort, String hbaseTable,
			String hbaseColumnFamily) {
		this.dbName = "HBase";
		setHbaseZKQuorum(hbaseZKQuorum);
		setHbaseZKPropertyClientPort(hbaseZKPropertyClientPort);
		setHbaseTable(hbaseTable);
		setHbaseColumnFamily(hbaseColumnFamily);
	}

	public String getHbaseZKQuorum() {
		return hbaseZKQuorum;
	}

	public void setHbaseZKQuorum(String hbaseZKQuorum) {
		this.hbaseZKQuorum = hbaseZKQuorum;
	}

	public String getHbaseZKPropertyClientPort() {
		return hbaseZKPropertyClientPort;
	}

	public void setHbaseZKPropertyClientPort(String hbaseZKPropertyClientPort) {
		this.hbaseZKPropertyClientPort = hbaseZKPropertyClientPort;
	}

	public String getHbaseTable() {
		return hbaseTable;
	}

	public void setHbaseTable(String hbaseTable) {
		this.hbaseTable = hbaseTable;
	}

	public String getHbaseColumnFamily() {
		return hbaseColumnFamily;
	}

	public void setHbaseColumnFamily(String hbaseColumnFamily) {
		this.hbaseColumnFamily = hbaseColumnFamily;
	}

	@Override
	public String toString() {
		return "HBaseInfo [hbaseZKQuorum=" + hbaseZKQuorum + ", hbaseZKPropertyClientPort=" + hbaseZKPropertyClientPort
				+ ", hbaseTable=" + hbaseTable + ", hbaseColumnFamily=" + hbaseColumnFamily + "]";
	}

}
