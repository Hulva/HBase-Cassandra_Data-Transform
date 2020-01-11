package hulva.luva.wxx.tools.datatransform.hbasecassandra.model;

/**
 * @author Hulva Luva.H from ECBD
 * @date 2017年5月5日
 * @description
 *
 */
public class CassandraInfo extends DBInfo {

	private String cassandraHosts;
	private String cassandraClusterName;
	private String cassandraKeyspace;
	private String cassandraColumnFamily;

	public CassandraInfo() {
		this.dbName = "Cassandra";
	}

	public CassandraInfo(String cassandraHosts, String cassandraClusterName, String cassandraKeyspace,
			String cassandraColumnFamily) {
		this.dbName = "Cassandra";
		setCassandraHosts(cassandraHosts);
		setCassandraClusterName(cassandraClusterName);
		setCassandraKeyspace(cassandraKeyspace);
		setCassandraColumnFamily(cassandraColumnFamily);
	}

	public String getCassandraHosts() {
		return cassandraHosts;
	}

	public void setCassandraHosts(String cassandraHosts) {
		this.cassandraHosts = cassandraHosts;
	}

	public String getCassandraClusterName() {
		return cassandraClusterName;
	}

	public void setCassandraClusterName(String cassandraClusterName) {
		this.cassandraClusterName = cassandraClusterName;
	}

	public String getCassandraKeyspace() {
		return cassandraKeyspace;
	}

	public void setCassandraKeyspace(String cassandraKeyspace) {
		this.cassandraKeyspace = cassandraKeyspace;
	}

	public String getCassandraColumnFamily() {
		return cassandraColumnFamily;
	}

	public void setCassandraColumnFamily(String cassandraColumnFamily) {
		this.cassandraColumnFamily = cassandraColumnFamily;
	}

	@Override
	public String toString() {
		return "CassandraInfo [cassandraHosts=" + cassandraHosts + ", cassandraClusterName=" + cassandraClusterName
				+ ", cassandraKeyspace=" + cassandraKeyspace + ", cassandraColumnFamily=" + cassandraColumnFamily + "]";
	}

}
