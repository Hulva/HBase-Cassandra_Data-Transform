package hulva.luva.wxx.tools.datatransform.hbasecassandra.model.requestbody;

import hulva.luva.wxx.tools.datatransform.hbasecassandra.model.CassandraInfo;

/**
 * @author Hulva Luva.H from ECBD
 * @date 2017年5月6日
 * @description
 *
 */
public class RequestParamCC {
	private CassandraInfo source;
	private CassandraInfo destination;

	public CassandraInfo getSource() {
		return source;
	}

	public void setSource(CassandraInfo source) {
		this.source = source;
	}

	public CassandraInfo getDestination() {
		return destination;
	}

	public void setDestination(CassandraInfo destination) {
		this.destination = destination;
	}

	@Override
	public String toString() {
		return "RequestParam [source=" + source + ", destination=" + destination + "]";
	}

}
