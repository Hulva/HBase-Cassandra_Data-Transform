package hulva.luva.wxx.tools.datatransform.hbasecassandra.model.requestbody;

import hulva.luva.wxx.tools.datatransform.hbasecassandra.model.CassandraInfo;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.model.HBaseInfo;

/**
 * @author Hulva Luva.H from ECBD
 * @date 2017年5月6日
 * @description
 *
 */
public class RequestParamHC {
	private HBaseInfo source;
	private CassandraInfo destination;

	public HBaseInfo getSource() {
		return source;
	}

	public void setSource(HBaseInfo source) {
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
