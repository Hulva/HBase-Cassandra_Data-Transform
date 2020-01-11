package hulva.luva.wxx.tools.datatransform.hbasecassandra.model.requestbody;

import hulva.luva.wxx.tools.datatransform.hbasecassandra.model.HBaseInfo;

/**
 * @author Hulva Luva.H from ECBD
 * @date 2017年5月6日
 * @description
 *
 */
public class RequestParamHH {
	private HBaseInfo source;
	private HBaseInfo destination;

	public HBaseInfo getSource() {
		return source;
	}

	public void setSource(HBaseInfo source) {
		this.source = source;
	}

	public HBaseInfo getDestination() {
		return destination;
	}

	public void setDestination(HBaseInfo destination) {
		this.destination = destination;
	}

	@Override
	public String toString() {
		return "RequestParam [source=" + source + ", destination=" + destination + "]";
	}

}
