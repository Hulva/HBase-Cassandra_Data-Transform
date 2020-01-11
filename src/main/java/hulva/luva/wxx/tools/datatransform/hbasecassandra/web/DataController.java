package hulva.luva.wxx.tools.datatransform.hbasecassandra.web;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import hulva.luva.wxx.tools.datatransform.hbasecassandra.model.requestbody.RequestParamCC;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.model.requestbody.RequestParamCH;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.model.requestbody.RequestParamHC;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.model.requestbody.RequestParamHH;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.service.DataCopyService;

@Controller
public class DataController {

	@Autowired
	private DataCopyService service;

	@RequestMapping(value = { "/datacopy/database/ttl" }, method = RequestMethod.GET)
	@ResponseBody
	public int getTTL() {
		return service.getTTL();
	}

	@RequestMapping(value = { "/datacopy/database/ttl" }, method = RequestMethod.POST)
	@ResponseBody
	public int setTTL(@RequestBody int ttl) {
		service.setTTL(ttl);
		return service.getTTL();
	}

	@RequestMapping(value = { "/datacopy/hbase2hbase" }, method = RequestMethod.POST)
	@ResponseBody
	public Map<String, String> copyHBase2HBase(@RequestBody RequestParamHH dbinfo) {
		Map<String, String> response = new HashMap<String, String>();

		if (DataCopyService.ISCOPYING) {
			response.put("message", "Copy is running. Check it with /datacopy/status in GET");
		} else {
			DataCopyService.ISCOPYING = true;
			new Thread(new Runnable() {
				@Override
				public void run() {
					service.doCopyHbase2Hbase(dbinfo.getSource(), dbinfo.getDestination());
				}
			}).start();
			response.put("message", "Start copy HBase to HBase success.");

		}
		response.put("code", "200");
		return response;
	}

	@RequestMapping(value = { "/datacopy/hbase2cassandra" }, method = RequestMethod.POST)
	@ResponseBody
	public Map<String, String> copyHBase2Cassandra(@RequestBody RequestParamHC dbinfo) {
		Map<String, String> response = new HashMap<String, String>();

		if (DataCopyService.ISCOPYING) {
			response.put("message", "Copy is running. Check it with /datacopy/status in GET");
		} else {
			DataCopyService.ISCOPYING = true;
			new Thread(new Runnable() {
				@Override
				public void run() {
					service.doCopyHbase2Cassandra(dbinfo.getSource(), dbinfo.getDestination());
				}
			}).start();
			response.put("message", "Start copy HBase to Cassandra success.");

		}
		response.put("code", "200");
		return response;
	}

	@RequestMapping(value = { "/datacopy/cassandra2cassandra" }, method = RequestMethod.POST)
	@ResponseBody
	public Map<String, String> copyCassandra2Cassandra(@RequestBody RequestParamCC dbinfo) {
		Map<String, String> response = new HashMap<String, String>();

		if (DataCopyService.ISCOPYING) {
			response.put("message", "Copy is running. Check it with /datacopy/status in GET");
		} else {
			DataCopyService.ISCOPYING = true;
			new Thread(new Runnable() {
				@Override
				public void run() {

					service.doCopyCassandra2Cassandra(dbinfo.getSource(), dbinfo.getDestination());
				}
			}).start();
			response.put("message", "Start copy Cassandra to Cassandra success.");

		}
		response.put("code", "200");
		return response;
	}

	@RequestMapping(value = { "/datacopy/cassandra2hbase" }, method = RequestMethod.POST)
	@ResponseBody
	public Map<String, String> copyCassandra2HBase(@RequestBody RequestParamCH dbinfo) {
		Map<String, String> response = new HashMap<String, String>();

		if (DataCopyService.ISCOPYING) {
			response.put("message", "Copy is running. Check it with /datacopy/status in GET");
		} else {
			DataCopyService.ISCOPYING = true;
			new Thread(new Runnable() {
				@Override
				public void run() {
					service.doCopyCassandra2Hbase(dbinfo.getSource(), dbinfo.getDestination());
				}
			}).start();
			response.put("message", "Start copy Cassandra to HBase success.");

		}
		response.put("code", "200");
		return response;
	}

	@RequestMapping(value = { "/datacopy/status" }, method = RequestMethod.GET)
	@ResponseBody
	public Map<String, Object> getStatus() {
		Map<String, Object> response = new HashMap<String, Object>();
		response.put("Code", "200");
		response.put("Time", "" + System.currentTimeMillis());
		response.put("JobStatus", DataCopyService.JOBSTATUS);
		response.put("CurrentRow", "" + DataCopyService.CURRENTROW);
		response.put("RunningFunction", DataCopyService.RUNNINGFUNCTION);
		response.put("enable", service.enableStatus());

		return response;
	}
	
	@RequestMapping(value = { "/datacopy/enable" }, method = RequestMethod.GET)
	@ResponseBody
	public Map<String, Object> chnageEnableStatus() {
		if (service.enableStatus()) {
			service.disable();
		} else {
			service.enable();
		}
		Map<String, Object> response = new HashMap<String, Object>();
		response.put("Code", "200");
		response.put("Time", "" + System.currentTimeMillis());
		response.put("JobStatus", DataCopyService.JOBSTATUS);
		response.put("CurrentRow", "" + DataCopyService.CURRENTROW);
		response.put("RunningFunction", DataCopyService.RUNNINGFUNCTION);
		response.put("enable", service.enableStatus());

		return response;
	}
}
