package hulva.luva.wxx.tools.datatransform.hbasecassandra.log;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MonitorLog {
	static final Log LOG =  LogFactory.getLog("monitor");
	
	public static boolean enableLog = true;
	public static boolean enableDBLog = false;
	
	public static boolean enableRedisLog = true;
	public static boolean enableHBaseLog = true;
	public static boolean enableCassandraLog = true;
	
	public static boolean enableJumpLog = true;
	public static boolean enableKafkaLog = true;
	public static boolean enableRestLog = true;
	
	public static void restLog(MonitorOperationType type, long time){
		if(enableRestLog){
			log("REST", type.name(), time);
		}
	}

	public static void redisLog(MonitorOperationType type, long time){
		if(enableRedisLog){
			log("REDIS", type.name(), time);
		}
	}
	
	public static void hbaseLog(MonitorOperationType type, long time){
		if(enableHBaseLog){
			log("HBASE", type.name(), time);
		}
	}
	
	public static void cassandraLog(MonitorOperationType type, long time){
		if(enableCassandraLog){
			log("CASSANDRA", type.name(), time);
		}
	}
	
	public static void db(MonitorOperationType type,long time) {
		if(enableDBLog){
			log("DB", type.name(), time);
		}
	}
	
	public static void jumpLog(long time){
		if(enableJumpLog){
			log("JUMP", null, time);
		}
	}
	
	public static void kafkaLog(long time){
		if(enableKafkaLog){
			log("KAFKA", null, time);
		}
	}
	
    public static String join(String[] array, String separator) {
        if (array == null) {
            return null;
        }
        if (separator == null) {
            separator = "";
        }
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < array.length; i++) {
        	if(i != 0){
        		buf = buf.append(separator);
        	}
        	buf = buf.append(array[i]);
		}
        return buf.toString();
    }
    
	private static void log(final String action, final String type, final long time) {
		if(enableLog){
			String[] log = new String[]{action, System.currentTimeMillis() + "", time + "", type == null?"TIME":type};
			LOG.debug(join(log, " "));
		}
	}
	
	public enum MonitorOperationType {
		READ,
		WRITE
	}
	
	public static void main(String[] args) {
		int i = 100;
		while(i-- > 0){
			redisLog(MonitorOperationType.READ, 25L);
		}
	}

}