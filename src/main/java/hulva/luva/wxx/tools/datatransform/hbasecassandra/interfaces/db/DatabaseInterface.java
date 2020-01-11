package hulva.luva.wxx.tools.datatransform.hbasecassandra.interfaces.db;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import hulva.luva.wxx.tools.datatransform.hbasecassandra.exceptions.DbException;

public abstract class DatabaseInterface implements Closeable{
	static Log LOGGER = LogFactory.getLog(DatabaseInterface.class);
	
	static ExecutorService pool = Executors.newSingleThreadExecutor();
	boolean isStoped = false;
	boolean isException = false;
	protected DatabaseWatcher waitch;
	protected static String VersionColumn = "v";
	protected Lock lock = new ReentrantLock();
	protected String exceptionMessage = null;
	
	public abstract boolean ping();
	
	public abstract String name();
	
	public abstract String randomKey() throws DbException;
	
	public abstract boolean exist(String key) throws DbException;
	
	public abstract void put(String key, Map<String, String> map) throws DbException;
	
	public abstract void put(String key, Map<String, String> map, int ttl) throws DbException;
	
	public abstract Map<String, String> get(String key) throws DbException;
	
	public abstract String get(String key, String field) throws DbException;
	
	public abstract void delete(String key) throws DbException;
	
	public abstract void iterator(WitchData witch) throws DbException;
	
	public synchronized void onException(final Throwable t){
		exceptionMessage = t.getMessage();
		LOGGER.error("",t);
		if(isStoped || isException){return;}
		isException = true;
		if(ping()){ return; }
		isStoped = true;
		if(waitch != null){
			waitch.onStop(t);//先通知系统集群失败情况
		}
		pool.execute(new Runnable() {
			public void run() {
				synchronized (lock) {
					int waitTime = 0;
					while(isStoped && !ping()){
						if(waitTime < 60){
							waitTime += 5;
						}
						try { lock.wait(waitTime * 1000); } catch (InterruptedException e1) { e1.printStackTrace(); }
					}
					isStoped = false;
					isException = false;
					if(waitch != null){
						exceptionMessage = null;
						waitch.onRecover();
					}
				}
			}
		});
	};
	
	public String exceptionMessage(){
		return exceptionMessage;
	}

	public interface WitchData{
		public void next(String key, Map<String, String> data) throws Exception;
	}
}
