package hulva.luva.wxx.tools.datatransform.hbasecassandra.interfaces.db;

public interface DatabaseWatcher{
	
	public void onStop(Throwable t);
	public void onRecover();
}