package hulva.luva.wxx.tools.datatransform.hbasecassandra.exceptions;

/**
 * 数据库相关异常
 */
public class DbException extends Exception{
	private static final long serialVersionUID = 3780917330757333709L;
	
	public DbException(String msg,Throwable e) {
		super(msg, e);
	}
	
	public DbException(String msg){
		super(msg);
	}
}
