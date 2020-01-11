package hulva.luva.wxx.tools.datatransform.hbasecassandra;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * 
  * @ClassName: HBaseCassandraDataTransform
  * @Description: HBase Cassandra data transform
  * @author Hulva Luva
  * @date 2016年8月18日 上午10:34:02
  *
 */
@SpringBootApplication
@ComponentScan("hulva.luva.wxx.tools.datatransform.hbasecassandra.**")
public class HBaseCassandraDataTransform{
	
	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(HBaseCassandraDataTransform.class); 
        app.run(args);
	}
}
