package com.canal.server;

import com.alibaba.otter.canal.deployer.CanalController;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * canal服务启动类
 */
public class CanalLauncher implements InitializingBean {
    private static final String CLASSPATH_PREFIX = "classpath:";
    private static final Logger logger=LoggerFactory.getLogger(CanalLauncher.class);

    private String canalProperties;
    @Override
    public void afterPropertiesSet() throws Exception {
        try{
            //加载canal server配置文件
            Assert.notNull(this.canalProperties,"canalProperties is null or empty");
            String conf = System.getProperty("canal.conf",this.canalProperties);
            Properties properties = new Properties();
            if(conf.startsWith(CLASSPATH_PREFIX)){
                conf = StringUtils.substringAfter(conf,CLASSPATH_PREFIX);
                properties.load(CanalLauncher.class.getClassLoader().getResourceAsStream(conf));
            }else{
                properties.load(new FileInputStream(conf));
            }
            //启动canal server服务
            logger.info(" start the canal server ......");
            final CanalController controller = new CanalController();
            controller.start();
            logger.info(" the canal server is running now ....");

            Runtime.getRuntime().addShutdownHook(new Thread(){
                @Override
                public void run(){
                    try{
                        CanalLauncher.logger.info("## stop the canal server");
                        controller.stop();
                    }catch (Throwable t){
                        logger.error(" errors happen when stopping canal Server",t);
                        System.exit(0);
                    }
                }
            });
        }catch (Throwable t){
            logger.error(" errors happen when starting canal Server",t);
            System.exit(0);
        }
    }

    public void setCanalProperties(String canalProperties) {
        this.canalProperties = canalProperties;
    }
}
