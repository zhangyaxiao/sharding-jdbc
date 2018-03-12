package io.shardingjdbc.service;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * <p>描述内容</br>这里填写类注释</p>
 *
 * @author zhangyaxiao
 * @version 2018/2/23
 */

public class SpringPropertyPlaceholderConfigurer  extends PropertyPlaceholderConfigurer {

    private List<String> diamondList;

    public List<String> getDiamondList() {
        return diamondList;
    }

    public void setDiamondList(List<String> diamondList) {
        this.diamondList = diamondList;
    }

    @Override
    protected void processProperties(ConfigurableListableBeanFactory beanFactoryToProcess, Properties props) throws BeansException {
        Properties properties = PropertiesUtils.getProperties(diamondList);
        if (properties == null) {
            String diamondFilePath = PropertiesUtils.DIAMOND_FILEPATH;//System.getProperty("user.home") + System.getProperty("file.separator") + ".diamond.domain";
            throw new RuntimeException("从diamond获取配置为空(dataId和group是" + diamondList + ")，请检查diamond要连接的环境:" + diamondFilePath);
        }
        this.setProperties(properties);
        for (Iterator<Object> iterator = properties.keySet().iterator(); iterator.hasNext();) {
            String key = (String) iterator.next();
            String value = (String) properties.get(key);
            props.setProperty(key, value);
        }
        super.processProperties(beanFactoryToProcess, properties);
    }
}
