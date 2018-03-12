package io.shardingjdbc.service;

import com.taobao.diamond.manager.ManagerListenerAdapter;
import com.taobao.diamond.manager.impl.DefaultDiamondManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * <p>工具类，获取diamond配置</p>
 *
 * @author zhangyaxiao
 * @version 2018/1/26
 */

public class PropertiesUtils {

        public static Properties properties;

        private static Logger logger = Logger.getLogger(PropertiesUtils.class);
        private static final long TIME_OUT = 5000L;
        private static String diamondIpList;
        private static List<String> diamondIdgroupList;
        protected static final String DIAMOND_FILEPATH="diamond.data";

        public static Properties getProperties(List<String> diamondList) {
            diamondIdgroupList = diamondList;
            if (null == properties) {
                init();
            }
            return properties;
        }

        public static Properties getProperties() {
            if (null == properties) {
                init();
            }
            return properties;
        }

        /**
         * 根据key从map中取值
         *
         * @param key
         * @return
         */
        public static Object getValueByKey(String key) {
            if (null == properties) {
                init();
            }
            return properties.get(key);
        }

        public static String getStringValueByKey(String key) {
            return (String) getValueByKey(key);
        }

        public static int getIntValueByKey(String key) {
            return Integer.parseInt((String) getValueByKey(key));
        }

        public static double getDoubleValueByKey(String key) {
            return Double.parseDouble((String) getValueByKey(key));
        }

        public static boolean getBooleanValueByKey(String key) {
            return Boolean.parseBoolean((String) (getValueByKey(key)));
        }

        public static String getStringValueByKey(String key, String defaultV) {
            Object value = getValueByKey(key);
            if (value == null) {
                return defaultV;
            }
            return (String) value;
        }

        public static int getIntValueByKey(String key, int defaultV) {
            Object value = getValueByKey(key);
            if (value == null) {
                return defaultV;
            }
            return Integer.parseInt((String) value);
        }

        public static double getDoubleValueByKey(String key, double defaultV) {
            Object value = getValueByKey(key);
            if (value == null) {
                return defaultV;
            }
            return Double.parseDouble((String) value);
        }

        public static boolean getBooleanValueByKey(String key, boolean defaultV) {
            Object value = getValueByKey(key);
            if (value == null) {
                return defaultV;
            }
            return Boolean.parseBoolean((String) (value));
        }

        /**
         * init(读取多个dataId 与 groupId )
         *
         * @param diamondList
         * @return void
         * @author luantian
         * @exception
         * @since 1.0.0
         */
        private static void init() {

            String diamondFilePath = PropertiesUtils.class.getClassLoader().getResource(DIAMOND_FILEPATH).getPath() ;//System.getProperty("user.home") + "/.diamond.domain";
            try {

                List<String> contentList = FileUtils.readLines(new File(diamondFilePath), "UTF-8");
                for (String ipList : contentList) {
                    if (!ipList.contains("#")) {
                        diamondIpList = ipList.trim();
                        break;
                    }
                }
            } catch (Exception e) {
                logger.error("获取diamond文件内容失败：" + e.getMessage(), e);
            }
            logger.info("diaond-->filePath:" + diamondFilePath + " change diamondIpList:" + diamondIpList);
            if (diamondIdgroupList != null && diamondIpList != null) {
                for (String str : diamondIdgroupList) {
                    // dataid
                    String dataId = "";
                    String groupId = "";
                    if (str.indexOf(":") > -1) {
                        dataId = str.substring(0, str.indexOf(":"));
                    }
                    if (str.lastIndexOf(":") > -1) {
                        groupId = str.substring(str.indexOf(":") + 1,str.length());
                    }
                    if (!StringUtils.isEmpty(dataId) && !StringUtils.isEmpty(groupId)) {
                        DefaultDiamondManager manager = new DefaultDiamondManager(dataId, groupId, new ManagerListenerAdapter() {
                            public void receiveConfigInfo(String configInfo) {
                                //数据发生变更时，更新数据
                                putAndUpdateProperties(configInfo);
                            }
                        }, diamondIpList);
                        String configInfo = manager.getAvailableConfigureInfomation(TIME_OUT);
                        logger.debug("从diamond取到的数据是：" + configInfo);
                        putAndUpdateProperties(configInfo);
                    } else {
                        logger.error("diamond数据配置properties异常: DataId:" + dataId + ",Group:" + groupId);
                    }
                }
            } else {
                logger.error("diamond数据配置properties异常: diamondBeanList is null or diamondIpList is null");
            }
        }

        /**
         * 更新properties中数据
         *
         * @param configInfo
         */
        public static void putAndUpdateProperties(String configInfo) {
            if (StringUtils.isNotEmpty(configInfo)) {
                if (properties == null) {
                    properties = new Properties();
                }
                try {
                    properties.load(new ByteArrayInputStream(configInfo.getBytes()));
                } catch (IOException e) {
                    logger.error("根据diamond数据流转成properties异常" + e.getMessage(), e);
                }
            } else {
                logger.error("从diamond取出的数据为空，请检查配置");
            }
        }

        public static List<String> getDiamondIdgroupList() {
            return diamondIdgroupList;
        }

        public static void setDiamondIdgroupList(List<String> diamondIdgroupList) {
            PropertiesUtils.diamondIdgroupList = diamondIdgroupList;
        }

        public String getDiamondIpList() {
            return diamondIpList;
        }

}
