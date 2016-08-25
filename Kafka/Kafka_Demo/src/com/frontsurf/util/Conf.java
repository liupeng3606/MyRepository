package com.frontsurf.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
/**
 * 单例类,保存全局配置
 * @author Liupeng
 *
 */
public class Conf {
	/*
	 * 单例模式
	 */
	private static final Conf conf = new Conf();

	private final String zkHosts;
	private final String groupId;
	private final String topic;
	private final String kafkaBrokeList;
	private int interval;
	private int messageNum;
	private int sleep_time;

	private Conf() {
		/* IDE上测试 */
//		String baseDirectory = System.getProperty("user.dir");
//		String confFile = baseDirectory + "/config/conf.properties";
		/* 打包测试，需在conf.properties文件所在目录运行程序 */
		String confFile = "./conf.properties";
		Properties prop = new Properties();
		try {
		      FileInputStream in = new FileInputStream(confFile);
		      prop.load(in);
		    } catch (FileNotFoundException e) {
		      throw new ExceptionInInitializerError(e);
		    } catch (IOException e) {
		      throw new ExceptionInInitializerError(e);
		    }
		zkHosts = prop.getProperty("zkHosts");
		groupId = prop.getProperty("groupId");
		topic = prop.getProperty("topic");
		kafkaBrokeList = prop.getProperty("kafkaBrokeList");
		interval = Integer.parseInt(prop.getProperty("interval","5"));
		messageNum = Integer.parseInt(prop.getProperty("messageNum","1000"));
		sleep_time = Integer.parseInt(prop.getProperty("sleep_time","100"));
	}

	/**
	 * 返回单例类实例
	 * @return
	 */
	public static Conf getInstance() {
		return conf;
	}

	public String getZkHosts() {
		return zkHosts;
	}

	public String getGroupId() {
		return groupId;
	}

	public String getTopic() {
		return topic;
	}

	public String getKafkaBrokeList() {
		return kafkaBrokeList;
	}

	public int getInterval() {
		return interval;
	}

	public int getMessageNum() {
		return messageNum;
	}

	public int getSleep_time() {
		return sleep_time;
	}
}
