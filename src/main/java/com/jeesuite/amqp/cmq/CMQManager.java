package com.jeesuite.amqp.cmq;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jeesuite.common.JeesuiteBaseException;
import com.jeesuite.common.json.JsonUtils;
import com.jeesuite.common.util.ResourceUtils;
import com.qcloud.cmq.Account;
import com.qcloud.cmq.Queue;
import com.qcloud.cmq.QueueMeta;
import com.qcloud.cmq.Subscription;
import com.qcloud.cmq.Topic;
import com.qcloud.cmq.entity.CmqConfig;

/**
 * 
 * <br>
 * Class Name   : CMQManager
 *
 * @author jiangwei
 * @version 1.0.0
 * @date 2019年4月23日
 */
public class CMQManager {

	private static Logger logger = LoggerFactory.getLogger("com.zyframework.core.mq");
	private Account account;
	
	private String queueName;
	private volatile Queue queue;
	private Map<String, Topic> topicMappings = new ConcurrentHashMap<String, Topic>();
	
	private static CMQManager instance = new CMQManager();
	
	private boolean useTopicMode;

	private CMQManager() {
		doInit();
	}

	private void doInit() {
		queueName = ResourceUtils.getProperty("mq.cmq.queueName");
		CmqConfig config = new CmqConfig();
		Field[] fields = CmqConfig.class.getDeclaredFields();
		String configKey;
		Object configValue;
		for (Field field : fields) {
			field.setAccessible(true);
			configKey = "mq.cmq." + field.getName();
			if(ResourceUtils.containsProperty(configKey)){
				if(field.getType() == int.class || field.getType() == Integer.class){
					configValue = ResourceUtils.getInt(configKey);
				}else if(field.getType() == long.class || field.getType() == Long.class){
					configValue = ResourceUtils.getLong(configKey);
				}else if(field.getType() == boolean.class || field.getType() == Boolean.class){
					configValue = ResourceUtils.getBoolean(configKey);
				}else{
					configValue = ResourceUtils.getProperty(configKey);
				}
				try {field.set(config, configValue);} catch (Exception e) {}
			}
		}
		
		Validate.notBlank(config.getEndpoint(),"config[mq.cmq.endpoint] not found");
		Validate.notBlank(config.getSecretId(),"config[mq.cmq.secretId] not found");
		Validate.notBlank(config.getSecretKey(),"config[mq.cmq.secretKey] not found");
		
		useTopicMode = config.getEndpoint().contains("cmq-topic");

		config.setAlwaysPrintResultLog(false);
		config.setPrintSlow(false);
		this.account = new Account(config);		
		logger.info("init CMQ Account OK -> endpoint:{}",config.getEndpoint());
	}
	

	public static Account getAccount() {
		return instance.account;
	}
	
	public static Queue getQueue() {
		if(instance.queue != null) {
			return instance.queue;
		}
		synchronized (instance) {
			if(instance.queue != null) {
				return instance.queue;
			}
			instance.queue = instance.createQueueIfAbsent();
		}
		return instance.queue;
	}

	public static boolean isTopicMode() {
		return instance.useTopicMode;
	}

	private Queue createQueueIfAbsent(){
		Validate.notBlank(queueName,"config[mq.cmq.queueName] not found");
		Queue queue = account.getQueue(queueName);
		try {
			List<String> existList = new ArrayList<>(1);
			account.listQueue(queueName, -1, -1, existList);
			if(!existList.contains(queueName)) {
				queue = createQueue(queueName);
			}
			QueueMeta meta = queue.getQueueAttributes();
			System.out.println(">>QueueMeta:" + JsonUtils.toJson(meta));
		}catch (Exception e) {
			throw new JeesuiteBaseException(e.getMessage());
		} 
		return queue;
	}
	
	public static Topic createTopicIfAbsent(String topicName){
		if(instance.topicMappings.containsKey(topicName)){
			return instance.topicMappings.get(topicName);
		}
		Topic topic = getAccount().getTopic(topicName);
		
		try {
			List<String> existList = new ArrayList<>(1);
			getAccount().listTopic(topicName, existList, -1, -1);
			if (!existList.contains(topicName)) {
				createTopic(topicName);
			}
		}catch (Exception e) {
			throw new RuntimeException(e);
		} 
		instance.topicMappings.put(topicName, topic);
		return topic;
	}
	
	public static Subscription createSubscriptionIfAbsent(final String topicName){
		if(!isTopicMode())return null;
		Validate.notBlank(instance.queueName,"config[mq.cmq.queueName] not found");
		Topic topic = CMQManager.createTopicIfAbsent(topicName);
		String subscriptionName = buildSubscriptionName(topicName,instance.queueName);
		Subscription subscription = getAccount().getSubscription(topicName, subscriptionName);
		try {
			List<String> existList = new ArrayList<>(1);
			topic.ListSubscription(-1, -1, subscriptionName, existList);
			if(!existList.contains(subscriptionName)) {
				createSubscription(topicName, instance.queueName, subscriptionName);
			}
			logger.info(">>subscriptionName:{} for queue:{},topic:{}",subscriptionName,instance.queueName,topicName);
		}catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return subscription;
	}
	
	
	/**
	 * @param topicName
	 * @param queueName
	 * @return
	 */
	private static String buildSubscriptionName(String topicName, String queueName) {
		return String.format("sub-for_%s-%s", queueName,topicName);
	}

	private Queue createQueue(String queueName) {
		try {
			QueueMeta meta = new QueueMeta();
			meta.pollingWaitSeconds = ResourceUtils.getInt("mq.cmq.consumer.pollingWaitSeconds",0);
			meta.visibilityTimeout = ResourceUtils.getInt("mq.cmq.consumer.visibilityTimeout",30);//如果 Worker 在 VisibilityTImeout 时间内没能处理完 Message，则消息就有可能被其他 Worker 接收到并处理。
			meta.maxMsgSize = 1048576;
			meta.msgRetentionSeconds = 345600;
			account.createQueue(queueName, meta);
			//
			logger.info("createQueue finished -> queueName:{}",queueName);
		} catch (Exception e) {
			if(!e.getMessage().contains("is already existed")){				
				throw new JeesuiteBaseException(e.getMessage());
			}
			logger.info("queueName:{} is already existed",queueName);
		}
		return account.getQueue(queueName);
	}
	
	private static void createTopic(String topicName) {
		try {
			logger.info("createTopic begin -> topicName:",topicName);
			int maxMsgSize = 1024*1024;
			getAccount().createTopic(topicName, maxMsgSize);
			logger.info("createTopic finished -> topicName:{}",topicName);
		}catch (Exception e) {
			if(!e.getMessage().contains("is already existed")){				
				throw new JeesuiteBaseException(e.getMessage());
			}
			logger.info("topicName:{} is already existed",topicName);
		}
	}
	
	private static void createSubscription(String topicName,String queueName,String subscriptionName){
		try {
			getAccount().createSubscribe(topicName, subscriptionName, queueName, "queue");
			logger.info("createSubscription finished -> subscriptionName:{}",subscriptionName);
		}catch (Exception e) {
			if(!e.getMessage().contains("is already existed")){				
				throw new JeesuiteBaseException(e.getMessage());
			}
			logger.info("subscriptionName:{} is already existed",subscriptionName);
		}
	}
	
}
