package com.jeesuite.amqp;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;

import com.jeesuite.amqp.cmq.CMQConsumer;
import com.jeesuite.amqp.cmq.CMQProducer;
import com.jeesuite.amqp.memoryqueue.MemoryQueueProducer;
import com.jeesuite.amqp.rocketmq.RocketMQConsumer;
import com.jeesuite.amqp.rocketmq.RocketMQProducer;
import com.jeesuite.common.JeesuiteBaseException;
import com.jeesuite.common.util.ResourceUtils;
import com.jeesuite.spring.helper.SpringAopHelper;

/**
 * 
 * <br>
 * Class Name   : MQInstanceDelegate
 *
 * @author jiangwei
 * @version 1.0.0
 * @date 2019年3月16日
 */
public class MQInstanceBaseDelegate implements DisposableBean,ApplicationContextAware,EnvironmentAware{

	protected  static final Logger logger = LoggerFactory.getLogger("com.zyframework.core.mq");
	
	private Environment environment;
	private ApplicationContext applicationContext;
	private MQConsumer consumer;
	private MQProducer producer;
	
	@Value("${mq.groupName}")
	private String groupName;
	@Value("${mq.provider}")
	private String providerName;
	@Value("${mq.namespace:none}")
	private String namespace;
	@Value("${mq.producer.enabled:true}")
	private boolean producerEnabled;
	@Value("${mq.consumer.enabled:false}")
	private boolean consumerEnabled;
	
	private String namespacePrefix;
	
	private static MQInstanceBaseDelegate deledate;

	public void start() throws Exception {
		if(!"none".equals(namespace)){
			namespacePrefix = namespace + "_";
		}
		
		if("cmq".equals(providerName)){
			if(ResourceUtils.containsProperty("mq.cmq.queueName")) {				
				String queueName = ResourceUtils.getProperty("mq.cmq.queueName");
				ResourceUtils.add("mq.cmq.queueName", rebuildWithNamespace(queueName));
			}
		}
		//
		groupName = rebuildWithNamespace(groupName);
		//
		if(producerEnabled){			
			startProducer();
		}
		//
		if(consumerEnabled){			
			startConsumer();
		}
		//
		MQInstanceBaseDelegate.deledate = this;
	}

	/**
	 * 
	 */
	private void startConsumer() throws Exception{
		Map<String, MessageHandler> messageHanlders = applicationContext.getBeansOfType(MessageHandler.class);
		if(messageHanlders != null && !messageHanlders.isEmpty()){
			Map<String, MessageHandler> messageHandlerMaps = new HashMap<>(); 
			messageHanlders.values().forEach(e -> {
				Object origin = e;
				try {origin = SpringAopHelper.getTarget(e);} catch (Exception ex) {ex.printStackTrace();}
				MQTopicRef topicRef = origin.getClass().getAnnotation(MQTopicRef.class);
				String topicName = rebuildWithNamespace(topicRef.value());
				messageHandlerMaps.put(topicName, e);
				logger.info("ADD MQ_COMSUMER_HANDLER ->topic:{},handlerClass:{} ",topicName,e.getClass().getName());
			});
			
			if("rocketmq".equals(providerName)){
				consumer = new RocketMQConsumer(environment,groupName,messageHandlerMaps);
			}else if("cmq".equals(providerName)){
				consumer = new CMQConsumer(groupName,messageHandlerMaps);
			}else if("memoryqueue".equals(providerName)){
				MemoryQueueProducer.setMessageHandlers(messageHandlerMaps);
			}else{
				throw new JeesuiteBaseException("NOT_SUPPORT[providerName]:" + providerName);
			}
			
			consumer.start();
			logger.info("MQ_COMSUMER started -> groupName:{},providerName:{}",groupName,providerName);
		}
	}

	private void startProducer() throws Exception{
		
		if("rocketmq".equals(providerName)){
			producer = new RocketMQProducer(environment,groupName);
		}else if("cmq".equals(providerName)){
			producer = new CMQProducer();
		}else if("memoryqueue".equals(providerName)){
			producer = new MemoryQueueProducer();
		}else{
			throw new JeesuiteBaseException("NOT_SUPPORT[providerName]:" + providerName);
		}
		producer.start();
		logger.info("MQ_PRODUCER started -> groupName:{},providerName:{}",groupName,providerName);
	}

	@Override
	public void destroy() throws Exception {
		try {
			if(consumer != null)consumer.shutdown();
			consumer = null;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			if(producer != null)producer.shutdown();
			producer = null;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static void send(MQMessage message){
		if(deledate.producer == null){
			logger.warn("MQProducer did not Initialization,Please check config[mq.provider] AND [mq.producer.enabled]");
			return;
		}
		message.setTopic(deledate.rebuildWithNamespace(message.getTopic()));
		deledate.producer.sendMessage(message, false);
	}
	
    public static void asyncSend(MQMessage message){
    	if(deledate.producer == null){
    		logger.warn("MQProducer did not Initialization,Please check config[mq.provider] AND [mq.producer.enabled]");
    		return;
		}
    	message.setTopic(deledate.rebuildWithNamespace(message.getTopic()));
    	deledate.producer.sendMessage(message, true);
	}
    
    public static boolean producerRegistered(){
    	return deledate != null && deledate.producer != null;
    }
    
	public static MQConsumer getConsumer() {
		return deledate.consumer;
	}

	public static MQProducer getProducer() {
		return deledate.producer;
	}

	private String rebuildWithNamespace(String topicName){
    	if(namespacePrefix == null)return topicName;
    	return namespacePrefix + topicName;
    }

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}
}
