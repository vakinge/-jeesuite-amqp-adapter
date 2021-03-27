package com.jeesuite.amqp.rocketmq;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import com.jeesuite.amqp.MQConsumer;
import com.jeesuite.amqp.MQMessage;
import com.jeesuite.amqp.MessageHandler;
import com.jeesuite.amqp.MessageHeaderNames;
import com.jeesuite.common.ThreadLocalContext;
import com.jeesuite.common.util.AssertUtil;
import com.jeesuite.common.util.ResourceUtils;

/**
 * 
 * <br>
 * Class Name   : RocketMQConsumer
 *
 * @author jiangwei
 * @version 1.0.0
 * @date 2019年5月19日
 */
public class RocketMQConsumer implements MQConsumer {
	
	private final static Logger logger = LoggerFactory.getLogger("com.zyframework.core.mq");

	private Environment environment;
	private String groupName;
	private String namesrvAddr;
	
	private Map<String, MessageHandler> messageHandlers = new HashMap<>(); 
	
	private DefaultMQPushConsumer consumer;

	
	/**
	 * @param groupName
	 * @param namesrvAddr
	 * @param messageHandlers
	 */
	public RocketMQConsumer(Environment environment,String groupName, Map<String, MessageHandler> messageHandlers) {
		super();
		this.environment = environment;
		this.groupName = groupName;
		this.namesrvAddr = environment.getProperty("mq.rocketmq.namesrvAddr");
		if(StringUtils.isBlank(this.namesrvAddr)){
			this.namesrvAddr = ResourceUtils.getProperty("mq.rocketmq.namesrvAddr");
		}
		AssertUtil.notBlank(namesrvAddr,"配置[mq.rocketmq.namesrvAddr]缺失");
		this.messageHandlers = messageHandlers;
	}



	/**
	 * @param groupName the groupName to set
	 */
	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	/**
	 * @param namesrvAddr the namesrvAddr to set
	 */
	public void setNamesrvAddr(String namesrvAddr) {
		this.namesrvAddr = namesrvAddr;
	}
	
	public void registerHanlder(String topic ,MessageHandler handler){
		messageHandlers.put(topic, handler);
	}

	@Override
	public void start() throws Exception {
		int consumeThreads = ResourceUtils.getInt("mq.consumer.threads", 20);
		if(environment.containsProperty("mq.consumer.threads")){
			consumeThreads = Integer.parseInt(environment.getProperty("mq.consumer.threads"));
		}
		consumer = new DefaultMQPushConsumer(groupName);
		consumer.setNamesrvAddr(namesrvAddr);
		consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMin(consumeThreads);
        consumer.setConsumeThreadMax(consumeThreads);
        consumer.setPullThresholdForQueue(1000);
        consumer.setConsumeConcurrentlyMaxSpan(500);
		for (String topic : messageHandlers.keySet()) {
			consumer.subscribe(topic, "*");
		}
		consumer.registerMessageListener(new customMessageListener());
		consumer.start();
	}
	
	private class customMessageListener implements MessageListenerConcurrently{
		@Override
		public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
			MQMessage message;
			for (MessageExt msg : msgs) {
				message = new MQMessage(msg.getTopic(),msg.getTags(),msg.getKeys(), msg.getBody());
				//message.setTransactionId(msg.getTransactionId());
				
				message.setRequestId(msg.getUserProperty(MessageHeaderNames.requestId.name()));
				message.setProduceBy(msg.getUserProperty(MessageHeaderNames.produceBy.name()));
				message.setTenantId(msg.getUserProperty(MessageHeaderNames.tenantId.name()));
				//多租户支持
				ThreadLocalContext.set(ThreadLocalContext.TENANT_ID_KEY, message.getTenantId());
				try {
					messageHandlers.get(message.getTopic()).process(message);
				} catch (Exception e) {
					logger.error(String.format("MQ_MESSAGE_CONSUME_ERROR ->message:%s",message.toString()),e);
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}finally{
					ThreadLocalContext.unset();
				}				
				if(logger.isDebugEnabled())logger.debug("MQ_MESSAGE_CONSUME_SUCCESS ->message:{}",message);
			}
			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		}
		
	}

	@Override
	public void shutdown() {
		consumer.shutdown();
	}
	
}
