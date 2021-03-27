package com.jeesuite.amqp.rocketmq;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import com.jeesuite.amqp.MQMessage;
import com.jeesuite.amqp.MQProducer;
import com.jeesuite.common.util.AssertUtil;
import com.jeesuite.common.util.ResourceUtils;

/**
 * 
 * <br>
 * Class Name   : RocketMQProducer
 *
 * @author jiangwei
 * @version 1.0.0
 * @date 2019年5月19日
 */
public class RocketMQProducer implements MQProducer {

	private final Logger logger = LoggerFactory.getLogger("com.zyframework.core.mq");
	
	private String groupName;
	private String namesrvAddr;
	
	private DefaultMQProducer producer;
	
	/**
	 * @param groupName
	 * @param namesrvAddr
	 */
	public RocketMQProducer(Environment environment,String groupName) {
		super();
		this.groupName = groupName;
		this.namesrvAddr = environment.getProperty("mq.rocketmq.namesrvAddr");
		if(StringUtils.isBlank(this.namesrvAddr)){
			this.namesrvAddr = ResourceUtils.getProperty("mq.rocketmq.namesrvAddr");
		}
		AssertUtil.notBlank(namesrvAddr,"配置[mq.rocketmq.namesrvAddr]缺失");
	}

	@Override
	public void start() throws Exception{
		producer = new DefaultMQProducer(groupName);
		producer.setNamesrvAddr(namesrvAddr);
		producer.start();
	}
	
	@Override
	public String sendMessage(MQMessage message,boolean async) {
		Message _message = new Message(message.getTopic(), message.getTag(), message.getBizKey(), message.bodyAsBytes());
		//_message.setTransactionId(message.getTransactionId());
		try {
			if(async){
				producer.send(_message, new SendCallback() {
					@Override
					public void onSuccess(SendResult sendResult) {
						logger.debug("MQ_SEND_SUCCESS:{} -> msgId:{},status:{},offset:{}",message.getTopic(),sendResult.getMsgId(),sendResult.getSendStatus().name(),sendResult.getQueueOffset());
					}
					
					@Override
					public void onException(Throwable e) {
						logger.warn("MQ_SEND_FAIL:"+message.getTopic(),e);
					}
				});
			}else{
				producer.send(_message);	
			}
		} catch (Exception e) {
			logger.warn("MQ_SEND_FAIL:"+message.getTopic(),e);
		}
		
		return null;
	}

	@Override
	public void shutdown() {
		producer.shutdown();
	}

	

}
