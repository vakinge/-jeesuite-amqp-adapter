package com.jeesuite.amqp.cmq;

import com.jeesuite.amqp.MQMessage;
import com.jeesuite.amqp.MQProducer;
import com.jeesuite.common.JeesuiteBaseException;
import com.qcloud.cmq.CMQServerException;
import com.qcloud.cmq.Topic;
import com.qcloud.cmq.entity.CmqResponse;

/**
 * 
 * <br>
 * Class Name   : CMQProducer
 *
 * @author jiangwei
 * @version 1.0.0
 * @date 2019年4月23日
 */
public class CMQProducer implements MQProducer {

	@Override
	public void start() throws Exception {}

	@Override
	public String sendMessage(MQMessage message, boolean async) {
		try {
			if(CMQManager.isTopicMode()){
				Topic topic = CMQManager.createTopicIfAbsent(message.getTopic());
				return topic.publishMessage(message.toJSON());
			}else{
				CmqResponse response = CMQManager.getQueue().send(message.toJSON());
				return response.getMsgId();
			}
		} catch (CMQServerException e) {
			throw new JeesuiteBaseException(e.getMessage());
		} catch (Exception e) {
			throw new JeesuiteBaseException("cmq_error:" + e.getMessage());
		}
	}

	
	@Override
	public void shutdown() {}

}
