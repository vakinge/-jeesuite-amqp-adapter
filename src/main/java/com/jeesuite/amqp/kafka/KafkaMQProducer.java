package com.jeesuite.amqp.kafka;

import com.jeesuite.amqp.MQMessage;
import com.jeesuite.amqp.MQProducer;

/**
 * 
 * <br>
 * Class Name   : KafkaMQProducer
 *
 * @author jiangwei
 * @version 1.0.0
 * @date 2019年3月25日
 */
public class KafkaMQProducer implements MQProducer {

	/* (non-Javadoc)
	 * @see com.zyframework.core.mq.MQProducer#start()
	 */
	@Override
	public void start() throws Exception {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see com.zyframework.core.mq.MQProducer#sendMessage(com.zyframework.core.mq.MQMessage, boolean)
	 */
	@Override
	public String sendMessage(MQMessage message, boolean async) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.zyframework.core.mq.MQProducer#shutdown()
	 */
	@Override
	public void shutdown() {
		// TODO Auto-generated method stub

	}

}
