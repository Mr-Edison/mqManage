package com.ppdai.infrastructure.demo;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.ppdai.infrastructure.mq.biz.dto.base.MessageDto;
import com.ppdai.infrastructure.mq.biz.dto.base.ProducerDataDto;
import com.ppdai.infrastructure.mq.biz.event.ISubscriber;
import com.ppdai.infrastructure.mq.client.MqClient;
import com.ppdai.infrastructure.mq.client.exception.ContentExceed65535Exception;
import com.ppdai.infrastructure.mq.client.exception.MqNotInitException;

public class TestSub implements ISubscriber {
	@Override
	public List<Long> onMessageReceived(List<MessageDto> messages) {
		try {
		    System.out.println(messages.size());
		    if (true) {
		        return messages.stream().map(MessageDto::getId).collect(Collectors.toList());
            }
			MqClient.publish("test2",null, new ProducerDataDto(messages.get(0).getBody()));
		} catch (MqNotInitException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ContentExceed65535Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
