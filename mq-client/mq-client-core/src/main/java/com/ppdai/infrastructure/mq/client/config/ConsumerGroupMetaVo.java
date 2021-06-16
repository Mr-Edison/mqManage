package com.ppdai.infrastructure.mq.client.config;

public class ConsumerGroupMetaVo {

    /**
     * 消费组名
     */
    private String name;
    /**
     * 原消费组名
     * 如果不存在，则会赋值为 name
     */
	private String originName;

	public String getOriginName() {
		return originName;
	}

	public void setOriginName(String originName) {
		this.originName = originName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
