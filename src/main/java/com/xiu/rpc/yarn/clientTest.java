package com.xiu.rpc.yarn;

import java.net.InetSocketAddress;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.Records;

public class clientTest {
	
	private ApplicationClientProtocol rmClient;

	public void test() throws Exception{
		/**
		 * 步骤1. client通过函数ApplicationClientProtocol#getNewApplication
		 * 从ResourceManager获取唯一的application ID
		 */
		this.rmClient = RPC.
				getProxy(ApplicationClientProtocol.class, 1L,
						new InetSocketAddress("127.0.0.1",8080), 
					new Configuration());
		GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
		GetNewApplicationResponse response = this.rmClient.getNewApplication(request);
		ApplicationId appId = response.getApplicationId();
		
		/**
		 * 步骤2，client通过RPC函数ApplicationProtocol#submitApplication将ApplicationMaster
		 * 提交的ResourceManager
		 */
		ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);
		appContext.setApplicationName(""); //设置应用程序名称
		//...设置应用程序其他属性，比如优先级，队列名称
		ContainerLaunchContext amcontainer = Records.newRecord(ContainerLaunchContext.class);
		//...设置AM相关变量
		amcontainer.setLocalResources(new HashMap<String,LocalResource>());  //设置AM启动所需本地资源
		amcontainer.setEnvironment(null); //设置AM所需环境变量
		appContext.setAMContainerSpec(amcontainer);
		appContext.setApplicationId(appId);
		
		SubmitApplicationRequest subRequest = Records.newRecord(SubmitApplicationRequest.class);
		subRequest.setApplicationSubmissionContext(appContext);
		this.rmClient.submitApplication(subRequest);
		
		/**
		 * 步骤3.还需要实现ApplicationProtocol接口实现
		 */
		
		//yarnClient提供了编程库
		
		
	}
	
	public static void main(String[] args) {
		
	}
}
