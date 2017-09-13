package com.xiu.rpc.yarn;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;

public class AMResourceManagerTest {

	public  void test() throws Exception {
		/**
		 * 步骤1.ApplicationMaster向ResourceManager注册
		 */
		ApplicationMasterProtocol rmClient = RPC.getProxy(ApplicationMasterProtocol.class, 1L, 
				new InetSocketAddress("127.0.0.1",8080), new Configuration());
		RecordFactory recordFactory = null;
		RegisterApplicationMasterRequest request = recordFactory
				.newRecordInstance(RegisterApplicationMasterRequest.class);
		request.setHost("");
		request.setRpcPort(8080);
		request.setTrackingUrl("");
		RegisterApplicationMasterResponse response = rmClient.registerApplicationMaster(request);
		
		/**
		 * 步骤2.ApplicationMaster向ResourceManager申请资源
		 */
		List<ResourceRequest> askList = null;
		List<ContainerId> releaseList = null;
		AllocateRequest allocateRequest = null;
		int ask = 0;
		int release = 0;
		while(true){ //维持与ResourceManager心跳
			askList = new ArrayList<ResourceRequest>(ask);
			releaseList = new ArrayList<ContainerId>(release);
			allocateRequest =  null;
			break;
		}
		AllocateResponse allocateResponse = rmClient.allocate(allocateRequest);
		
		
		/**
		 * 步骤3.ApplicationMaster通过ApplicationMasterProtocol#finishApplicationMaster告诉
		 * ResourceManager应用程序完毕，并退出
		 */
		FinishApplicationMasterRequest finishrequest = recordFactory
				.newRecordInstance(FinishApplicationMasterRequest.class);
		
		finishrequest.setDiagnostics(null);
		finishrequest.setTrackingUrl(null);
		rmClient.finishApplicationMaster(finishrequest);
	}

}
