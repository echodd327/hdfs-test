package com.xiu.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class ClientTest {
	
	public static void main(String[] args) throws IOException {
		ClientProtocol proxy = RPC.getProxy(ClientProtocol.class, ClientProtocol.versionID,
				new InetSocketAddress("127.0.0.1",8080), new Configuration());
		int total = proxy.add(1, 2);
		System.out.println(total);
	}
}
