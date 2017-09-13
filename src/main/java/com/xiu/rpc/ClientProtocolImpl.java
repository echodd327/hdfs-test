package com.xiu.rpc;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolSignature;

public class ClientProtocolImpl implements ClientProtocol {

	@Override
	public ProtocolSignature getProtocolSignature(String arg0, long arg1,
			int arg2) throws IOException {
		return new ProtocolSignature(ClientProtocol.versionID, null);
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return ClientProtocol.versionID;
	}

	@Override
	public String echo(String value) throws IOException {
		return value;
	}

	@Override
	public int add(int v1, int v2) throws IOException {
		return v1+v2;
	}

}
