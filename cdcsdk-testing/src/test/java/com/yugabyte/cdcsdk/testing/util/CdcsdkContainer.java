package com.yugabyte.cdcsdk.testing.util;

import org.testcontainers.containers.GenericContainer;

public class CdcsdkContainer {
  private String cdcsdkHostname = "127.0.0.1";
  private String cdcsdkMasterAddresses = "127.0.0.1:7100";
  private String cdcsdkStreamId;
  private String cdcsdkSinkType;
  
}
