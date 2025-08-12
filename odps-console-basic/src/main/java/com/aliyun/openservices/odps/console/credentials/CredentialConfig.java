package com.aliyun.openservices.odps.console.credentials;

import java.util.Map;

import com.aliyun.credentials.exception.CredentialException;
import com.aliyun.credentials.models.Config;
import com.aliyun.tea.NameInMap;
import com.aliyun.tea.TeaModel;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class CredentialConfig extends Config {

  @NameInMap("processCommand")
  public String processCommand;
  @NameInMap("processCommandTimeout")
  public Long processCommandTimeout;

  public static CredentialConfig build(Map<String, ?> map) {
    CredentialConfig self = new CredentialConfig();

    try {
      return TeaModel.build(map, self);
    } catch (Exception e) {
      throw new CredentialException(e.getMessage(), e);
    }
  }
}
