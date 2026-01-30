package com.aliyun.openservices.odps.console.credentials;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.aliyun.credentials.exception.CredentialException;
import com.aliyun.credentials.models.CredentialModel;
import com.aliyun.credentials.provider.AlibabaCloudCredentialsProvider;
import com.google.gson.Gson;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ExternalCredentialsProvider implements AlibabaCloudCredentialsProvider {

  private CredentialModel cachedCredential;
  private String processCommand;
  private ZonedDateTime cachedCredentialExpiration;
  private long processCommandTimeout = 60;
  private static final long EXPIRATION_TIME_TOLERANCE = 5;

  public ExternalCredentialsProvider(String processCommand, Long processCommandTimeout) {
    this.processCommand = processCommand;
    if (processCommandTimeout != null && processCommandTimeout > 0) {
      this.processCommandTimeout = Math.min(processCommandTimeout, 600);
    }
  }

  /**
   * Sample:
   * {
   * "AccessKeyId": "STS.NTqDw5opx8xmAib8SCnuApuQR",
   * "AccessKeySecret": "*******************************",
   * "SecurityToken": "",
   * "Expiration": "2024-09-23T11:05:35Z"
   * }
   */
  private void loadCredential() {
    // clear cached credential
    cachedCredential = null;
    cachedCredentialExpiration = null;
    // fetch new credential and cache it
    String commandOutput = executeCommand(processCommand, processCommandTimeout);
    CredentialModel.Builder builder = CredentialModel.builder();
    Gson gson = new Gson();
    Map<String, Object> map;
    try {
      map = gson.fromJson(commandOutput, Map.class);
    } catch (Exception e) {
      throw new CredentialException("Illegal external command return value, requires JSON format, currently returns: \n"
                                    + commandOutput, e);
    }
    Object accessKeyId = map.get("AccessKeyId");
    Object accessKeySecret = map.get("AccessKeySecret");
    Object stsToken = map.get("SecurityToken");
    Object expiration = map.get("Expiration");

    if (accessKeyId == null || accessKeySecret == null) {
      throw new CredentialException("Illegal external command return value, requires JSON format, "
                                    + "including at least two fields 'AccessKeyId' and 'AccessKeySecret', currently returns \n"
                                    + commandOutput);
    }
    builder.accessKeyId(accessKeyId.toString());
    builder.accessKeySecret(accessKeySecret.toString());
    if (stsToken != null) {
      builder.securityToken(stsToken.toString());
    }
    if (expiration != null) {
      try {
        cachedCredentialExpiration = ZonedDateTime.parse(expiration.toString());
        if (cachedCredentialExpiration.isBefore(ZonedDateTime.now())) {
          throw new CredentialException(
              "Illegal external command return value, Expiration is before current time, command return " + expiration
              + " and now is " + ZonedDateTime.now());
        }
      } catch (DateTimeParseException e) {
        throw new CredentialException(
            "Illegal external command return value, requires 'Expiration' in ISO 8601 format, currently returns "
            + expiration, e);
      }
    }
    cachedCredential = builder.build();
  }

  private static String executeCommand(String command, long timeout) {
    String os = System.getProperty("os.name").toLowerCase();

    ProcessBuilder processBuilder;
    if (os.contains("win")) {
      // Windows 系统，使用 cmd.exe
      processBuilder = new ProcessBuilder("cmd.exe", "/c", command);
    } else {
      // Unix 系统，使用 bash
      processBuilder = new ProcessBuilder("bash", "-c", command);
    }
    try {
      Process process = processBuilder.start();
      // 捕获输出和错误流
      String output = readOutput(process);
      String error = readError(process);

      boolean isSuccess = process.waitFor(timeout, TimeUnit.SECONDS);
      if (!isSuccess) {
        throw new CredentialException(
            "Execute process command timed out after " + timeout + " seconds");
      }
      int exitCode = process.exitValue();
      // 检查执行结果
      if (exitCode != 0) {
        throw new CredentialException(
            "Execute process command failed with exit code " + exitCode + ": " + error);
      }
      // 输出正常结果
      return output;
    } catch (IOException e) {
      throw new CredentialException("Error when executing process command: " + e.getMessage(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CredentialException("User interrupted.", e);
    }
  }

  private static String readOutput(Process process) throws IOException {
    StringBuilder output = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        output.append(line).append("\n");
      }
    }
    return output.toString();
  }

  private static String readError(Process process) throws IOException {
    StringBuilder error = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(process.getErrorStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        error.append(line).append("\n");
      }
    }
    return error.toString();
  }

  @Override
  public CredentialModel getCredentials() {
    if (cachedCredential == null) {
      loadCredential();
    }
    Duration tolerance = Duration.ofSeconds(EXPIRATION_TIME_TOLERANCE);
    if (cachedCredential != null && cachedCredentialExpiration != null
        && cachedCredentialExpiration.minus(tolerance).isBefore(ZonedDateTime.now())) {
      loadCredential();
    }
    return cachedCredential;
  }

  @Override
  public String getProviderName() {
    return "external";
  }

  @Override
  public void close() throws Exception {

  }
}
