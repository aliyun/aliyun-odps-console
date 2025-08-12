package com.aliyun.openservices.odps.console.credentials;

import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.ALIYUN;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.CHAIN;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.DEFAULT;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.EXTERNAL;

import com.aliyun.credentials.models.CredentialModel;
import com.aliyun.credentials.provider.AlibabaCloudCredentialsProvider;
import com.aliyun.credentials.provider.DefaultCredentialsProvider;
import com.aliyun.credentials.provider.EcsRamRoleCredentialProvider;
import com.aliyun.credentials.provider.OIDCRoleArnCredentialProvider;
import com.aliyun.credentials.provider.RamRoleArnCredentialProvider;
import com.aliyun.credentials.provider.RsaKeyPairCredentialProvider;
import com.aliyun.credentials.provider.StaticCredentialsProvider;
import com.aliyun.credentials.provider.URLCredentialProvider;
import com.aliyun.credentials.utils.AuthConstant;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.tea.utils.Validate;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class AlibabaCloudCredentials {

  /**
   * odpscmd specific logic beside alibaba cloud credentials
   */
  public static AlibabaCloudCredentialsProvider getProvider(
      ExecutionContext context) throws ODPSConsoleException {
    CredentialConfig credentialConfig = context.getCredentialConfig();
    if (credentialConfig == null) {
      throw new ODPSConsoleException("Can't get credential information, please check whether your config file address is correct.");
    }
    credentialConfig.type = context.getAccountProvider();

    if (credentialConfig.type.equalsIgnoreCase(ALIYUN) || credentialConfig.type.equalsIgnoreCase(DEFAULT)) {
      credentialConfig.type = AuthConstant.ACCESS_KEY;
    }
    if (!StringUtils.isNullOrEmpty(context.getAccessId())) {
      credentialConfig.accessKeyId = context.getAccessId();
    }
    if (!StringUtils.isNullOrEmpty(context.getAccessKey())) {
      credentialConfig.accessKeySecret = context.getAccessKey();
    }
    if (!StringUtils.isNullOrEmpty(context.getStsToken())) {
      credentialConfig.securityToken = context.getStsToken();
    }
    if (credentialConfig.type.equals(CHAIN)) {
      return new DefaultCredentialsProvider();
    }
    if (credentialConfig.type.equals(EXTERNAL)) {
      return new ExternalCredentialsProvider(credentialConfig.processCommand,
                                             credentialConfig.processCommandTimeout);
    }
    return getProviderInternal(credentialConfig);
  }

  /**
   * Code copy from credentials-java, see
   * <a href="https://github.com/aliyun/credentials-java/blob/v0.3.12/src/main/java/com/aliyun/credentials/Client.java">Link</a>
   */
  private static AlibabaCloudCredentialsProvider getProviderInternal(
      com.aliyun.credentials.models.Config config) throws ODPSConsoleException {
    switch (config.type.toLowerCase()) {
      case AuthConstant.ACCESS_KEY:
        return StaticCredentialsProvider.builder()
            .credential(CredentialModel.builder()
                            .accessKeyId(Validate.notNull(
                                config.accessKeyId, "AccessKeyId must not be null."))
                            .accessKeySecret(Validate.notNull(
                                config.accessKeySecret, "AccessKeySecret must not be null."))
                            .type(config.type)
                            .build())
            .build();
      case AuthConstant.STS:
        return StaticCredentialsProvider.builder()
            .credential(CredentialModel.builder()
                            .accessKeyId(Validate.notNull(
                                config.accessKeyId, "AccessKeyId must not be null."))
                            .accessKeySecret(Validate.notNull(
                                config.accessKeySecret, "AccessKeySecret must not be null."))
                            .securityToken(Validate.notNull(
                                config.securityToken, "SecurityToken must not be null."))
                            .type(config.type)
                            .build())
            .build();
      case AuthConstant.BEARER:
        return StaticCredentialsProvider.builder()
            .credential(CredentialModel.builder()
                            .bearerToken(Validate.notNull(
                                config.bearerToken, "BearerToken must not be null."))
                            .type(config.type)
                            .build())
            .build();
      case AuthConstant.ECS_RAM_ROLE:
        return EcsRamRoleCredentialProvider.builder()
            .roleName(config.roleName)
            .disableIMDSv1(config.disableIMDSv1)
            .connectionTimeout(config.connectTimeout)
            .readTimeout(config.timeout)
            .build();
      case AuthConstant.RAM_ROLE_ARN:
        AlibabaCloudCredentialsProvider innerProvider;
        if (StringUtils.isEmpty(config.securityToken)) {
          innerProvider = StaticCredentialsProvider.builder()
              .credential(CredentialModel.builder()
                              .accessKeyId(Validate.notNull(
                                  config.accessKeyId, "AccessKeyId must not be null."))
                              .accessKeySecret(Validate.notNull(
                                  config.accessKeySecret, "AccessKeySecret must not be null."))
                              .type(AuthConstant.ACCESS_KEY)
                              .build())
              .build();
        } else {
          innerProvider = StaticCredentialsProvider.builder()
              .credential(CredentialModel.builder()
                              .accessKeyId(Validate.notNull(
                                  config.accessKeyId, "AccessKeyId must not be null."))
                              .accessKeySecret(Validate.notNull(
                                  config.accessKeySecret, "AccessKeySecret must not be null."))
                              .securityToken(Validate.notNull(
                                  config.securityToken, "SecurityToken must not be null."))
                              .type(AuthConstant.STS)
                              .build())
              .build();
        }
        return RamRoleArnCredentialProvider.builder()
            .credentialsProvider(innerProvider)
            .durationSeconds(config.roleSessionExpiration)
            .roleArn(config.roleArn)
            .roleSessionName(config.roleSessionName)
            .policy(config.policy)
            .STSEndpoint(config.STSEndpoint)
            .externalId(config.externalId)
            .connectionTimeout(config.connectTimeout)
            .readTimeout(config.timeout)
            .build();
      case AuthConstant.RSA_KEY_PAIR:
        return RsaKeyPairCredentialProvider.builder()
            .publicKeyId(config.publicKeyId)
            .privateKeyFile(config.privateKeyFile)
            .durationSeconds(config.roleSessionExpiration)
            .STSEndpoint(config.STSEndpoint)
            .connectionTimeout(config.connectTimeout)
            .readTimeout(config.timeout)
            .build();
      case AuthConstant.OIDC_ROLE_ARN:
        return OIDCRoleArnCredentialProvider.builder()
            .durationSeconds(config.roleSessionExpiration)
            .roleArn(config.roleArn)
            .roleSessionName(config.roleSessionName)
            .oidcProviderArn(config.oidcProviderArn)
            .oidcTokenFilePath(config.oidcTokenFilePath)
            .policy(config.policy)
            .STSEndpoint(config.STSEndpoint)
            .connectionTimeout(config.connectTimeout)
            .readTimeout(config.timeout)
            .build();
      case AuthConstant.CREDENTIALS_URI:
        return URLCredentialProvider.builder()
            .credentialsURI(config.credentialsURI)
            .connectionTimeout(config.connectTimeout)
            .readTimeout(config.timeout)
            .build();
      default:
        throw new ODPSConsoleException(ODPSConsoleConstants.UNSUPPORTED_ACCOUNT_PROVIDER);
    }
  }
}
