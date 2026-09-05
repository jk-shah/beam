/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.postgres.auth;

/**
 * Dynamic password provider for AWS RDS / Aurora PostgreSQL using IAM database authentication.
 *
 * <p>Generates short-lived (15-minute) SigV4 authentication tokens for connection pool recycling.
 */
public class AwsRdsIamPasswordProvider implements DynamicPasswordProvider {

  private final String hostname;
  private final int port;
  private final String dbUsername;
  private final String region;

  public AwsRdsIamPasswordProvider(String hostname, int port, String dbUsername, String region) {
    this.hostname = hostname;
    this.port = port;
    this.dbUsername = dbUsername;
    this.region = region;
  }

  public static AwsRdsIamPasswordProvider create(
      String hostname, int port, String dbUsername, String region) {
    return new AwsRdsIamPasswordProvider(hostname, port, dbUsername, region);
  }

  @Override
  public String getPassword() throws Exception {
    // In production AWS environments, invokes
    // RdsUtilities.builder().build().generateAuthenticationToken(...)
    // via reflection or AWS SDK v2 to avoid hard runtime dependency if AWS is not on the classpath.
    try {
      Class<?> rdsUtilsClass = Class.forName("software.amazon.awssdk.services.rds.RdsUtilities");
      Object rdsUtils = rdsUtilsClass.getMethod("builder").invoke(null);
      Object builtUtils = rdsUtils.getClass().getMethod("build").invoke(rdsUtils);

      Class<?> reqBuilderClass =
          Class.forName(
              "software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest$Builder");
      Class<?> reqClass =
          Class.forName(
              "software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest");

      Object reqBuilder = reqClass.getMethod("builder").invoke(null);
      reqBuilderClass.getMethod("hostname", String.class).invoke(reqBuilder, hostname);
      reqBuilderClass.getMethod("port", int.class).invoke(reqBuilder, port);
      reqBuilderClass.getMethod("username", String.class).invoke(reqBuilder, dbUsername);

      Class<?> regionClass = Class.forName("software.amazon.awssdk.regions.Region");
      Object regionObj = regionClass.getMethod("of", String.class).invoke(null, region);
      reqBuilderClass.getMethod("region", regionClass).invoke(reqBuilder, regionObj);

      Object request = reqBuilderClass.getMethod("build").invoke(reqBuilder);
      return (String)
          builtUtils
              .getClass()
              .getMethod("generateAuthenticationToken", reqClass)
              .invoke(builtUtils, request);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          "AWS SDK v2 (software.amazon.awssdk:rds) is required on classpath for AwsRdsIamPasswordProvider",
          e);
    }
  }
}
