package org.apache.hadoop.ozone.s3;


import javax.enterprise.inject.Produces;

public class RegistryServiceProducer {
  @Produces
  RegistryService getRegistryService() {
    return RegistryService.getRegistryService();
  }
}
