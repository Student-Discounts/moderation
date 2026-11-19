package org.studentdiscounts.media;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(MediaProperties.class)
public class MediaApplication {
  public static void main(String[] args) {
    SpringApplication.run(MediaApplication.class, args);
  }
}
