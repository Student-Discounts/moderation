package org.studentdiscounts.moderation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(ModerationProperties.class)
public class ModerationApplication {
  public static void main(String[] args) {
    SpringApplication.run(ModerationApplication.class, args);
  }
}
