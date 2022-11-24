/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static org.apache.http.Consts.UTF_8;
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public final class GoogleLineageTransport extends Transport {

  private AtomicInteger atomicInteger = new AtomicInteger();

  private GoogleLineageTransport() {
    super(Type.GOOGLE);
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    int num = atomicInteger.incrementAndGet();
    String fileName = "lineage_tmp_" + num;
    final String eventAsJson = OpenLineageClientUtils.toJson(runEvent);
    log.info("Lineage Test POST {}: {}", fileName, eventAsJson);
    writeToFile(fileName, eventAsJson);
  }

  private void writeToFile(String fileName, String data){
    BufferedWriter output = null;
    try {
      String fn = "/tmp/" + fileName;
      File file = new File(fn);
      output = new BufferedWriter(new FileWriter(file));
      output.write(data);
    } catch ( IOException e ) {
      e.printStackTrace();
    } finally {
      if ( output != null ) {
        try {
          output.close();
        }catch (IOException e){
          System.out.println(e.getMessage());
          e.printStackTrace();
        }
      }
    }
  }

  public static GoogleLineageTransportBuilder builder() {
    return new GoogleLineageTransportBuilder();
  }

  public static final class GoogleLineageTransportBuilder {
    public GoogleLineageTransport build() {
      return new GoogleLineageTransport();
    }
  }
}
