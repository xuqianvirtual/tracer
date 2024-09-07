package com.xuqianvirtual.tracer.common.util;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonUtil {

  private GsonUtil() {}

  public static final Gson GSON = new GsonBuilder()
      .disableHtmlEscaping()
      .enableComplexMapKeySerialization()
      .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
      .setDateFormat("yyyy-MM-dd HH:mm:ss")
      .create();
}
