package com.xuqianvirtual.tracer.common.util;

import java.util.Random;

public class RandomChineseNameGenerator {

  private static final int UNICODE_START = 0x4E00;
  private static final int UNICODE_END = 0x9FA5;

  private RandomChineseNameGenerator() {}

  // 生成一个随机中文字符
  public static char getRandomChineseChar() {
    Random random = new Random();
    // 生成一个位于指定范围内的随机Unicode码点
    int unicodeCodePoint = UNICODE_START + random.nextInt(UNICODE_END - UNICODE_START + 1);
    return (char) unicodeCodePoint;
  }

  // 生成一个指定长度的随机中文名（由随机中文字符组成）
  public static String generateRandomChineseName(int length) {
    StringBuilder name = new StringBuilder();
    for (int i = 0; i < length; i++) {
      name.append(getRandomChineseChar());
    }
    return name.toString();
  }
}
