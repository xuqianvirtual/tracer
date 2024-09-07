package com.xuqianvirtual.tracer.analysis.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class HelloTracerController {

  @RequestMapping("/hello")
  @ResponseBody
  public String hello() {
    return "Hello Tracer Analysis System.";
  }
}
