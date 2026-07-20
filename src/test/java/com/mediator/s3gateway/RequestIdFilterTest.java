package com.mediator.s3gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class RequestIdFilterTest {
  @Test
  void addsBothS3RequestIdentifiers() throws Exception {
    MockHttpServletRequest request=new MockHttpServletRequest("GET","/");
    MockHttpServletResponse response=new MockHttpServletResponse();

    new RequestIdFilter().doFilter(request,response,(req,res)->{});

    assertTrue(response.getHeader("x-amz-request-id").matches("[0-9a-f]{32}"));
    assertEquals(32,Base64.getDecoder().decode(response.getHeader("x-amz-id-2")).length);
    assertEquals(response.getHeader("x-amz-request-id"),request.getAttribute("requestId"));
    assertEquals(response.getHeader("x-amz-id-2"),request.getAttribute("extendedRequestId"));
  }
}
