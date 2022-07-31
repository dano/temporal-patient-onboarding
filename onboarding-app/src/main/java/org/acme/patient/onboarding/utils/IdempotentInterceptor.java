package org.acme.patient.onboarding.utils;

import io.quarkus.logging.Log;
import io.temporal.activity.Activity;

import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

@Interceptor
@Idempotent
public class IdempotentInterceptor {

  @AroundInvoke
  public Object handleIdempotency(InvocationContext ic) throws Exception {
    Log.info("Fun test");
    var o = ic.proceed();
    Log.info("Fun test2");
    return o;
  }
}
