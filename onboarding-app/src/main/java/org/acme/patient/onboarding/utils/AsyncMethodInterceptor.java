package org.acme.patient.onboarding.utils;

import io.quarkus.logging.Log;
import io.temporal.client.WorkflowClient;
import io.vertx.core.Future;

import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

import static org.acme.patient.onboarding.utils.Activities.getCompletionClient;

/**
 * An interceptor designed to be used on asynchronous Activity methods, which return a {@link Future}. When one
 * of these methods is annotated with {@link AsyncMethod}, this interceptor will automatically indicate to
 * Temporal that the activity is asynchronous, and will also mark the activity as completed once the Future
 * returned by the annotated method completes.
 */
@Interceptor
@AsyncMethod
public class AsyncMethodInterceptor {

  @Inject
  WorkflowClient wc;

  @AroundInvoke
  public Object handle(InvocationContext ic) throws Exception {
    var out = ic.proceed();
    if (out instanceof Future) {
      ((Future<?>) out).onComplete(getCompletionClient(wc)::handle);
    } else {
      Log.warnv("@AsyncMethod annotation used on a method ({}) which does not return a Future", ic.getMethod().getName());
    }
    return out;
  }
}
