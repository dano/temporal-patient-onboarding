package org.acme.patient.onboarding.utils;

import io.quarkus.logging.Log;
import io.temporal.client.WorkflowClient;
import io.vertx.core.Future;

import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

import static org.acme.patient.onboarding.app.ServiceExecutorImpl.sleep;
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

  /**
   * Mark the intercepted activity method as asynchronous, create a CompletionClient for the activity,
   * and chain it to the Future returned by the activity method, so that it marks the activity as
   * complete when the Future emits success or failure.
   *
   * @param ic The invocation context.
   * @return Whatever was returned by the intercepted method.
   * @throws Exception thrown if the InvocationContext throws when its proceed method is called.
   */
  @AroundInvoke
  public Object handle(InvocationContext ic) throws Exception {
    var out = ic.proceed();
    if (out instanceof Future) {
      ((Future<?>) out)
          .onComplete(ign -> Log.info("Handling completion!"))
          .onComplete(getCompletionClient(wc)::handle);
    } else {
      Log.warnv("@AsyncMethod annotation used on a method ({0}) which does not return a Future", ic.getMethod().getName());
    }
    return out;
  }
}
