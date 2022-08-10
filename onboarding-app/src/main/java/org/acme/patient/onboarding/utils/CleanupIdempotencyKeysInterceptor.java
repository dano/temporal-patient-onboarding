package org.acme.patient.onboarding.utils;

import io.quarkus.logging.Log;
import io.temporal.workflow.Workflow;
import io.vertx.core.Future;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;

import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

/**
 * An interceptor designed to be used on asynchronous Activity methods, which return a {@link Future}. When one
 * of these methods is annotated with {@link AsyncMethod}, this interceptor will automatically indicate to
 * Temporal that the activity is asynchronous, and will also mark the activity as completed once the Future
 * returned by the annotated method completes.
 */
@Interceptor
@CleanupIdempotencyKeys
public class CleanupIdempotencyKeysInterceptor {

  @Inject
  PgPool client;

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
    var wfId = Workflow.getInfo().getWorkflowId();
    try {
      Log.info("Cleaning up idempotency keys");
      client.preparedQuery("DELETE FROM idempotency_keys where wf_id = $1").execute(Tuple.of(wfId));
      Log.info("Done cleaning up keys");
    } catch (Exception ex ) {
      Log.warn("Caught an error deleting idempotency keys", ex);
    }
    return out;
  }
}
