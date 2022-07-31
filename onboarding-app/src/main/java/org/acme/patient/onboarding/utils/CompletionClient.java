package org.acme.patient.onboarding.utils;

import io.temporal.client.ActivityCompletionClient;
import io.temporal.client.ActivityCompletionException;
import io.vertx.core.AsyncResult;

/**
 * Client used to asynchronous complete a Temporal Activity. This is just a thin wrapper over Temporal's
 * {@link ActivityCompletionClient}, that is meant to be a bit less verbose.
 */
public class CompletionClient {

  private final ActivityCompletionClient client;
  private final byte[] token;

  /**
   * Create an instance.
   *
   * @param client The Temporal completion client.
   * @param token The token used to complete this Activity.
   */
  public CompletionClient(ActivityCompletionClient client, byte[] token) {
    this.client = client;
    this.token = token;
  }

  /**
   * Complete the managed Activity.
   *
   * @param result The result to return
   * @param <R> The type of the result.
   * @throws ActivityCompletionException Thrown if completing fails
   */
  public <R> void complete(R result) throws ActivityCompletionException {
    client.complete(token, result);
  }

  /**
   * Complete the managed Activity exceptionally.
   *
   * @param result The exception to return
   * @throws ActivityCompletionException Thrown if completing fails
   */
  public void completeExceptionally(Throwable result) throws ActivityCompletionException {
    Exception exc = (result instanceof Exception)
        ? (Exception) result
        : new RuntimeException(result);
    client.completeExceptionally(token, exc);
  }

  /**
   * Report cancellation of the activity.
   *
   * @param details The cancellation details.
   * @param <V> The type of the details.
   * @throws ActivityCompletionException Thrown if reporting cancellation fails
   */
  public <V> void reportCancellation(V details) throws ActivityCompletionException {
    client.reportCancellation(token, details);

  }

  /**
   * Send a heartbeat for the managed activity
   *
   * @param details The details to include in the heartbeart.
   * @param <V> The type of the details.
   * @throws ActivityCompletionException Thrown if sending the heartbeat fails
   */
  public <V> void heartbeat(V details) throws ActivityCompletionException {
    client.heartbeat(token, details);
  }

  public <V> void handle(AsyncResult<V> result) {
    if (result.failed()) {
      completeExceptionally(result.cause());
    } else {
      complete(result.result());
    }
  }
}
