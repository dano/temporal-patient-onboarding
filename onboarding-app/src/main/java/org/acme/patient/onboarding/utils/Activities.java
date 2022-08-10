package org.acme.patient.onboarding.utils;

import io.quarkus.logging.Log;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.client.WorkflowClient;
import io.vertx.core.Future;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;

import java.util.UUID;
import java.util.function.Function;

/**
 * This class contains helper methods for use in Activity implementations.
 */
public final class Activities {
  public static final String SELECT_IDEMPOTENCY_KEY = "SELECT * from idempotency_keys where id = ($1)";

  public static final String INSERT_IDEMPOTENCY_KEY = "INSERT INTO idempotency_keys (id, wf_id) VALUES ($1, $2)";

  /**
   * Execute a function that takes a SqlConnection managing an open a DB transaction, and returns a Future.
   * It is guaranteed that the Function will only be executed if it has not already been executed to the point
   * where the returned Future completed, and the DB transaction managed by the SqlConnection was committed.
   * <p>
   * This method is meant to be called inside a Temporal Activity.
   *
   * @param client The Reactive PG Client used to create the DB transaction.
   * @param runOnce The function to execute only once.
   * @return The Future returned by the runOnce or runBackup function.
   */
  public static Future<Void> runOnce(PgPool client, Function<SqlConnection, Future<?>> runOnce) {
    return runOnce(client, c -> runOnce.apply(c).mapEmpty(), ign -> Future.succeededFuture());
  }

  /**
   * Execute a function that takes a SqlConnection managing an open a DB transaction, and returns a Future.
   * It is guaranteed that the Function will only be executed if it has not already been executed to the point
   * where the returned Future completed, and the DB transaction managed by the SqlConnection was committed.
   * <p>
   * If it is determined that the given function already executed, a backup function will be called instead. This
   * can be used if it's necessary to use some fallback logic to get the data that would have been returned by
   * the work that you only wanted to run once.
   * <p>
   * This method is meant to be called inside a Temporal Activity.
   *
   * @param client The Reactive PG Client used to create the DB transaction.
   * @param runOnce The function to execute only once.
   * @param runBackup A function to execute if its determined that the runOnce function has already executed.
   * @return The Future returned by the runOnce or runBackup function.
   * @param <T> The type emitted by the returned Future.
   */
  public static <T> Future<T> runOnce(PgPool client, Function<SqlConnection, Future<T>> runOnce,
      Function<SqlConnection, Future<T>> runBackup) {
    var ctx = Activity.getExecutionContext();
    var uuid = getIdempotencyKey(ctx);
    Log.info("Starting transaction and checking idempotency");
    return client.withTransaction(c -> hasAlreadyRun(c, uuid)
        .flatMap(alreadyRun -> {
          if (!alreadyRun) {
            Log.info("Idempotency key not found. Code hasn't already run");
            return saveIdempotencyKey(ctx, uuid, c)
                .flatMap(ign -> runOnce.apply(c));
          } else {
            Log.info("Idempotency key found! Skipping work!");
            return runBackup.apply(c);
          }
        })
        .mapEmpty()
    );
  }

  /**
   * Generate or fetch the idempotency key for an Activity. An idempotency key is a UUID
   * that is generated once for an activity, then stored in the Workflow via a heartbeat call.
   * Prior to generation, a check will be made to determine if a key has already been generated
   * and stored. If one is found, that previously generated UUID will be returned.
   *
   * @param ctx The Activity's execution context.
   * @return The idempotency key for this Activity.
   * @deprecated Use {@link #getIdempotencyKey(ActivityExecutionContext)} instead.
   */
  @Deprecated
  public static String getIdempotencyKeyViaHeartBeat(ActivityExecutionContext ctx) {
    return ctx.getHeartbeatDetails(String.class)
        .orElseGet(() -> {
          var id = UUID.randomUUID().toString();
          Activity.getExecutionContext().heartbeat(id);
          return id;
        });
  }

  /**
   * Generate an Idempotency key.
   *
   * This implementation simply uses the ID of the current activity as the ID.
   *
   * @param ctx The Activity's execution context.
   * @return The idempotency key for this Activity.
   */
  public static String getIdempotencyKey(ActivityExecutionContext ctx) {
    return ctx.getInfo().getActivityId();
  }

  /**
   * Mark the activity this method is called from as asynchronous, and return a CompletionClient that
   * can be used to indicate the activity has completed.
   *
   * @param wc The WorkflowClient for this Activity.
   * @return A CompletionClient that can be used to complete the asynchronous activity.
   */
  public static CompletionClient getCompletionClient(WorkflowClient wc) {
    var ctx = Activity.getExecutionContext();
    ctx.doNotCompleteOnReturn();
    var token = ctx.getTaskToken();
    return new CompletionClient(wc.newActivityCompletionClient(), token);
  }

  /**
   * Determine if a given idempotency key has already been used.
   *
   * @param c A SqlConnection ot use to query the DB for the idempotency key.
   * @param idempotencyKey The idempotency key to check for.
   * @return A Future that will emit true if the idempotency key was already used to execute
   * some code, false if it hasn't.
   */
  private static Future<Boolean> hasAlreadyRun(SqlConnection c, String idempotencyKey) {
    return c.preparedQuery(SELECT_IDEMPOTENCY_KEY).execute(Tuple.of(idempotencyKey))
        .map(rows -> rows.size() > 0);
  }

  /**
   * Save the given idempotency key to the DB.
   *
   * @param ctx The activity execution context.
   * @param uuid The idempotency key to save.
   * @param c The SqlConnection to use to save the key to the db.
   * @return A Future that emits success when the key is saved.
   */
  private static Future<Void> saveIdempotencyKey(ActivityExecutionContext ctx, String uuid, SqlConnection c) {
    var wfId = ctx.getInfo().getWorkflowId();
    return c.preparedQuery(INSERT_IDEMPOTENCY_KEY).execute(Tuple.of(uuid, wfId)).mapEmpty();
  }

  private Activities() {
    // Prevent instantiation
  }
}
