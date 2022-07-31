package org.acme.patient.onboarding.app;

import io.quarkus.logging.Log;
import io.quarkus.temporal.runtime.annotations.TemporalActivity;
import io.temporal.activity.Activity;
import io.temporal.client.WorkflowClient;
import io.vertx.core.Future;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.*;
import org.acme.patient.onboarding.model.Doctor;
import org.acme.patient.onboarding.model.Hospital;
import org.acme.patient.onboarding.model.Patient;
import org.acme.patient.onboarding.utils.AsyncMethod;
import org.acme.patient.onboarding.utils.OnboardingServiceClient;
import org.eclipse.microprofile.rest.client.inject.RestClient;

import javax.inject.Inject;

import java.util.UUID;

import static org.acme.patient.onboarding.utils.Activities.*;

@TemporalActivity(name="serviceExecutor")
public class ServiceExecutorImpl implements ServiceExecutor {

    public static final String PATIENT_INSERT = "INSERT INTO patients (name) VALUES ($1)";
    private final OnboardingServiceClient serviceClient;
    private final PgPool client;
    private final WorkflowClient wc;

    @Inject
    public ServiceExecutorImpl(@RestClient OnboardingServiceClient serviceClient, PgPool client, WorkflowClient wc) {
        this.serviceClient = serviceClient;
        this.client = client;
        this.wc = wc;
    }

    /**
     * Here's a basic version of the save patient activity. We need to indicate that the method is async,
     * Get a Task token to use to complete the method asynchronously, and then explicitly complete it
     * when we're done.
     *
     * This version is also not idempotent at all - if we crash after doing the insert, but before we can tell
     * Temporal that we finished, this method will get called again on restart, and fail.
     *
     * @param patient The patient to save.
     */
    public void savePatientBasic(Patient patient) {
        Log.info("saving the patient!");
        var ctx = Activity.getExecutionContext();
        ctx.doNotCompleteOnReturn();
        var token = ctx.getTaskToken();
        var completionClient = wc.newActivityCompletionClient();
        doSavePatient(client, patient)
            .onSuccess(igin -> sleep(9))
            .onFailure(err -> completionClient.completeExceptionally(token, (Exception) err))
            .onSuccess(ign -> completionClient.complete(token, null));
    }

    /**
     * This adds idempotency to the basic version. It is a bit contrived, because probably no one would try to
     * do all this work in a single method, but it gives you an idea of how much extra "stuff" is required
     * besides doing the actual thing you care about: saving the patient pojo to the db.
     * @param patient
     */
    public void savePatientIdempotent(Patient patient) {
        Log.info("saving the patient!");
        var ctx = Activity.getExecutionContext();
        // Setup for asynchronous execution
        ctx.doNotCompleteOnReturn();
        var token = ctx.getTaskToken();
        var completionClient = wc.newActivityCompletionClient();

        // Set up for idempotent execution
        var idempotencyKey = ctx.getHeartbeatDetails(String.class)
            .orElseGet(() -> {
                var id = UUID.randomUUID().toString();
                Activity.getExecutionContext().heartbeat(id);
                return id;
            });
        client.withTransaction(c -> c.preparedQuery(SELECT_IDEMPOTENCY_KEY).execute(Tuple.of(idempotencyKey))
            .map(rows -> rows.size() > 0)
            .flatMap(alreadyRun -> {
                if (!alreadyRun) {
                    Log.info("Idempotency key not found. Code hasn't already run");
                    var wfId = ctx.getInfo().getWorkflowId();
                    return c.preparedQuery(INSERT_IDEMPOTENCY_KEY).execute(Tuple.of(idempotencyKey, wfId))
                        .flatMap(ign -> doSavePatient(c, patient));
                } else {
                    Log.info("Idempotency key found! Skipping work!");
                    return Future.succeededFuture();
                }
            })
        )
            .onSuccess(igin -> sleep(9))
            .onFailure(err -> completionClient.completeExceptionally(token, (Exception) err))
            .onSuccess(ign -> completionClient.complete(token, null));
    }

    /**
     * This implementation of savePatient uses two helpers: one which handles executing some SQL
     * in an idempotent way, and another which both registers the activity as asynchronous,
     * and completes the activity when the asynchronous work is done.
     *
     * @param patient The patient to save
     */
    public void savePatientWithHelpers(Patient patient) {
        Log.info("saving the patient!");
        runOnce(client, c -> doSavePatient(c, patient))
            .onComplete(getCompletionClient(wc)::handle);
    }

    /**
     * This implementation of savePatient is similar to the one above that uses helpers, but
     * instead of using a helper method to handle the async business, it uses a Quarkus interceptor.
     *
     * @param patient The patient to save
     */
    public void savePatient(Patient patient) {
        Log.info("saving the patient!");
        asyncSave(patient);
    }

    @AsyncMethod
    public Future<Void> asyncSave(Patient patient) {
        return runOnce(client, c -> doSavePatient(c, patient));
    }

    private Future<RowSet<Row>> doSavePatient(SqlClient c, Patient patient) {
        return c.preparedQuery(PATIENT_INSERT).execute(Tuple.of(patient.getName()));
    }

    @Override
    public Hospital assignHospitalToPatient(String zip) {
        Log.info("Assigning hospital to patient");
        // call onboarding service
        Hospital hospital = serviceClient.assignHospitalToPatient(zip);
        // simulate some work...
        sleep(5);
        return hospital;
    }

    @Override
    public Doctor assignDoctorToPatient(String condition) {
        Log.info("Assigning doctor to patient");
        Doctor doctor = serviceClient.assignDoctorToPatient(condition);
        // simulate some work...
        sleep(5);
        return doctor;
    }

    @Override
    public void notifyViaEmail(String email) {
        Log.info("notifying via email");
        serviceClient.notifyPatient(email);
        // simulate some work...
        sleep(5);
    }

    @Override
    public void notifyViaText(String phone) {
        Log.info("notifying via text");
        serviceClient.notifyPatient(phone);
        // simulate some work...
        sleep(5);
    }

    @Override
    public String finalizeOnboarding() {
        Log.info("Finalizing");
        // simulate some work...
        sleep(5);
        return "yes";
    }

    private void sleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException ee) {
            // Empty
        }
    }
}
