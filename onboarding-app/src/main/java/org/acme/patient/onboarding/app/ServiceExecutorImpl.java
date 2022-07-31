package org.acme.patient.onboarding.app;

import io.quarkus.logging.Log;
import io.quarkus.temporal.runtime.annotations.TemporalActivity;
import io.temporal.activity.Activity;
import io.temporal.client.WorkflowClient;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;
import org.acme.patient.onboarding.model.Doctor;
import org.acme.patient.onboarding.model.Hospital;
import org.acme.patient.onboarding.model.Patient;
import org.acme.patient.onboarding.utils.Idempotent;
import org.acme.patient.onboarding.utils.OnboardingServiceClient;
import org.eclipse.microprofile.rest.client.inject.RestClient;

import javax.inject.Inject;

import static org.acme.patient.onboarding.utils.Activities.getCompletionClient;
import static org.acme.patient.onboarding.utils.Activities.runOnce;

@TemporalActivity(name="serviceExecutor")
public class ServiceExecutorImpl implements ServiceExecutor {

    private final OnboardingServiceClient serviceClient;
    private final PgPool client;
    private final WorkflowClient wc;

    @Inject
    public ServiceExecutorImpl(@RestClient OnboardingServiceClient serviceClient, PgPool client, WorkflowClient wc) {
        this.serviceClient = serviceClient;
        this.client = client;
        this.wc = wc;
    }

    public void savePatientOrig(Patient patient) {
        Log.info("saving the patient!");
        var ctx = Activity.getExecutionContext();
        ctx.doNotCompleteOnReturn();
        var token = ctx.getTaskToken();
        var completionClient = wc.newActivityCompletionClient();
        client.preparedQuery("INSERT INTO patients (name) VALUES ($1)").execute(Tuple.of(patient.getName()))
            .onFailure(err -> {
                Log.error("Insert failed:", err);
                completionClient.completeExceptionally(token, (Exception) err);
            })
            .onSuccess(ign -> completionClient.complete(token, null));
    }

    @Override
    public void savePatient(Patient patient) {
        Log.info("saving the patient!");
        var completionClient = getCompletionClient(wc);
        runOnce(client, c -> c.preparedQuery("INSERT INTO patients (name) VALUES ($1)").execute(Tuple.of(patient.getName())))
            .onFailure(err -> {
                Log.error("Insert failed:", err);
                completionClient.completeExceptionally((Exception) err);
            })
            .onSuccess(ign -> completionClient.complete(null));
    }

    @Override
    public Hospital assignHospitalToPatient(String zip) {
        // call onboarding service
        Hospital hospital = serviceClient.assignHospitalToPatient(zip);
        // simulate some work...
        sleep(5);
        return hospital;
    }

    @Override
    public Doctor assignDoctorToPatient(String condition) {
        Doctor doctor = serviceClient.assignDoctorToPatient(condition);
        // simulate some work...
        sleep(5);
        return doctor;
    }

    @Override
    public void notifyViaEmail(String email) {
        serviceClient.notifyPatient(email);
        // simulate some work...
        sleep(5);
    }

    @Override
    public void notifyViaText(String phone) {
        serviceClient.notifyPatient(phone);
        // simulate some work...
        sleep(5);
    }

    @Override
    public String finalizeOnboarding() {
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
