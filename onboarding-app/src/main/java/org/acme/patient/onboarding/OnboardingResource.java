package org.acme.patient.onboarding;

import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.temporal.runtime.builder.WorkflowBuilder;
import io.temporal.api.workflowservice.v1.ListClosedWorkflowExecutionsRequest;
import io.temporal.api.workflowservice.v1.ListOpenWorkflowExecutionsRequest;
import io.temporal.api.workflowservice.v1.ListOpenWorkflowExecutionsResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;
import org.acme.patient.onboarding.app.Onboarding;
import org.acme.patient.onboarding.model.Patient;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.util.stream.Collectors;

@ApplicationScoped
@Path("/onboard")
@Tag(name = "New Patient Onboarding Endpoint")
public class OnboardingResource {

    @Inject
    WorkflowBuilder builder;

    @Inject
    PgPool pool;

    @Inject
    Vertx vertx;

    @Inject
    WorkflowClient client;

    void config (@Observes StartupEvent ev) {
        // Setup DB tables
        pool.query("CREATE TABLE IF NOT EXISTS patients (name TEXT PRIMARY KEY)").execute()
            .flatMap(ign -> pool.query("CREATE TABLE IF NOT EXISTS idempotency_keys (id TEXT PRIMARY KEY, wf_id TEXT)").execute())
            .result();

        // Do periodic cleanup of idempotency keys.
        vertx.setPeriodic(60_000L, v -> {
            Log.info("Doing a cleanup run");
            cleanupIdempotencyKeys();
        });

    }

    @POST
    public Patient doOnboard(Patient patient) {
        // start a new workflow execution
        // use the patient id for the unique id
        Onboarding workflow =
                builder.build(Onboarding.class, patient.getId());
        Log.info("About to call onboard workflow");
        return workflow.onboardNewPatient(patient);
    }

    @GET
    public String getStatus(@QueryParam("id") String patientId) {
        // query workflow to get the status message
        try {
            Onboarding workflow = builder.build(Onboarding.class, patientId);
            return workflow.getStatus();
        } catch (Exception e) {
//            e.printStackTrace();
            return "Unable to query workflow with id: " + patientId;
        }
    }

    private void cleanupIdempotencyKeys() {
        var stub = client.getWorkflowServiceStubs();
        ListOpenWorkflowExecutionsRequest req = ListOpenWorkflowExecutionsRequest.newBuilder()
            .setNamespace("default")
            .build();
        vertx.<ListOpenWorkflowExecutionsResponse>executeBlocking(p -> {
                var resp = stub.blockingStub().listOpenWorkflowExecutions(req);
                p.complete(resp);
            }).flatMap(resp -> {
                var executions = resp.getExecutionsList().stream()
                    .map(e -> "'" + e.getExecution().getWorkflowId() + "'")
                    .collect(Collectors.toList());
                Log.info("Open executions are: " + executions);
                var query = "DELETE FROM idempotency_keys";
                if (!executions.isEmpty()) {
                    query += " WHERE wf_id NOT IN (" + String.join(",", executions) + ")";
                }
                Log.info("Running cleanup query " + query);
                return pool.query(query).execute();
            })
            .onFailure(e -> Log.error("Failed to do cleanup ", e));
    }
}