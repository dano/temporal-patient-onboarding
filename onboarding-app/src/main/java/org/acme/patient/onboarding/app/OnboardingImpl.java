package org.acme.patient.onboarding.app;

import io.quarkus.logging.Log;
import io.quarkus.temporal.runtime.annotations.TemporalActivityStub;
import io.quarkus.temporal.runtime.annotations.TemporalWorkflow;
import org.acme.patient.onboarding.model.Patient;

@TemporalWorkflow(name="test", queue = "OnboardingTaskQueue")
public class OnboardingImpl implements Onboarding {

    @TemporalActivityStub
    ServiceExecutor serviceExecutor;
    String status;
    Patient onboardingPatient;

    @Override
    public Patient onboardNewPatient(Patient patient) {
        onboardingPatient = patient;
//        Log.info("Starting to onboard a patient!");

        try {
            // 0. save the patient
            status = "Saving the patient to the db";
            serviceExecutor.savePatient(patient);
            // 1. assign hospital to patient
            status = "Assigning hospital to patient: " + onboardingPatient.getName();
            onboardingPatient.setHospital(
                    serviceExecutor.assignHospitalToPatient(onboardingPatient.getZip()));

            // 2. assign doctor to patient
            status = "Assigning doctor to patient: " + onboardingPatient.getName();
            onboardingPatient.setDoctor(
                    serviceExecutor.assignDoctorToPatient(onboardingPatient.getCondition()));

            // 3. notify patient with preferred contact method
            status = "Notifying patient: " + onboardingPatient.getName();
            switch (onboardingPatient.getContactMethod()) {
                case PHONE:
                    serviceExecutor.notifyViaEmail(onboardingPatient.getEmail());
                    break;

                case TEXT:
                    serviceExecutor.notifyViaText(onboardingPatient.getPhone());
                    break;
            }

            // 4. finalize onboarding
            status = "Finalizing onboarding for: " + onboardingPatient.getName();
            patient.setOnboarded(
                    serviceExecutor.finalizeOnboarding());

        } catch (Exception e) {
            patient.setOnboarded("no");
        }

//        Log.info("Done onboarding!");
        return onboardingPatient;
    }

    @Override
    public String getStatus() {
        return status;
    }

}
