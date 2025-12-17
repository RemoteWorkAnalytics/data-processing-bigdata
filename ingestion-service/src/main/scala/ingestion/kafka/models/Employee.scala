package ingestion.kafka.models

import play.api.libs.json.{Json, Writes, JsValue}

case class Employee(
                     employeeId: String,
                     age: Int,
                     gender: String,
                     jobRole: String,
                     industry: String,
                     yearsOfExperience: Int,
                     workLocation: String,
                     hoursWorkedPerWeek: Int,
                     numberOfVirtualMeetings: Int,
                     workLifeBalanceRating: String,
                     stressLevel: String,
                     mentalHealthCondition: String,
                     accessToMentalHealthResources: String,
                     productivityChange: String,
                     socialIsolationRating: String,
                     satisfactionWithRemoteWork: String,
                     companySupportForRemoteWork: String,
                     physicalActivity: String,
                     sleepQuality: String,
                     region: String,
                     variantIndex: Int,
                     recordDate: String,
                     generatedNote: String
                   )

object Employee {
  implicit val employeeWrites: Writes[Employee] = new Writes[Employee] {
    def writes(emp: Employee): JsValue = Json.obj(
      "employeeId" -> emp.employeeId,
      "age" -> emp.age,
      "gender" -> emp.gender,
      "jobRole" -> emp.jobRole,
      "industry" -> emp.industry,
      "yearsOfExperience" -> emp.yearsOfExperience,
      "workLocation" -> emp.workLocation,
      "hoursWorkedPerWeek" -> emp.hoursWorkedPerWeek,
      "numberOfVirtualMeetings" -> emp.numberOfVirtualMeetings,
      "workLifeBalanceRating" -> emp.workLifeBalanceRating,
      "stressLevel" -> emp.stressLevel,
      "mentalHealthCondition" -> emp.mentalHealthCondition,
      "accessToMentalHealthResources" -> emp.accessToMentalHealthResources,
      "productivityChange" -> emp.productivityChange,
      "socialIsolationRating" -> emp.socialIsolationRating,
      "satisfactionWithRemoteWork" -> emp.satisfactionWithRemoteWork,
      "companySupportForRemoteWork" -> emp.companySupportForRemoteWork,
      "physicalActivity" -> emp.physicalActivity,
      "sleepQuality" -> emp.sleepQuality,
      "region" -> emp.region,
      "variantIndex" -> emp.variantIndex,
      "recordDate" -> emp.recordDate,
      "generatedNote" -> emp.generatedNote
    )
  }
}
