package ingestion.kafka.models

import play.api.libs.json.{Json, Writes, JsValue}

case class Employee(
                     employeeId: String,
                     age: Int,
                     industry: String,
                     yearsOfExperience: Int,
                     workLocation: String,
                     hoursWorkedPerWeek: Int,
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
                     recordDate: String,

                   )

object Employee {
  implicit val employeeWrites: Writes[Employee] = new Writes[Employee] {
    def writes(emp: Employee): JsValue = Json.obj(
      "employeeId" -> emp.employeeId,
      "age" -> emp.age,

      "industry" -> emp.industry,
      "yearsOfExperience" -> emp.yearsOfExperience,
      "workLocation" -> emp.workLocation,
      "hoursWorkedPerWeek" -> emp.hoursWorkedPerWeek,
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
    "recordDate" -> emp.recordDate,
    )
  }
}