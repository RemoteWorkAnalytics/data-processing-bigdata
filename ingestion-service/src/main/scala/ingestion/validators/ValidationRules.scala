package ingestion.validators

import ingestion.kafka.models.Employee
object ValidationRules {

  def isValid(employee: Employee): Boolean = {
    val genderValid = Set("Male", "Female", "Non-binary", "Prefer not to say").contains(employee.gender)
    val stressValid = Set("Low", "Medium", "High").contains(employee.stressLevel)
    val productivityValid = Set("Increase", "Decrease", "No Change").contains(employee.productivityChange)
    val workLifeBalanceValid = employee.workLifeBalanceRating.matches("[1-5]")
    val variantValid = employee.variantIndex >= 0
    val recordDateValid = employee.recordDate.nonEmpty

    employee.employeeId.nonEmpty &&
      employee.age > 0 &&
      employee.yearsOfExperience >= 0 &&
      employee.hoursWorkedPerWeek >= 0 &&
      genderValid &&
      stressValid &&
      productivityValid &&
      workLifeBalanceValid &&
      variantValid &&
      recordDateValid
  }

  def filterValid(employees: List[Employee]): List[Employee] = {
    employees.filter(isValid)
  }
}