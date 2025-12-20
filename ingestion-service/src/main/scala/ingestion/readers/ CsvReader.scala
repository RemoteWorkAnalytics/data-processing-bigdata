package ingestion.readers

import com.github.tototoshi.csv._
import java.io.File
import ingestion.kafka.models.Employee
import play.api.libs.json.Json

object CsvReader {
  def readEmployeesIterator(csvFilePath: String): Iterator[Employee] = {

    val reader = CSVReader.open(new File(csvFilePath))

    reader.iteratorWithHeaders.map { row =>
      Employee(
        employeeId = row("Employee_ID"),
        age = row("Age").toInt,
        industry = row("Industry"),
        yearsOfExperience = row("Years_of_Experience").toInt,
        workLocation = row("Work_Location"),
        hoursWorkedPerWeek = row("Hours_Worked_Per_Week").toInt,
        workLifeBalanceRating = row("Work_Life_Balance_Rating"),
        stressLevel = row("Stress_Level"),
        mentalHealthCondition = row("Mental_Health_Condition"),
        accessToMentalHealthResources = row("Access_to_Mental_Health_Resources"),
        productivityChange = row("Productivity_Change"),
        socialIsolationRating = row("Social_Isolation_Rating"),
        satisfactionWithRemoteWork = row("Satisfaction_with_Remote_Work"),
        companySupportForRemoteWork = row("Company_Support_for_Remote_Work"),
        physicalActivity = row("Physical_Activity"),
        sleepQuality = row("Sleep_Quality"),
        recordDate = row("Record_Date")
      )
    }
  }
}
