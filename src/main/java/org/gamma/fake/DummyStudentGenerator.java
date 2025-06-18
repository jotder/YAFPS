package org.gamma.fake;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class DummyStudentGenerator {

    private static final String[] FIRST_NAMES = {"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy", "Kiran", "Liam", "Mia", "Noah", "Olivia"};
    private static final String[] LAST_NAMES = {"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Sharma", "Singh", "Das", "Roy", "Khan"};
    private static final String[] MAJORS = {"Computer Science", "Engineering", "Business", "Arts", "Science", "Math", "History", "Psychology", "Physics", "Chemistry", "Biology", "Economics"};
    private static final String[] STREET_NAMES = {"Park Ave", "Main St", "Oak Ln", "Maple Dr", "Pine St", "Garden Rd", "Church St", "Gandhi Marg", "Subhash Sarani", "Vivekananda Rd"};
    private static final String[] CITIES = {"Kolkata", "Delhi", "Mumbai", "Chennai", "Bengaluru", "South Dumdum", "Howrah", "Barasat", "Hooghly"};
    private static final String[] STATES = {"West Bengal", "Maharashtra", "Delhi", "Tamil Nadu", "Karnataka"};
    private static final String[] ZIP_CODES = {"700001", "700002", "700003", "700004", "700005", "700006", "700007"};


    /**
     * Generates a list of 1000 dummy student records with updated fields.
     *
     * @return A List of Student objects.
     */
    public static List<Student> generateDummyStudents(int numStudent) {
        List<Student> students = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < numStudent; i++) {
            String firstName = FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
            String lastName = LAST_NAMES[random.nextInt(LAST_NAMES.length)];
            int age = 18 + random.nextInt(5); // Age between 18 and 22
            String major = MAJORS[random.nextInt(MAJORS.length)];
            double gpa = 2.0 + (4.0 - 2.0) * random.nextDouble(); // GPA between 2.0 and 4.0
            int semester = 1 + random.nextInt(8); // Semester between 1 and 8 (assuming 4 years, 2 semesters/year)

            // Generate a dummy address
            String street = (100 + random.nextInt(900)) + " " + STREET_NAMES[random.nextInt(STREET_NAMES.length)];
            String city = CITIES[random.nextInt(CITIES.length)];
            String state = STATES[random.nextInt(STATES.length)];
            String zip = ZIP_CODES[random.nextInt(ZIP_CODES.length)];
            String address = street + ", " + city + ", " + state + " - " + zip;

            // Generate a dummy contact number (e.g., 10 digits starting with 9, 8, or 7 for Indian numbers)
            String contactNumber = String.format("%d%d%d%d%d%d%d%d%d%d",
                    (random.nextInt(3) + 7), // First digit: 7, 8, or 9
                    random.nextInt(10), random.nextInt(10), random.nextInt(10),
                    random.nextInt(10), random.nextInt(10), random.nextInt(10),
                    random.nextInt(10), random.nextInt(10), random.nextInt(10));


            students.add(new Student(firstName, lastName, age, major, gpa, semester, address, contactNumber));
        }
        return students;
    }

    /**
     * Escapes a string for CSV format.
     * Rules:
     * 1. If the field contains a comma (,), double quote ("), or newline character,
     * it must be enclosed in double quotes.
     * 2. If a field enclosed in double quotes contains a double quote,
     * that double quote must be escaped by preceding it with another double quote.
     * @param field The string to escape.
     * @return The escaped string.
     */
    private static String escapeCsvField(String field) {
        if (field == null) {
            return ""; // Treat null as empty string for CSV
        }

        // Check if quoting is necessary
        boolean needsQuotes = field.contains(",") || field.contains("\"") || field.contains("\n") || field.contains("\r");

        // Escape internal double quotes by doubling them
        String escapedField = field.replace("\"", "\"\"");

        if (needsQuotes) {
            return "\"" + escapedField + "\"";
        } else {
            return escapedField;
        }
    }

    /**
     * Writes a list of Student objects to a CSV file.
     * Handles proper CSV escaping for fields containing commas, double quotes, or newlines.
     *
     * @param students The list of Student objects to write.
     * @param filePath The path to the CSV file (e.g., "students.csv").
     * @throws IOException If an I/O error occurs during writing.
     */
    public static void toCSV(List<Student> students, Path filePath) throws IOException {
        // Use try-with-resources to ensure PrintWriter and FileWriter are closed automatically
        try (PrintWriter writer = new PrintWriter(new FileWriter(filePath.toFile()))) {
            // Write CSV Header
            writer.println(
                    "Id,FirstName,LastName,Age,Major,GPA,Semester,Address,ContactNumber"
            );

            // Write each student record
            for (Student student : students) {
                StringBuilder line = new StringBuilder();

                // Append each field, escaping as necessary
                line.append(escapeCsvField(String.valueOf(student.getId()))).append(",");
                line.append(escapeCsvField(student.getFirstName())).append(",");
                line.append(escapeCsvField(student.getLastName())).append(",");
                line.append(escapeCsvField(String.valueOf(student.getAge()))).append(",");
                line.append(escapeCsvField(student.getMajor())).append(",");
                line.append(escapeCsvField(String.format("%.2f", student.getGpa()))).append(","); // Format GPA
                line.append(escapeCsvField(String.valueOf(student.getSemester()))).append(",");
                line.append(escapeCsvField(student.getAddress())).append(",");
                line.append(escapeCsvField(student.getContactNumber())); // No comma after the last field

                writer.println(line.toString());
            }
            System.out.println("Successfully wrote " + students.size() + " records to " + filePath);
        } catch (IOException e) {
            System.err.println("Error writing to CSV file: " + e.getMessage());
            throw e; // Re-throw the exception after logging
        }
    }

    public static void generate(Path filePath) throws IOException {
        List<Student> dummyStudents = generateDummyStudents(1000);
        System.out.println("Generated " + dummyStudents.size() + " dummy student records with updated fields.");

        toCSV(dummyStudents,filePath);
//        // Optionally, print a few to verify
//        for (int i = 0; i < Math.min(10, dummyStudents.size()); i++) {
//            System.out.println(dummyStudents.get(i));
//        }
    }



     static class Student {
        private static final AtomicInteger count = new AtomicInteger(0);
        private int id; // Unique ID for the student
        private String firstName;
        private String lastName;
        private int age;
        private String major;
        private double gpa;
        private int semester; // New field: Current academic semester
        private String address; // New field: Student's address
        private String contactNumber; // New field: Student's contact number

        public Student(String firstName, String lastName, int age, String major, double gpa,
                       int semester, String address, String contactNumber) {
            this.id = count.incrementAndGet();
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
            this.major = major;
            this.gpa = gpa;
            this.semester = semester;
            this.address = address;
            this.contactNumber = contactNumber;
        }

        // Getters for all fields
        public int getId() {
            return id;
        }

        public String getFirstName() {
            return firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public int getAge() {
            return age;
        }

        public String getMajor() {
            return major;
        }

        public double getGpa() {
            return gpa;
        }

        public int getSemester() {
            return semester;
        }

        public String getAddress() {
            return address;
        }

        public String getContactNumber() {
            return contactNumber;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "id=" + id +
                    ", firstName='" + firstName + '\'' +
                    ", lastName='" + lastName + '\'' +
                    ", age=" + age +
                    ", major='" + major + '\'' +
                    ", gpa=" + String.format("%.2f", gpa) +
                    ", semester=" + semester +
                    ", address='" + address + '\'' +
                    ", contactNumber='" + contactNumber + '\'' +
                    '}';
        }
    }
}