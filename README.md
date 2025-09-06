## Tourist Flow MapReduce (Java + Hadoop)

Compute total tourist flows between countries from airline booking data using Hadoop MapReduce in Java.

### Dataset Format
- CSV with columns: `origin_country,destination_country,passengers`
- Example rows are provided in `data/sample_bookings.csv`. To use it, copy it (or any
  of your own CSV files) into `data/input/` before running.

### What Map and Reduce Do (Concept)
- **Map (FlowMapper)**: For each row, emits key as `origin->destination` and value as the passenger count for that booking record.
- **Reduce (FlowReducer)**: For each route key, sums all passenger counts to produce total tourist flow per origin-destination pair.

This captures directional flows (tourists traveling from origin to destination). If needed, you can adapt the key to a normalized pair (e.g., `min(origin,destination)-max(origin,destination)`) to represent undirected flows.

### Prerequisites
- Java 8+ (`java -version`)
- Maven 3.8+ (`mvn -v`)
- Optional: Hadoop (for `hadoop jar`), otherwise job runs locally via Maven.

### Project Structure
```
src/main/java/org/example/touristflow/
  FlowMapper.java
  FlowReducer.java
  TouristFlowDriver.java
data/
  sample_bookings.csv
scripts/
  run_local.sh
```

### Quick Start (Local Execution)
1) Prepare input files: place one or more CSV files in `data/input/`.
   For example:
```bash
mkdir -p data/input
cp data/sample_bookings.csv data/input/part-00000.csv
```

2) Build and run the job:
```bash
bash scripts/run_local.sh
```

3) Results will be written to `data/output/`.
```bash
cat data/output/part-r-*
```

### Run with Hadoop CLI (optional)
If you have Hadoop installed and configured:
```bash
mvn -q -DskipTests package
hadoop jar target/tourist-flow-1.0.0.jar \
  org.example.touristflow.TouristFlowDriver \
  file://$PWD/data/input \
  file://$PWD/data/output
```

### Taking Screenshots for Submission
- **Environment setup**: screenshot `java -version`, `mvn -v`, and if available `hadoop version`.
- **Dataset**: open `data/sample_bookings.csv` and capture.
- **Compile/Build**: screenshot Maven build success from terminal.
- **Execution**: screenshot running `bash scripts/run_local.sh`.
- **Results**: screenshot of `data/output/part-r-*` contents.
- Ensure your username is visible in the terminal prompt to show execution in your login.

### Notes
- Input CSV can contain headers; the mapper skips a header line automatically.
- Malformed or non-positive passenger counts are ignored gracefully.


