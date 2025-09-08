#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
cd "$PROJECT_ROOT"

INPUT_DIR="${PROJECT_ROOT}/data/input"
OUTPUT_DIR="${PROJECT_ROOT}/data/output"

rm -rf "$OUTPUT_DIR"
mkdir -p "$INPUT_DIR"
if [ -z "$(ls -A "$INPUT_DIR" 2>/dev/null)" ]; then
  echo "No input files found in $INPUT_DIR. Please add your CSV(s) and rerun." >&2
  exit 1
fi

echo "[1/2] Building the project..."
mvn -q -DskipTests package | cat

MAIN_CLASS="org.example.touristflow.TouristFlowDriver"
JAR_FILE="${PROJECT_ROOT}/target/tourist-flow-1.0.0.jar"

echo "[2/2] Running the MapReduce job..."
if command -v hadoop >/dev/null 2>&1; then
  echo "Detected Hadoop CLI. Running with hadoop jar (local FS)."
  hadoop jar "$JAR_FILE" "$MAIN_CLASS" "file://${INPUT_DIR}" "file://${OUTPUT_DIR}"
else
  echo "Hadoop CLI not found. Running via mvn exec:java with LocalJobRunner."
  mvn -q exec:java -Dexec.mainClass="$MAIN_CLASS" -Dexec.args="${INPUT_DIR} ${OUTPUT_DIR}" | cat
fi

echo "\nJob completed. Output files: ${OUTPUT_DIR}"
echo "Preview of results:"
if [ -d "$OUTPUT_DIR" ]; then
  cat "${OUTPUT_DIR}/part-r-*" 2>/dev/null || true
fi

