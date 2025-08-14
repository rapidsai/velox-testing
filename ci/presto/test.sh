#!/bin/bash

set -e
echo "Running Presto integration tests with pytest..."

# Check if integration tests directory exists
if [ -d "testing" ]; then
  echo "Setting up Python virtual environment for tests..."
  
  # Create virtual environment
  python3 -m venv test_venv
  
  # Activate virtual environment
  source test_venv/bin/activate
  
  echo "Installing Python test dependencies in virtual environment..."
  # Install pytest and other requirements
  pip install -r testing/requirements.txt
  
  echo "Running integration tests..."
  # Run pytest with verbose output (discovers all test files automatically)
  pytest testing -v
  
  # Store test result
  test_result=$?
  
  # Deactivate virtual environment
  deactivate
  
  # Clean up virtual environment
  rm -rf test_venv
  
  # Check test results
  if [ $test_result -eq 0 ]; then
    echo "Integration tests completed successfully"
  else
    echo "Integration tests failed"
    exit 1
  fi
else
  echo "Testing directory not found at testing/, exiting..."
  exit 1
fi


echo ""
echo "Presto testing completed successfully!"