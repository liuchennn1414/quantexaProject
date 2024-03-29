Quantexa Project 
---
Author: Lynn Liu 

## Project Structure
This assessment is written with both scala and spark. Each question is stored on its own as one class. 
The reason for us to adopt such a structure instead of one class for flightData and one class for passengers is for the convenience to review the main logic of each question, though a more OOP style structure could be preferred in real life context. 

```bash
quantexaProject
├── src
│   ├── main
│   │   ├── scala
│   │   │   ├── Main
│   │   │   ├── ReadCSV
│   │   │   ├── Question1
│   │   │   ├── Question2
│   │   │   ├── Question3
│   │   │   └── Question4
│   ├── resources
│   │   ├── flightData.csv
│   │   ├── passengers.csv
│   ├── output
│   │   ├── Q1.csv
│   │   ├── Q2.csv
│   │   ├── Q3.csv
│   │   ├── Q4.csv
│   ├── test
│   │   ├── scala
└── build.sbt
```

## How To Run 
### To view output csv files directly: 
The output csv files of the 4 questions (excluding the extra question) are available as csv files in the output directory. I have also store them separately in the submission folder for your convenience. 

### To run the program: 
There are 2 parts under Main that you can run: 
1. main function which returns the first 5 rows of each question (Q1-Q4)
2. additional function which accept input to show first 5 rows of the extra question from Q4 

To run the program in IntelliJ IDEA: 
1. Create new project from VCS and copy in the repository's URL to clone the project: 
```bash
https://github.com/liuchennn1414/quantexaProject.git
```

2. Click Build at the side bar to build the project (This is optional)

3. To run the main function and view the output of the first 4 questions, open sbt shell and run: 
```bash
runMain org.learnSpark.application.Main
```

4. To run the additional function: 

```bash
# Example input 1
runMain org.learnSpark.application.Main flownTogether 6 "2017-05-02" "2017-11-11"
```

```bash
# Example input 2 (no date input)
runMain org.learnSpark.application.Main flownTogether 6 
```
We can modify the example input to test out the function. 
As we can see, the dates are set to be optional. If no date is put, it will generate result for the full date range. However, an integer value is required to get the minimum threshold of count. More detail can be found in the main logic of Q4

## Unit Testing 
Although test cases are not written, here are some ideas of what we can do:
### Basic Test Cases for all questions / datasets
1. Check for duplicates 
2. Check for data type
3. Check for missing values 
4. Check for column names 
5. Check for extreme value (e.g. date range, count)

### Specific Test Cases for each question: 
###### Q1 
1. Check if the data are exactly 12 months (assuming only one year data available)
2. Extract out unique flightId within a particular month and check if the count match
###### Q2
1. Check if number of unique passengerId == 100
2. Extract out a particular passengerId and verify if the name is correct (to check for the join)
3. Extract out a particular passengerId and check if the number of unique flightIds match the count
###### Q3
1. Extract out a random passenger who did not visited uk, print out its to & from column and count the distinct number of countries this passenger has visited. 
2. Extract out a passenger who visited uk, and check if the longestRun logic is correct (i.e. longest run without visiting uk)
###### Q4
1. Check for any wrong record with count <= 3
2. Extract out the unique flightIds of 2 passengers, compare the 2 lists to find out if the number of matching flightIds match with the answer. 
###### Q4 - Extra 
1. Check if the date range in output does fall into the input date range 
2. Extract out the unique flightIds of 2 passengers within the range, compare the 2 lists to find out if the number of matching flightIds match with the answer.

### How to implement: 
1. Basic test cases / those checking for basic correctness like number of months / passengers can be implemented within each question class to ensure the output has the right format
2. We can also use ScalaTest framework such as FunSuite for unit testing 

### Others - Efficiency Test: 
1. Run the project on a larger set of data and evaluate the time & space complexity 





