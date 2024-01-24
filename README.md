Quantexa Assessment 
---
Author: Lynn Liu 

## Project structure
This assessment is written with both scala and spark. Each question is stored on its own as one class. 
The reason for us to adapt such a structure instead of one class for flightData and one class for passengers is for the convenience to review the main logic of each question, though a more OOP style structure could be preferred in real life context. 

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
│   └── test
│       └── scala
└── build.sbt
```

## How to run 
#### To view output: 
The output of the 4 questions (excluding the extra question) are available as csv files in the output directory. 

#### To run the program: 
There are 2 parts under Main that you can run: 
1. main function which returns the first 5 rows of each question (Q1-Q4)
2. additional function which accept input to show first 5 rows of the extra question from Q4 

To run the program: 
1. git clone this repository with: 
```bash
git clone https://github.com/liuchennn1414/quantexaProject.git
cd quantexaProject
```
2. To run the main function: 
```bash
git clone https://github.com/liuchennn1414/quantexaProject.git
cd quantexaProject
```
3. To run the additional function: 
```bash
# example input 
git clone https://github.com/liuchennn1414/quantexaProject.git
cd quantexaProject
```
You can modify the example input to test out the function. 
Take note that the dates are optional. If no date is put, it will generate result for the full date range. However, an integer value is required to get the minimum threshold of count. 

## Remark 
Although test cases are not written, here are some ideas of what we can do:
#### Basic Test Cases for all questions / datasets
1. Check for duplicates 
2. Check for data type
3. Check for missing values 
4. Check for column names 
5. Check for extreme value (e.g. date range, count)

#### Specific Test Cases for each question: 
###### Q1 
1. Check if the data are exactly 12 months (assuming only one year data available)
2. Extract out unique flightId within a particular month and check if the count match
###### Q2
1. Extract out unique passengerId and check if the number of unique flightIds match the count
###### Q3
1. Extract out a random passenger, print out its to & from column and make a manual comparison with the actual count. 
###### Q4
1. Extract out the unique flightIds of 2 passengers, iterate over to find if the number of matching flightIds match with the answer. 




