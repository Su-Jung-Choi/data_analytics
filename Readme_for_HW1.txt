================================================
Sujung Choi
Data Analytics (CSC 735-Sec 1)- HW1
================================================
Overview: The programs saved in this file are designed to check whether or not a number is a valid credit card number based on Luhn's algorithm.
	 It will read an input file named numbers.txt and display the output either "valid" or "invalid" for the validity of given number in each line.
----------------------------------------------
File names and explanation of each file:
(a) HW1.scala
	It is a source code file that can be compiled and executed on the terminal (local version).
	It consists of a singleton object named CreditCardValidation.
	To run the code locally, follow the steps below:
	1. Go to the directory you saved the program (e.g., C:\Users\user).
	2. Save the numbers.txt file to be in the same directory if it is not already.
	2. Use the command prompt and type the following command to complie:  scalac HW1.scala
	3. To execute, type:  scala CreditCardValidation

(b) HW1_2.scala
	It is a source code file that can be run on Databricks (Databricks version).
	To run the code using Databricks, follow the steps below:
	1. Use 'import notebooks' on your workspace of Databricks Community Edition to upload this file.
	2. Use 'upload data to DBFS' to upload the numbers.txt file you have.
	3. In the main function, replace the file directory to your file directory after uploading data to DBFS.


