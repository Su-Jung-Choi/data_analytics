//********************************************************************
//
// Author: Sujung Choi
// Course name: Data Analytics (CSC 735)
// Assignment: HW1
// Date of Creation: September 27, 2023
// Purpose: This program is to check whether or not a number is a valid credit card number from an input file (.txt)
//
//********************************************************************
// CreditCardValidation Object
//
// This singleton object contains methods--1) compute_value, 2) check_validity, 3) main--to validate credit card numbers based on Luhn's algorithm.
// The main function reads credit card numbers from a file and checks their validity using the algorithm.
//
object CreditCardValidation{
//********************************************************************
//
// Compute Value Function
//
// This function calculates the value of a character based on its position in a string.
// For characters located in index of even number, it doubles the character's integer value.
// If this doubling results in a two-digit number, it splits the two digits and sum them to get a signle-digit value.
// For characters located in index of odd number, it simply returns the character's integer value. 
//
// Return Value
// ------------
// Int                         Computed integer value based on the character and its poisition
//
// Value Parameters
// --------------------
// char      Char              Character to process
// index      Int              Position of the character in string
//
//*******************************************************************
def compute_value(char: Char, index: Int): Int = {
        if (index % 2 == 1){//take every second character from right to left
            //apply asDigit method to convert the character to its integer value
            val second_digit = char.asDigit
            //double every second digit
            val doubled = second_digit * 2
            
            //if doubling of a digit results in a two-digit number, seperate the two digits and add them up to get a single-digit number
            //(e.g. if a doubled number is 12, split them to 1 and 2. And add them up to be one; 1 + 2 = 3)
            if (doubled >= 10){
                (doubled % 10) + 1 //since doubling a single digit can results at most 18 (9*2), the first digit will always be 1
            }
            //if a doubled number is still a single-digit number, simply return the doubled number
            else{
                doubled
            }
        }
        else {//take every character in the odd places from right to left
            //apply asDigit method to convert the character to its integer value
            val first_digit = char.asDigit
            //return the digits from the odd number indices
            first_digit
        }  
}

//********************************************************************
//
// Check Validity Function
//
// This function determines the validity of given line based on Luhn's algorithm.
// The initial validation of the number is checked on two following criteria:
// 1) The number must have between 13 and 16 digits.
// 2) It must start with: 4 for Visa cards, 5 for MasterCard credit cards, 37 for American Express cards, 6 for Discover cards.
// By calling compute_value function, it processes each character of a line from right to left based on their position. 
// Then it sums up the computed values and if the total sum is a multiple of 10, it is considered valid, which return True.
// Otherwise, return False.
//
// Return Value
// ------------
// Boolean                      True if total sum is divisible by 10, otherwise False
//
// Value Parameters
// --------------------
// line      String            The line of characters to check its validity
//
//*******************************************************************
def check_validity(line: String): Boolean ={   
    import scala.collection.mutable.ArrayBuffer

    // initial validation for credit card number patterns
    // if the length of number is not between 13 and 16 digits or does not start with valid digits (i.e., 4, 5, 37, or 6),
    // return false
    if (line.length < 13 || line.length > 16 || 
    !(line.startsWith("4") || 
        line.startsWith("5") || 
        line.startsWith("37") || 
        line.startsWith("6"))){
        return false
    }    
    //create an array buffer to add every digit
    val digit_sum = ArrayBuffer[Int]()
    
    //iterate over each character in each line in reverse order (from right to left)
    //by using zipWithIndex method, it zips each character with its corresponding index
    for ((char,index) <- line.reverse.zipWithIndex){
        //call compute_value function to compute the value and add them into the array buffer
        digit_sum += compute_value(char, index)
    }
    //use the built-in sum function to sum up the results as a whole 
    val total_sum = digit_sum.sum
    if (total_sum % 10 == 0){
        true
    }
    else
        false
}

//********************************************************************
//
// Main Function
//
// This is the entry point of the program. The function reads a text file using scala.io.Source.
// Each line from the file is then processed using the check_validity function. 
// If a line is valid, it prints "valid". Otherwise, it prints "invalid".
//
// Return Value
// ------------
// Unit                         No value is to be returned (only prints the validity of a credit card number)
//
// Value Parameters
// --------------------
// args      Array[String]            Command line arguments
//
//*******************************************************************
def main(args: Array[String]): Unit = {

    import scala.io.Source

    //the numbers.txt file must be in the same directory as this program; otherwise, specify the path by changing the filePath variable below
    val filePath = "./numbers.txt"
    
    //using scala.io.Source to read the file
    val lines = Source.fromFile(filePath).getLines()
    
    //iterate over lines
    for (line <- lines){
      if (check_validity(line))
        println("valid")
      else
        println("invalid")
    }
    //close the source when it is done
    Source.fromFile(filePath).close()
}
}
