# Python solutions

# Using regex
import re

def remove_special_chars_numbers_regex(input_string):
    """
    Removes all special characters and numbers from a given string using regex.
    
    Args:
        input_string (str): The input string to clean
        
    Returns:
        str: String with only alphabetic characters and spaces
    """
    # Pattern matches anything that is not a letter or space
    return re.sub(r'[^a-zA-Z\s]', '', input_string)


# Without regex
def remove_special_chars_numbers_no_regex(input_string):
    """
    Removes all special characters and numbers from a given string without using regex.
    
    Args:
        input_string (str): The input string to clean
        
    Returns:
        str: String with only alphabetic characters and spaces
    """
    result = ""
    for char in input_string:
        # Check if the character is a letter or space
        if char.isalpha() or char.isspace():
            result += char
    return result


# Example usage
if __name__ == "__main__":
    test_string = "Hello123! This is a #sample string with 456 special @characters."
    
    # Using regex
    cleaned_regex = remove_special_chars_numbers_regex(test_string)
    print(f"Cleaned with regex: {cleaned_regex}")
    
    # Without regex
    cleaned_no_regex = remove_special_chars_numbers_no_regex(test_string)
    print(f"Cleaned without regex: {cleaned_no_regex}")
