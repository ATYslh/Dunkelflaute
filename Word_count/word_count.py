def extract_first_number(line):
    """Extracts the first number before the first '+' in the line."""
    try:
        return int(line.split('+')[0])
    except (IndexError, ValueError):
        return 0

def should_ignore(line):
    """Checks if the line should be ignored based on keywords."""
    return '_top_' in line or 'Acknowledgements' in line

def sum_first_numbers(filename):
    total = 0
    with open(filename, 'r') as file:
        for line in file:
            if not should_ignore(line):
                total += extract_first_number(line.strip())
    return total

if __name__ == "__main__":
    filename = 'text.txt'
    result = sum_first_numbers(filename)
    print(f"Sum of first numbers (excluding _top_ and Acknowledgements): {result}")
