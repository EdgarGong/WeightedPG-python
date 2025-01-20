import re

def find_min_difference(filename):
    # Regular expression to match the line and capture the difference value
    # pattern = re.compile(r'MAX/MIN used capacity deviation: [\d\.]+ [\d\.]+ difference:([\d\.]+)')
    pattern = re.compile(r'difference:([\d\.]+)')

    min_difference = float('inf')
    min_line = ""
    
    with open(filename, 'r') as file:
        for line in file:
            match = pattern.search(line)
            if match:
                difference = float(match.group(1))
                if difference < min_difference:
                    min_difference = difference
                    min_line = line
    
    return min_line.strip(), min_difference

def find_topn_min_differences(filename, top_n=10):
    # Regular expression to match the line and capture the difference value
    pattern = re.compile(r'difference:([\d\.]+)')

    # List to store tuples of (difference, line)
    differences = []

    with open(filename, 'r') as file:
        for line in file:
            match = pattern.search(line)
            if match:
                difference = float(match.group(1))
                differences.append((difference, line))

    # Sort the list by the first element of each tuple (the difference)
    differences.sort(key=lambda x: x[0])

    # Get the top N lines with the smallest differences
    top_lines = differences[:top_n]

    # print(top_lines)

    # Extract the lines and differences into separate lists
    min_lines = [line.strip() for _, line in top_lines]
    min_differences = [diff for diff, _ in top_lines]

    return min_lines, min_differences

if __name__ == "__main__":
    filename = '8+2engine-crush.log'  # Replace with your actual file name
    min_line, min_difference = find_topn_min_differences(filename)
    print("Line with the smallest difference: ", min_line)
    print("Smallest difference: ", min_difference)
