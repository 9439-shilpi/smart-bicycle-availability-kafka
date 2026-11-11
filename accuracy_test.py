def classify_availability(bicycle_count):
    if bicycle_count == 0:
        return "EMPTY"
    elif 1 <= bicycle_count <= 5:
        return "LOW"
    else:
        return "NORMAL"


test_cases = {
    0: "EMPTY",
    1: "LOW",
    5: "LOW",
    6: "NORMAL",
    10: "NORMAL"
}

for count, expected in test_cases.items():
    actual = classify_availability(count)

    if actual == expected:
        print(f"PASS: {count} bicycles -> {actual}")
    else:
        print(f"FAIL: {count} bicycles -> {actual}, expected {expected}")