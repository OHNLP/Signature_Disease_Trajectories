import pandas as pd

# Step 1: Load the CSV file
df = pd.read_csv("within_cohort_distribution.csv")  # Replace with your actual filename

# Step 2: Get distinct 'd1' values
distinct_d1 = df[['d1']].drop_duplicates()

# Step 3: Save to new CSV file
distinct_d1.to_csv("distinct_D1.csv", index=False)