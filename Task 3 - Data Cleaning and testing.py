# analysis.py
# ---------------------------------------------
# TASK 3 — Python (Data Cleaning + Statistics)
# ---------------------------------------------

import pandas as pd
import numpy as np
from scipy.stats import ttest_ind

# ---------------------------------------------
# 1. DATA CLEANING
# ---------------------------------------------

customers = pd.read_csv("customers.csv")
subscriptions = pd.read_csv("subscriptions.csv")

# Merge datasets on customer_id
df = customers.merge(subscriptions, on="customer_id", how="left")

print("Before cleaning:", df.shape)

# Handle missing values
# Replace missing plan_id with 'unknown'
df['plan_id'].fillna('unknown', inplace=True)

# Remove duplicate rows
df.drop_duplicates(inplace=True)

# Convert start_date to datetime format
# errors='coerce' will convert invalid dates to NaT
df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')

# Remove outliers from monthly_price_usd
# Keep only values below 99th percentile
df = df[df['monthly_price_usd'] < df['monthly_price_usd'].quantile(0.99)]

print("After cleaning:", df.shape)


# ---------------------------------------------
# 2. HYPOTHESIS TESTING
# ---------------------------------------------

# Hypothesis:
# H0: Feature usage has no effect on churn
# H1: Feature usage reduces churn

# Separate groups
active = df[df['feature_usage'] == 1]['churn']
inactive = df[df['feature_usage'] == 0]['churn']

# Perform independent t-test
stat, p = ttest_ind(active, inactive)

print("P-value:", p)

# Interpretation
if p < 0.05:
    print("Reject H0: Feature usage has a significant effect on churn")
else:
    print("Fail to reject H0: No significant effect found")


# ---------------------------------------------
# 3. CUSTOMER SEGMENTATION
# ---------------------------------------------

# Create LTV-based segments (Low, Medium, High)
df['segment'] = pd.qcut(df['ltv'], 3, labels=['Low', 'Medium', 'High'])

# Calculate average churn rate per segment
segmentation_result = df.groupby('segment')['churn'].mean()

print("\nChurn Rate by Segment:")
print(segmentation_result)

# ---------------------------------------------
# BUSINESS INSIGHT
# ---------------------------------------------
print("\nInsight:")
print("High LTV customers with high churn should be prioritized for retention strategies.")
