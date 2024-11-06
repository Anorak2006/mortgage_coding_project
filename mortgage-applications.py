### ASSIGNMENT DETAILS

# Student enrollment number: 42665
# Partner enrollment number: 42668

# Assignment 1 - 7313

### IMPORT LIBRARIES

import mysql.connector
from mysql.connector import Error
import pandas as pd
import json

### FUNCTIONS

# Connect to a database on a server
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try: 
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            passwd = user_password,
            database = db_name
        )
        print("MySQL Database connection successful.")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

# Read Data from SQL database
def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")

# Get column names (used for data imports)

def get_column_names(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    column_names = [column[0] for column in cursor.description]

    return column_names

# Amortization rate for requested loan
def compute_amortization_rate(property_valuation, requested_loan, gross_yearly_income):

    loan_to_value_ratio = requested_loan / property_valuation

    if gross_yearly_income == 0:
        loan_to_income_multiplier = 1000     #set to large amount if Error
    else:
        loan_to_income_multiplier = requested_loan / gross_yearly_income

    if loan_to_value_ratio <= 0.5:
        amortization_rate = 0.00
    elif loan_to_value_ratio > 0.5:
        amortization_rate = 0.01
    
    if loan_to_value_ratio > 0.7:
        amortization_rate += 0.01

    if loan_to_income_multiplier > 4.5:
        amortization_rate += 0.01

    return amortization_rate

# Child support
def compute_net_annual_child_costs(number_of_children):
    NUMBER_OF_MONTHS = 12
    MONTHLY_COST_PER_CHILD = 3700

    annual_child_costs = number_of_children * NUMBER_OF_MONTHS * MONTHLY_COST_PER_CHILD   

    if number_of_children == 0:
        monthly_child_support = 0
    elif number_of_children == 1:
        monthly_child_support = 1250
    elif number_of_children == 2:
        monthly_child_support = 2650
    elif number_of_children == 3:
        monthly_child_support = 4480
    elif number_of_children == 4:
        monthly_child_support = 6740
    elif number_of_children == 5:
        monthly_child_support = 9240
    elif number_of_children >= 6:
        monthly_child_support = 11740 + 1250 * (number_of_children - 6)
    
    annual_child_support = monthly_child_support * NUMBER_OF_MONTHS
    net_annual_child_costs = annual_child_support - annual_child_costs

    return net_annual_child_costs

# Net Annual Income
def compute_net_annual_income(gross_yearly_income, municipal_tax_rate):
    STATE_TAX_FREE_THRESHOLD = 598500
    STATE_TAX_RATE = 0.2
    gross_yearly_income = float(gross_yearly_income)
    municipal_tax_rate = float(municipal_tax_rate)

    if gross_yearly_income > STATE_TAX_FREE_THRESHOLD:
        state_income_tax = (gross_yearly_income - STATE_TAX_FREE_THRESHOLD) * STATE_TAX_RATE
    else:
        state_income_tax = 0

    municipality_income_tax = gross_yearly_income * (municipal_tax_rate / 100)

    net_annual_income = gross_yearly_income - state_income_tax - municipality_income_tax

    return net_annual_income

# Risk-adjusted interest expense on existing loans
def compute_risk_adjusted_interest_expense(interest_rate, loan_amount):
    MAXIMUM_ALLOWABLE_INTEREST_RATE = 0.2
    INTEREST_RATE_RISK_BUFFER = 0.03
    
    interest_rate = float(interest_rate)
    loan_amount = float(loan_amount)
    
    adjusted_interest_rate = min(MAXIMUM_ALLOWABLE_INTEREST_RATE, interest_rate + INTEREST_RATE_RISK_BUFFER)
    adjusted_interest_expense = loan_amount * adjusted_interest_rate

    return adjusted_interest_expense

# Compute annual interest on new loans
def additional_annual_interest_expense(new_loan_amount):
    new_loan_amount = float(new_loan_amount)
    INTEREST_RATE_NEW_LOANS = 0.07

    additional_annual_interest_expense = INTEREST_RATE_NEW_LOANS * new_loan_amount

    return additional_annual_interest_expense

# Compute housing costs
def annual_housing_costs(housing_type):
    ANNUAL_HOUSE_COSTS = 4700 * 12
    ANNUAL_APARTMENT_COSTS = 4200 * 12

    if housing_type == "house":
        annual_housing_costs = ANNUAL_HOUSE_COSTS
    elif housing_type == "apartment":
        annual_housing_costs = ANNUAL_APARTMENT_COSTS

    return annual_housing_costs

# Compute amortization on new loan (1st year)
def initial_amortization_amount(loan_amount, amortization_rate):
    loan_amount = float(loan_amount)
    amortization_rate = float(amortization_rate)

    initial_amortization_amount = loan_amount * amortization_rate

    return initial_amortization_amount

# Compute disposable income
def compute_disposable_income(annual_net_income, net_child_cost, interest_on_existing_loans, interest_on_new_loan, amortization_on_new_loan, housing_costs):
    ANNUAL_LIVING_COSTS = 10000 * 12
    disposable_income = annual_net_income - ANNUAL_LIVING_COSTS + net_child_cost - interest_on_existing_loans - interest_on_new_loan - amortization_on_new_loan - housing_costs
    
    return disposable_income

# Find maximum mortgage amount
def compute_maximum_mortgage(property_valuation, customer_data, housing_type, amortization_rate, tolerance = 100):
    MAXIMUM_LOAN_TO_VALUE_RATIO = 0.85
    maximum_loan_amount = MAXIMUM_LOAN_TO_VALUE_RATIO * property_valuation

    loan_amount = 0

    annual_housing_costs_value = annual_housing_costs(housing_type)
    annual_net_income = customer_data["net_annual_income"]
    net_child_cost = customer_data["net_annual_child_costs"]
    interest_on_existing_loans = customer_data["total_adjusted_interest_expense"]

    disposable_income_no_loan = compute_disposable_income(
        annual_net_income,
        net_child_cost,
        interest_on_existing_loans,
        interest_on_new_loan = 0,
        amortization_on_new_loan = 0,
        housing_costs = annual_housing_costs_value
    )

    if disposable_income_no_loan < 0:
        return 0
    
    low, high = 0, maximum_loan_amount              #changing from a linear to binary search algorithm reduced runtime by 94% (from 1min 40sec to 5sec), pretty cool)
    best_loan_amount = 0
    
    while high - low > tolerance:
        loan_amount = (low + high) / 2

        interest_on_new_loan = additional_annual_interest_expense(loan_amount)
        amortization_on_new_loan = initial_amortization_amount(loan_amount, amortization_rate)

        disposable_income_with_loan = compute_disposable_income(
        annual_net_income,
        net_child_cost,
        interest_on_existing_loans,
        interest_on_new_loan,
        amortization_on_new_loan,
        housing_costs = annual_housing_costs_value
    )
    
        if disposable_income_with_loan >= 0:
            best_loan_amount = loan_amount
            low = loan_amount + tolerance
        else:
            high = loan_amount - tolerance

    return round(best_loan_amount, 2)

### MAIN BODY

# READ AND STORE ALL TABLES AS LISTS OF DICTIONARIES

connection = create_db_connection("mysql-1.cda.hhs.se", "7313", "data", "MortgageApplications")

#Customers
read_customer_data = """
SELECT * FROM Customer;
"""
customer_column_names = ["customer_id", "firstname", "lastname", "zodiac_sign", "num_children", "municipality", "gross_yearly_income"]
customer_df = pd.DataFrame(read_query(connection, read_customer_data), columns = customer_column_names)
customers = customer_df.to_dict("records")

#Customer Loans
read_customer_loan_data = """
SELECT * FROM CustomerLoan;
"""
customer_loan_column_names = ["loan_id", "customer_id", "amount", "interest_rate", "lending_institute"]
customer_loan_df = pd.DataFrame(read_query(connection, read_customer_loan_data), columns = customer_loan_column_names)
customer_loans = customer_loan_df.to_dict("records")

#Loan Applications
read_loan_application_data = """
SELECT * FROM LoanApplication;
"""
loan_application_column_names = ["application_id", "property_valuation", "downpayment", "requested_loan", "housing_type", "customer_id"]
loan_application_df = pd.DataFrame(read_query(connection, read_loan_application_data), columns = loan_application_column_names)
loan_applications = loan_application_df.to_dict("records")

#2024 Tax Rates
read_tax_rate_data = """
SELECT * FROM TaxRate
WHERE tax_year = 2024;
"""
tax_rate_column_names = ("id", "tax_year", "municipality", "tax_rate")
tax_rate_df = pd.DataFrame(read_query(connection, read_tax_rate_data), columns = tax_rate_column_names)
tax_rates = tax_rate_df.to_dict("records")

# Determine required amortization rates for all loan applications

income_lookup_dictionary = {customer["customer_id"]: customer["gross_yearly_income"] for customer in customers}

for application in loan_applications:
    customer_id = application["customer_id"]
    gross_yearly_income = income_lookup_dictionary.get(customer_id)

    if gross_yearly_income is not None:
        amortization_rate = compute_amortization_rate(
            application["property_valuation"],
            application["requested_loan"],
            gross_yearly_income)
    
        application["amortization_rate"] = amortization_rate
    else:
        application["amortization_rate"] = None

# Compute net annual child costs for all customers

for customer in customers:
    customer["net_annual_child_costs"] = compute_net_annual_child_costs(customer["num_children"])

# Compute the net annual income for all customers

municipal_tax_lookup = {tax_rate["municipality"]: tax_rate["tax_rate"] for tax_rate in tax_rates}

for customer in customers:
    municipality = customer["municipality"]
    gross_yearly_income = customer["gross_yearly_income"]

    municipal_tax_rate = municipal_tax_lookup.get(municipality)

    annual_net_income = round(compute_net_annual_income(gross_yearly_income, municipal_tax_rate), 2)
    customer["net_annual_income"] = annual_net_income
    customer["annual_taxes_paid"] = gross_yearly_income - annual_net_income

# Compute the adjusted interest expense for all loans and aggregate across customers

for loan in customer_loans:
    loan["adjusted_interest_expense"] = round(compute_risk_adjusted_interest_expense(loan["interest_rate"],loan["amount"]),2)

interest_expense_by_customer ={}

for loan in customer_loans:
    customer_id = loan["customer_id"]
    adjusted_interest_expense = loan["adjusted_interest_expense"]

    if customer_id in interest_expense_by_customer:
        interest_expense_by_customer[customer_id] += adjusted_interest_expense
    else:
        interest_expense_by_customer[customer_id] = adjusted_interest_expense

for customer in customers:
    customer_id = customer["customer_id"]
    customer["total_adjusted_interest_expense"] = interest_expense_by_customer.get(customer_id, 0)

# Compute annual interest expense on new loans for all new loan applications

for loan_application in loan_applications:
    loan_application["additional_interest_expense"] = round(additional_annual_interest_expense(loan_application["requested_loan"]),2)

# Compute annual housing costs for all loan applications

for loan_application in loan_applications:
    loan_application["annual_housing_costs"] = annual_housing_costs(loan_application["housing_type"])

# Compute initial amortization on new loans

for loan_application in loan_applications:
    loan_application["initial_amortization_amount"] = initial_amortization_amount(loan_application["requested_loan"], loan_application["amortization_rate"])

# Compute disposable income for all loan applicants

customer_lookup = {customer["customer_id"]: customer for customer in customers}

for loan_application in loan_applications:
    customer_id = loan_application["customer_id"]
    customer_data = customer_lookup.get(customer_id)

    if customer_data:
        annual_net_income = customer_data["net_annual_income"]
        net_child_cost = customer_data["net_annual_child_costs"]
        interest_on_existing_loans = customer_data["total_adjusted_interest_expense"]
        interest_on_new_loan = loan_application["additional_interest_expense"]
        amortization_on_new_loan = loan_application["initial_amortization_amount"]
        housing_costs = loan_application["annual_housing_costs"]

        disposable_income = round(compute_disposable_income(
            annual_net_income,
            net_child_cost,
            interest_on_existing_loans,
            interest_on_new_loan,
            amortization_on_new_loan,
            housing_costs
        ), 0)

        loan_application["disposable_income"] = disposable_income
    else: 
        loan_application["disposable_income"] = None

# Compute maximum loan amount for each application

for loan_application in loan_applications:
    customer_id = loan_application["customer_id"]
    customer_data = customer_lookup.get(customer_id)

    if customer_data:
        maximum_mortgage = compute_maximum_mortgage(
            loan_application["property_valuation"],
            customer_data,
            loan_application["housing_type"],
            loan_application["amortization_rate"]

        )
        loan_application["maximum_mortgage"] = maximum_mortgage
        loan_application["maximum_loan_to_value"] = round(maximum_mortgage / loan_application["property_valuation"],2)
    else:
        loan_application["maximum_mortgage"] = 0

print(pd.DataFrame(loan_applications))

### EXPORT DATA IN JSON FORMAT

bank_output = []

for loan_application in loan_applications:
    customer_data = customer_lookup.get(loan_application["customer_id"], {})

    bank_output.append({
        "application_id": loan_application["application_id"], 
        "amortization_rate": float(loan_application.get("amortization_rate")),   
        "total_child_costs": int(customer_data.get("net_annual_child_costs")),  
        "annual_taxes": float(customer_data.get("net_annual_income")),   
        "existing_loans_expense": int(customer_data.get("total_adjusted_interest_expense")),  
        "disposable_income": int(loan_application.get("disposable_income")),   
        "maximum_loan": int(loan_application.get("maximum_mortgage"))  
    })

with open("assignment1_42665", "w") as json_file:
    json.dump(bank_output, json_file, indent = 2)

print("Data saved successfully.")
