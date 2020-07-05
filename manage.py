import csv
import psycopg2
from xlwt import Workbook


try:
    conn = psycopg2.connect(dbname="IBRD", host="localhost",
                            user="postgres", password='1234', options="-c search_path=ibrd_ug")
    cur = conn.cursor()

except psycopg2.DatabaseError as err:
    print('Error %s' % err)

# data supplier is PI would name file with date and name as (YYYYMMDDD_ibrd.csv)


# Average for original_principal_amount

main_workbook = Workbook()


def generate_excel(headers_one, batch_one, main_workbook, sheet_name):
    sheet = main_workbook.add_sheet(sheet_name)

    # first set of logs
    row_index = 0  # start writing from the first row
    for col, header in enumerate(headers_one):
        sheet.write(row_index, col, header)

    row_index += 1
    for log in batch_one:
        for col_index, field in enumerate(log):
            sheet.write(row_index, col_index, field)
        row_index += 1


def get_averages_per_country():
    average_principal_per_ctry = """select TEMP.country_name, avg(TEMP.original_principal_amount) from 
        (select distinct on(LN.loan_number) C.country_name,  LD.original_principal_amount from 
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where 
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_principal_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_averages_per_country()
avg_header = ["Country", "Average Principal"]
generate_excel(avg_header, rows, main_workbook, "Averages_Per_Country")

# Average for cancelled_amount


def get_averages_cancelled_amount_per_country():
    average_cancelled_amount_per_ctry = """select TEMP.country_name, avg(TEMP.cancelled_amount) from 
        (select distinct on(LN.loan_number) C.country_name,  LD.cancelled_amount from 
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where 
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_cancelled_amount_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_averages_cancelled_amount_per_country()
avg_header = ["Country", "Average Cancelled"]
generate_excel(avg_header, rows, main_workbook, "Cancelled_Per_Country")

main_workbook.save("Summary.xls")

# Average for undisbursed_amount


def get_averages_undisbursed_amount_per_country():
    average_undisbursed_amount_per_ctry = """select TEMP.country_name, avg(TEMP.undisbursed_amount) from 
        (select distinct on(LN.loan_number) C.country_name,  LD.undisbursed_amount from 
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where 
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_undisbursed_amount_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_averages_undisbursed_amount_per_country()
avg_header = ["Country", "Undisbursed Amount"]
generate_excel(avg_header, rows, main_workbook, "undisbursed_amount")

main_workbook.save("Summary.xls")

# Average for disbursed_amount


def get_averages_disbursed_amount_per_country():
    average_disbursed_amount_per_ctry = """select TEMP.country_name, avg(TEMP.disbursed_amount) from 
        (select distinct on(LN.loan_number) C.country_name,  LD.disbursed_amount from 
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where 
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_disbursed_amount_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_averages_disbursed_amount_per_country()
avg_header = ["Country", "Disbursed Amount"]
generate_excel(avg_header, rows, main_workbook, "disbursed_amount")

main_workbook.save("Summary.xls")

# Average for repaid_to_ibrd


def get_averages_repaid_to_ibrd_per_country():
    average_repaid_to_ibrd_per_ctry = """select TEMP.country_name, avg(TEMP.repaid_to_ibrd) from 
        (select distinct on(LN.loan_number) C.country_name,  LD.repaid_to_ibrd from 
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where 
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_repaid_to_ibrd_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_averages_repaid_to_ibrd_per_country()
avg_header = ["Country", "Repaid To IBRD"]
generate_excel(avg_header, rows, main_workbook, "repaid_to_ibrd")

main_workbook.save("Summary.xls")

# Average for due_to_ibrd


def get_averages_due_to_ibrd_per_country():
    average_due_to_ibrd_per_ctry = """select TEMP.country_name, avg(TEMP.due_to_ibrd) from 
        (select distinct on(LN.loan_number) C.country_name,  LD.due_to_ibrd from 
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where 
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_due_to_ibrd_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_averages_due_to_ibrd_per_country()
avg_header = ["Country", "Due To IBRD"]
generate_excel(avg_header, rows, main_workbook, "due_to_ibrd")

main_workbook.save("Summary.xls")

# Average for exchange_adjustment


def get_averages_exchange_adjustment_per_country():
    average_exchange_adjustment_per_ctry = """select TEMP.country_name, avg(TEMP.exchange_adjustment) from 
        (select distinct on(LN.loan_number) C.country_name,  LD.exchange_adjustment from 
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where 
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_exchange_adjustment_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_averages_exchange_adjustment_per_country()
avg_header = ["Country", "Exchange Adjustments"]
generate_excel(avg_header, rows, main_workbook, "exchange_adjustment")

main_workbook.save("Summary.xls")

# Average for sold_3rd_party


def get_averages_sold_3rd_party_per_country():
    average_sold_3rd_party_per_ctry = """select TEMP.country_name, avg(TEMP.sold_3rd_party) from 
        (select distinct on(LN.loan_number) C.country_name,  LD.sold_3rd_party from 
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where 
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_sold_3rd_party_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_averages_sold_3rd_party_per_country()
avg_header = ["Country", "Sold 3rd Party"]
generate_excel(avg_header, rows, main_workbook, "sold_3rd_party")

main_workbook.save("Summary.xls")

# Average for repaid_3rd_party


def get_averages_repaid_3rd_party_per_country():
    average_repaid_3rd_party_per_ctry = """select TEMP.country_name, avg(TEMP.repaid_3rd_party) from 
        (select distinct on(LN.loan_number) C.country_name,  LD.repaid_3rd_party from 
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where 
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_repaid_3rd_party_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_averages_repaid_3rd_party_per_country()
avg_header = ["Country", "Repaid 3rd Party"]
generate_excel(avg_header, rows, main_workbook, "repaid_3rd_party")

main_workbook.save("Summary.xls")


# Average for due_3rd_party


def get_averages_due_3rd_party_per_country():
    average_due_3rd_party_per_ctry = """select TEMP.country_name, avg(TEMP.due_3rd_party) from 
        (select distinct on(LN.loan_number) C.country_name,  LD.due_3rd_party from 
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where 
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_due_3rd_party_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_averages_due_3rd_party_per_country()
avg_header = ["Country", "Due 3rd Party"]
generate_excel(avg_header, rows, main_workbook, "due_3rd_party")

main_workbook.save("Summary.xls")
# Average for loans_held


def get_averages_loans_held_per_country():
    average_loans_held_per_ctry = """select TEMP.country_name, avg(TEMP.loans_held) from 
        (select distinct on(LN.loan_number) C.country_name,  LD.loans_held from 
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where 
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_loans_held_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_averages_loans_held_per_country()
avg_header = ["Country", "Loans Held"]
generate_excel(avg_header, rows, main_workbook, "loans_held")

main_workbook.save("Summary.xls")

# Loans taked by each Country


def get_total_loan_per_ctry():
    total_loan_per_ctry = """select TEMP.country_name, count(TEMP.loan_number) from 
        (select distinct on(LN.loan_number) C.country_name, LN.loan_number from 
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN where 
        C.country_id=LN.fk_country_id) AS TEMP group by country_name"""

    cur.execute(total_loan_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_total_loan_per_ctry()
avg_header = ["Country", "Total Loans Per Country"]
generate_excel(avg_header, rows, main_workbook, "total_loans")

main_workbook.save("Summary.xls")
# Maximum amount taken by a country


def get_max_loan_per_ctry():
    max_loan_per_ctry = """select TEMP.country_name, max(TEMP.disbursed_amount) from 
        (select distinct on(LN.loan_number) C.country_name,  LD.disbursed_amount from 
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where 
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(max_loan_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_max_loan_per_ctry()
avg_header = ["Country", "Maximum Disbursed Amount"]
generate_excel(avg_header, rows, main_workbook, "maximum_disbursed_amount")

main_workbook.save("Summary.xls")

# Minimum amount taken by a country


def get_min_loan_per_ctry():
    min_loan_per_ctry = """select TEMP.country_name, min(TEMP.disbursed_amount) from 
        (select distinct on(LN.loan_number) C.country_name,  LD.disbursed_amount from 
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where 
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(min_loan_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_min_loan_per_ctry()
avg_header = ["Country", "Minimum Disbursed Amount"]
generate_excel(avg_header, rows, main_workbook, "minimum_disbursed_amount")

main_workbook.save("Summary.xls")


# All Loan types submitted in current raw file


def get_total_loan_types():
    total_loan_type = """select DISTINCT loan_type from ibrd_ug.loan"""

    cur.execute(total_loan_type)
    avg_rows = cur.fetchall()

    return avg_rows


rowstypes = get_total_loan_types()
avg_header = ["Loan Types", ""]
generate_excel(avg_header, rowstypes, main_workbook, "loan_types")

main_workbook.save("Summary.xls")

# Count for all Loan statuses in file


def get_total_loan_statuses():
    total_loan_statuses = """select derived_status, count(*) from (select distinct on(loan_number) loan_status AS derived_status, * from ibrd_ug.loan) AS TEMP
    group by TEMP.derived_status"""

    cur.execute(total_loan_statuses)
    avg_rows = cur.fetchall()

    return avg_rows


rowstatuses = get_total_loan_statuses()
avg_header = ["Loan Status", "Total Count"]
generate_excel(avg_header, rowstatuses, main_workbook, "loan_statuses")

main_workbook.save("Summary.xls")


# Missing Values: Count for loan numbers without guarantor


def get_loan_without_guarantor():
    loan_without_guarantor = """select count(*) from (select distinct on(LN.loan_number) loan_number from ibrd_ug.loan LN, ibrd_ug.guarantor G where LN.fk_guarantor_id=G.guarantor_id and guarantor = '') AS TEMP"""

    cur.execute(loan_without_guarantor)
    avg_rows = cur.fetchall()

    return avg_rows


rowsmissing = get_loan_without_guarantor()
avg_header = ["Total Count", ""]
generate_excel(avg_header, rowsmissing, main_workbook, "Missing Guarantor")

main_workbook.save("Summary.xls")


# Missing Values: Count for loan numbers without borrower name


def get_loan_without_borrower_name():
    loan_without_borrower_name = """select count(*) from (select distinct on(LN.loan_number) loan_number from ibrd_ug.loan LN, ibrd_ug.borrower B where LN.fk_borrower_id=B.borrower_id and borrower = '') AS TEMP"""

    cur.execute(loan_without_borrower_name)
    avg_rows = cur.fetchall()

    return avg_rows


rowsmissingb = get_loan_without_borrower_name()
avg_header = ["Total Count", ""]
generate_excel(avg_header, rowsmissingb, main_workbook, "Missing Borrower")

main_workbook.save("Summary.xls")
