import csv
import psycopg2
from xlwt import Workbook
import xlsxwriter
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from datetime import datetime


try:
    conn = psycopg2.connect(dbname="IBRD", host="localhost",
                            user="postgres", password='1234', options="-c search_path=ibrd_ug")
    cur = conn.cursor()

except psycopg2.DatabaseError as err:
    print('Error %s' % err)

# data supplier is PI would name file with date and name as (YYYYMMDDD_ibrd.csv)
csv_filename = 'ibrd-statement-of-loans-latest-available-snapshot.csv'
cur.execute(
    "insert into data_loading_log(file_name, time_started) values ('%s', now())" % csv_filename)
conn.commit()
# get most recent log id
cur.execute("select max(log_id) from data_loading_log")
result = cur.fetchone()
log_id = result[0]
reader = csv.reader(open(csv_filename, 'r'), delimiter=',')
num_of_rows = 0
for index, row in enumerate(reader):
    if index == 0:
        continue
# Initialize Rows
    region_name = row[2].replace("'", "")
    country_code = row[3]
    country_name = row[4].replace("'", "")
    borrower = row[5].replace("'", "")
    guarantor_country_code = row[6]
    guarantor_name = row[7].replace("'", "")
    loan_number = row[1]
    loan_type = row[8]
    loan_status = row[9]
    end_of_period = row[0]
    interest_rate = row[10]
    currencyof_commitment = row[11]
    project_id = row[12]
    project_name = row[13].replace("'", "")
    original_principal_amount = row[14]
    cancelled_amount = row[15]
    undisbursed_amount = row[16]
    disbursed_amount = row[17]
    repaid_to_ibrd = row[18]
    due_to_ibrd = row[19]
    exchange_adjustment = row[20]
    borrowers_obligation = [21]
    sold_3rd_party = row[22]
    repaid_3rd_party = row[23]
    due_3rd_party = row[24]
    loans_held = row[25]
    first_repayment_date = row[26]
    last_repayment_date = row[27]
    agreement_signing_date = row[28]
    board_approval_date = row[29]
    effective_date_most_recent = row[30]
    closed_date_most_recent = row[31]
    last_disbursement_date = row[32]

# DISTRIBUTE DATA TO ALL TABLES

# REGION
    try:
        get_region = "Select region_id from region where region_name ilike '%s'" % region_name
        cur.execute(get_region)
        result = cur.fetchone()

        if result:
            region_id = result[0]
        else:  # region doesnt exist yet, create it
            insert_region = "INSERT INTO region (region_name) VALUES ('%s')" % region_name
            cur.execute(insert_region)
            conn.commit()

            # fetch after insert
            cur.execute(get_region)
            result = cur.fetchone()
            region_id = result[0]
            print("Created Region ID: ", region_id)

    except Exception as err:
        print("Region Error Occurred: ", err)

    # COUNTRY
    try:
        get_ctry = "select country_id from country where country_name ilike '%s'" % country_name
        cur.execute(get_ctry)
        result = cur.fetchone()

        if result:
            ctry_id = result[0]
        else:  # country doesnt exist yet, create it
            insert_country = "INSERT INTO country (country_name, country_code, fk_region_id) VALUES ('%s', '%s', %d)" % (
                country_name, country_code, region_id)
            cur.execute(insert_country)
            conn.commit()

            # fetch after insert
            cur.execute(get_ctry)
            result = cur.fetchone()
            ctry_id = result[0]
            print("Created Country ID: ", ctry_id)

    except Exception as err:
        print("Country Error Occurred: ", err)

    # BORROWER
    try:
        get_borrower = "select borrower_id from borrower where borrower ilike '%s'" % borrower
        cur.execute(get_borrower)
        result = cur.fetchone()

        if result:
            borrower_id = result[0]
        else:  # borrower doesnt exist yet, create it
            insert_borrower = "INSERT INTO borrower (borrower, borrowers_obligation) VALUES ('%s', '%s')" % (
                borrower, borrowers_obligation)
            cur.execute(insert_borrower)
            conn.commit()

            # fetch after insert
            cur.execute(get_borrower)
            result = cur.fetchone()
            borrower_id = result[0]
            print("Created Borrower ID: ", borrower_id)

    except Exception as err:
        print("Borrower Error Occurred: ", err)

    # GUARANTOR
    try:
        get_guarantor = "select guarantor_id from guarantor where guarantor ilike '%s'" % guarantor_name
        cur.execute(get_guarantor)
        result = cur.fetchone()

        if result:
            guarantor_id = result[0]
        else:  # guarantor doesnt exist yet, create it
            insert_country = "INSERT INTO guarantor (guarantor, guarantor_country_code) VALUES ('%s', '%s')" % (
                guarantor_name, guarantor_country_code)
            guarantor_id = cur.execute(insert_country)
            conn.commit()

            # fetch after insert
            cur.execute(get_guarantor)
            result = cur.fetchone()
            guarantor_id = result[0]
            print("Created Guarantor ID: ", guarantor_id)

    except Exception as err:
        print("Guarantor Error Occurred: ", err)
   # LOAN
    try:
        insert_loan = """INSERT INTO loan (loan_number, loan_status, loan_type,
            fk_borrower_id, fk_region_id, fk_guarantor_id, fk_country_id) VALUES
            ('%s', '%s', '%s', %d, %d, %d, %d)""" % (loan_number, loan_status, loan_type, borrower_id, region_id, guarantor_id, ctry_id)
        cur.execute(insert_loan)
        conn.commit()
        # fetch most recent loan entry just created. Since there are monthly insertions with same loan number
        get_loan = """select max(loan_id) from loan where loan_number ilike '%s'""" % loan_number
        cur.execute(get_loan)
        result = cur.fetchone()
        loan_id = result[0]

    except Exception as err:
        print("Loan Error Occurred: ", err)

        # LOAN DETAILS
    try:
        insert_details = """insert into loan_details (fk_loan_id,end_of_period,interest_rate,currencyof_commitment,project_id,project_name,original_principal_amount,cancelled_amount,undisbursed_amount,disbursed_amount,repaid_to_ibrd,due_to_ibrd,exchange_adjustment,sold_3rd_party,repaid_3rd_party,due_3rd_party,first_repayment_date,last_repayment_date,agreement_signing_date,board_approval_date,effective_date_most_recent,closed_date_most_recent,last_disbursement_date,loans_held) VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
            loan_id, end_of_period, interest_rate, currencyof_commitment, project_id, project_name, original_principal_amount, cancelled_amount, undisbursed_amount, disbursed_amount, repaid_to_ibrd, due_to_ibrd, exchange_adjustment, sold_3rd_party, repaid_3rd_party, due_3rd_party, first_repayment_date, last_repayment_date, agreement_signing_date, board_approval_date, effective_date_most_recent, closed_date_most_recent, last_disbursement_date, loans_held)
        cur.execute(insert_details)
        conn.commit()

    except:
        print("Loan Details Error Occurred: ", err)

    num_of_rows += 1
    print(num_of_rows)
# if index > 7:
#   exit()
# update after process ends
cur.execute(
    "update data_loading_log set time_finished = now(), records_processed = %d where log_id = %d" % (num_of_rows, log_id))
conn.commit()

# Excel Workbook build set up

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

# Process Summary


def get_process_summary():
    process_summary = "Select file_name, time_started, time_finished, records_processed from ibrd_ug.data_loading_log where log_id = %d" % log_id

    cur.execute(process_summary)
    avg_rows = cur.fetchall()

    return avg_rows


rowsprocessed = get_process_summary()
avg_header = ["File Name", "Time Started",
              "Fime_Finished", "Records Rrocessed"]
generate_excel(avg_header, rowsprocessed, main_workbook, "Process Summary")
main_workbook.save("OUTPUT/OUTPUT SUMMARY.xls")

# Missing Values: Count for loan numbers without guarantor


def get_loan_without_guarantor():
    loan_without_guarantor = """select count(*) from (select distinct on(LN.loan_number) loan_number from ibrd_ug.loan LN, ibrd_ug.guarantor G where LN.fk_guarantor_id=G.guarantor_id and guarantor = '') AS TEMP"""

    cur.execute(loan_without_guarantor)
    avg_rows = cur.fetchall()

    return avg_rows


rowsmissing = get_loan_without_guarantor()
avg_header = ["Total Count", ""]
generate_excel(avg_header, rowsmissing, main_workbook,
               "Total Loans Without Guarantor")

main_workbook.save("OUTPUT/OUTPUT SUMMARY.xls")


# Missing Values: Count for loan numbers without borrower name


def get_loan_without_borrower_name():
    loan_without_borrower_name = """select count(*) from (select distinct on(LN.loan_number) loan_number from ibrd_ug.loan LN, ibrd_ug.borrower B where LN.fk_borrower_id=B.borrower_id and borrower = '') AS TEMP"""

    cur.execute(loan_without_borrower_name)
    avg_rows = cur.fetchall()

    return avg_rows


rowsmissingb = get_loan_without_borrower_name()
avg_header = ["Total Count", ""]
generate_excel(avg_header, rowsmissingb, main_workbook,
               "Total Loans Without Borrower")

main_workbook.save("OUTPUT/OUTPUT SUMMARY.xls")

# # Average for original_principal_amount


def get_averages_per_country():
    average_principal_per_ctry = """select TEMP.country_name, avg(TEMP.original_principal_amount) from
        (select distinct on(LN.loan_number) C.country_name,  LD.original_principal_amount from
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_principal_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_averages_per_country()

# Average for cancelled_amount


def get_averages_cancelled_amount_per_country():
    average_cancelled_amount_per_ctry = """select TEMP.country_name, avg(TEMP.cancelled_amount) from
        (select distinct on(LN.loan_number) C.country_name,  LD.cancelled_amount from
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_cancelled_amount_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rowsC = get_averages_cancelled_amount_per_country()

# Average for undisbursed_amount


def get_averages_undisbursed_amount_per_country():
    average_undisbursed_amount_per_ctry = """select TEMP.country_name, avg(TEMP.undisbursed_amount) from
        (select distinct on(LN.loan_number) C.country_name,  LD.undisbursed_amount from
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_undisbursed_amount_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rowsU = get_averages_undisbursed_amount_per_country()

# Average for disbursed_amount


def get_averages_disbursed_amount_per_country():
    average_disbursed_amount_per_ctry = """select TEMP.country_name, avg(TEMP.disbursed_amount) from
        (select distinct on(LN.loan_number) C.country_name,  LD.disbursed_amount from
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_disbursed_amount_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rowsD = get_averages_disbursed_amount_per_country()
# Average for repaid_to_ibrd


def get_averages_repaid_to_ibrd_per_country():
    average_repaid_to_ibrd_per_ctry = """select TEMP.country_name, avg(TEMP.repaid_to_ibrd) from
        (select distinct on(LN.loan_number) C.country_name,  LD.repaid_to_ibrd from
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_repaid_to_ibrd_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rowsR = get_averages_repaid_to_ibrd_per_country()

# Average for due_to_ibrd


def get_averages_due_to_ibrd_per_country():
    average_due_to_ibrd_per_ctry = """select TEMP.country_name, avg(TEMP.due_to_ibrd) from
        (select distinct on(LN.loan_number) C.country_name,  LD.due_to_ibrd from
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_due_to_ibrd_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rowsDu = get_averages_due_to_ibrd_per_country()

# Average for exchange_adjustment


def get_averages_exchange_adjustment_per_country():
    average_exchange_adjustment_per_ctry = """select TEMP.country_name, avg(TEMP.exchange_adjustment) from
        (select distinct on(LN.loan_number) C.country_name,  LD.exchange_adjustment from
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_exchange_adjustment_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rowsE = get_averages_exchange_adjustment_per_country()

# Average for sold_3rd_party


def get_averages_sold_3rd_party_per_country():
    average_sold_3rd_party_per_ctry = """select TEMP.country_name, avg(TEMP.sold_3rd_party) from
        (select distinct on(LN.loan_number) C.country_name,  LD.sold_3rd_party from
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_sold_3rd_party_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rowsS = get_averages_sold_3rd_party_per_country()

# Average for repaid_3rd_party


def get_averages_repaid_3rd_party_per_country():
    average_repaid_3rd_party_per_ctry = """select TEMP.country_name, avg(TEMP.repaid_3rd_party) from
        (select distinct on(LN.loan_number) C.country_name,  LD.repaid_3rd_party from
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_repaid_3rd_party_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rowsRe = get_averages_repaid_3rd_party_per_country()


# Average for due_3rd_party


def get_averages_due_3rd_party_per_country():
    average_due_3rd_party_per_ctry = """select TEMP.country_name, avg(TEMP.due_3rd_party) from
        (select distinct on(LN.loan_number) C.country_name,  LD.due_3rd_party from
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_due_3rd_party_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rowsDue = get_averages_due_3rd_party_per_country()

# Average for loans_held


def get_averages_loans_held_per_country():
    average_loans_held_per_ctry = """select TEMP.country_name, avg(TEMP.loans_held) from
        (select distinct on(LN.loan_number) C.country_name,  LD.loans_held from
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_loans_held_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rowsL = get_averages_loans_held_per_country()


# ADD AVERAGES TO ONE WORKBOOK
amountAvg = {}
countries = [item[0] for item in rows]
for item in rows:
    if amountAvg.get("original_principal_amount"):
        amountAvg["original_principal_amount"].append(item[1])
    else:
        amountAvg["original_principal_amount"] = [item[1]]

for item in rowsC:
    if amountAvg.get("cancelled_amount"):
        amountAvg["cancelled_amount"].append(item[1])
    else:
        amountAvg["cancelled_amount"] = [item[1]]
for item in rowsU:
    if amountAvg.get("undisbursed_amount"):
        amountAvg["undisbursed_amount"].append(item[1])
    else:
        amountAvg["undisbursed_amount"] = [item[1]]
for item in rowsD:
    if amountAvg.get("disbursed_amount"):
        amountAvg["disbursed_amount"].append(item[1])
    else:
        amountAvg["disbursed_amount"] = [item[1]]
for item in rowsR:
    if amountAvg.get("repaid_to_ibrd"):
        amountAvg["repaid_to_ibrd"].append(item[1])
    else:
        amountAvg["repaid_to_ibrd"] = [item[1]]
for item in rowsDu:
    if amountAvg.get("due_to_ibrd"):
        amountAvg["due_to_ibrd"].append(item[1])
    else:
        amountAvg["due_to_ibrd"] = [item[1]]
for item in rowsE:
    if amountAvg.get("exchange_adjustment"):
        amountAvg["exchange_adjustment"].append(item[1])
    else:
        amountAvg["exchange_adjustment"] = [item[1]]
for item in rowsS:
    if amountAvg.get("sold_3rd_party"):
        amountAvg["sold_3rd_party"].append(item[1])
    else:
        amountAvg["sold_3rd_party"] = [item[1]]
for item in rowsRe:
    if amountAvg.get("repaid_3rd_party"):
        amountAvg["repaid_3rd_party"].append(item[1])
    else:
        amountAvg["repaid_3rd_party"] = [item[1]]
for item in rowsDue:
    if amountAvg.get("due_3rd_party"):
        amountAvg["due_3rd_party"].append(item[1])
    else:
        amountAvg["due_3rd_party"] = [item[1]]
for item in rowsL:
    if amountAvg.get("loans_held"):
        amountAvg["loans_held"].append(item[1])
    else:
        amountAvg["loans_held"] = [item[1]]

workbook = xlsxwriter.Workbook(
    'OUTPUT/SUMMARY FOR AVERAGE AMOUNTS PER COUNTRY.xlsx')
worksheet = workbook.add_worksheet()
bold = workbook.add_format({'bold': .5})

# Add the worksheet data that the charts will refer to.
headings = ['Country', 'Original Principal Amount',
            'Cancelled Amount', 'Undisbursed Amount', 'Disbursed Amount', 'Repaid To IBRD', 'Due To IBRD', 'Exchange Adjustment', 'Sold 3rd Party', 'Repaid 3rd Party', 'Due 3rd Party', 'Loans Held']
data = [
    countries,  # countries
    amountAvg["original_principal_amount"],  # original_principal_amount
    amountAvg["cancelled_amount"],  # cancelled_amount
    amountAvg["undisbursed_amount"],  # undisbursed_amount
    amountAvg["disbursed_amount"],  # disbursed_amount
    amountAvg["repaid_to_ibrd"],  # repaid_to_ibrd
    amountAvg["due_to_ibrd"],  # due_to_ibrd
    amountAvg["exchange_adjustment"],  # exchange_adjustment
    amountAvg["sold_3rd_party"],  # sold_3rd_party
    amountAvg["repaid_3rd_party"],  # repaid_3rd_party
    amountAvg["due_3rd_party"],  # due_3rd_party
    amountAvg["loans_held"],  # loans_held
]


worksheet.write_row('A1', headings, bold)
worksheet.write_column('A2', data[0])
worksheet.write_column('B2', data[1])
worksheet.write_column('C2', data[2])
worksheet.write_column('D2', data[3])
worksheet.write_column('E2', data[4])
worksheet.write_column('F2', data[5])
worksheet.write_column('G2', data[6])
worksheet.write_column('H2', data[7])
worksheet.write_column('I2', data[8])
worksheet.write_column('J2', data[9])
worksheet.write_column('K2', data[10])
worksheet.write_column('L2', data[11])

workbook.close()


# Averages for Original Principal Amount, Cancelled Amount, Undisbursed Amount, Disbursed Amount


amount = {}
countries = [item[0] for item in rows]
for item in rows:
    if amount.get("original_principal_amount"):
        amount["original_principal_amount"].append(item[1])
    else:
        amount["original_principal_amount"] = [item[1]]
for item in rowsC:
    if amount.get("cancelled_amount"):
        amount["cancelled_amount"].append(item[1])
    else:
        amount["cancelled_amount"] = [item[1]]
for item in rowsU:
    if amount.get("undisbursed_amount"):
        amount["undisbursed_amount"].append(item[1])
    else:
        amount["undisbursed_amount"] = [item[1]]
for item in rowsD:
    if amount.get("disbursed_amount"):
        amount["disbursed_amount"].append(item[1])
    else:
        amount["disbursed_amount"] = [item[1]]

# GRAPH HERE

workbook = xlsxwriter.Workbook('OUTPUT/GRAPH FOR AMOUNT AVERAGES.xlsx')
worksheet = workbook.add_worksheet()
bold = workbook.add_format({'bold': .5})

# Add the worksheet data that the charts will refer to.
headings = ['Country', 'Original Principal Amount',
            'Cancelled Amount', 'Undisbursed Amount', 'Disbursed Amount']
data = [
    countries,  # countries
    amount["original_principal_amount"],  # original_principal_amount
    amount["cancelled_amount"],  # cancelled_amount
    amount["undisbursed_amount"],  # undisbursed_amount
    amount["disbursed_amount"],  # disbursed_amount
]

worksheet.write_row('A1', headings, bold)
worksheet.write_column('A2', data[0])
worksheet.write_column('B2', data[1])
worksheet.write_column('C2', data[2])
worksheet.write_column('D2', data[3])
worksheet.write_column('E2', data[4])

# Create a new chart object. In this case an embedded chart.
chart1 = workbook.add_chart({'type': 'line'})

# Configure the first series.
chart1.add_series({
    'name':       '=Sheet1!$B$1',
    'categories': '=Sheet1!$A$2:$A$149',
    'values':     '=Sheet1!$B$2:$B$149',
})

# Configure a second series. Note use of alternative syntax to define ranges.
# Series are the different columns to be plotted on the line Graph
chart1.add_series({
    'name':       '=Sheet1!$C$1',
    'categories': '=Sheet1!$A1$2:$A$149',
    'values':     '=Sheet1!$C$2:$C$149',
})
chart1.add_series({
    'name':       '=Sheet1!$D$1',
    'categories': '=Sheet1!$A1$2:$A$149',
    'values':     '=Sheet1!$D$2:$D$149',
})
chart1.add_series({
    'name':       '=Sheet1!$E$1',
    'categories': '=Sheet1!$A1$2:$A$149',
    'values':     '=Sheet1!$E$2:$E$149',
})
# Add a chart title and some axis labels.
chart1.set_title(
    {'name': 'Graph Reprensting the average Original Principal Amount, Cancelled Amount, Undisbursed Amount and Disbursed Amount (Expand graph to view all results'})
chart1.set_x_axis({'name': 'Countries'})
chart1.set_y_axis({'name': 'Amount'})

# Set an Excel chart style. Colors with white outline and shadow.
chart1.set_style(10)
# Insert the chart into the worksheet (with an offset).
worksheet.insert_chart('G2', chart1, {'x_offset': 25, 'y_offset': 10})

workbook.close()
# Loans taked by each Country


def get_total_loan_per_ctry():
    total_loan_per_ctry = """select TEMP.country_name, count(TEMP.loan_number) from
        (select distinct on(LN.loan_number) C.country_name, LN.loan_number from
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN where
        C.country_id=LN.fk_country_id) AS TEMP group by country_name"""

    cur.execute(total_loan_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rowsTake = get_total_loan_per_ctry()

# Maximum amount taken by a country


def get_max_loan_per_ctry():
    max_loan_per_ctry = """select TEMP.country_name, max(TEMP.disbursed_amount) from
        (select distinct on(LN.loan_number) C.country_name,  LD.disbursed_amount from
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(max_loan_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rowsMax = get_max_loan_per_ctry()

# Minimum amount taken by a country


def get_min_loan_per_ctry():
    min_loan_per_ctry = """select TEMP.country_name, min(TEMP.disbursed_amount) from
        (select distinct on(LN.loan_number) C.country_name,  LD.disbursed_amount from
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(min_loan_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rowsMin = get_min_loan_per_ctry()

# ONE WORKBOOK FOR LOANS TAKEN, MAXIMUM AND MINIMUM DISBURSED AMOUNT
amountM = {}
countries = [item[0] for item in rowsTake]
for item in rowsTake:
    if amountM.get("loan_number"):
        amountM["loan_number"].append(item[1])
    else:
        amountM["loan_number"] = [item[1]]
for item in rowsMax:
    if amountM.get("disbursed_amount"):
        amountM["disbursed_amount"].append(item[1])
    else:
        amountM["disbursed_amount"] = [item[1]]
for item in rowsMin:
    if amountM.get("min_disbursed_amount"):
        amountM["min_disbursed_amount"].append(item[1])
    else:
        amountM["min_disbursed_amount"] = [item[1]]

workbook = xlsxwriter.Workbook(
    'OUTPUT/LOAN STATISTICS MIN AND MAX DISBURSED AMOUNTS.xlsx')
worksheet = workbook.add_worksheet()
bold = workbook.add_format({'bold': .5})

# Add the worksheet data that the charts will refer to.
headings = ['Country', 'Total Loans Taken',
            'Maximum Disbursed Amount', 'Minimum Disbursed Amount']
data = [
    countries,  # countries
    amountM["loan_number"],  # original_principal_amount
    amountM["disbursed_amount"],  # max_disbursed_amount
    amountM["min_disbursed_amount"],  # min_disbursed_amount
]

worksheet.write_row('A1', headings, bold)
worksheet.write_column('A2', data[0])
worksheet.write_column('B2', data[1])
worksheet.write_column('C2', data[2])
worksheet.write_column('D2', data[3])

# Create a new chart object. In this case an embedded chart.

workbook.close()
##
# All Loan types submitted in current raw file


def get_total_loan_types():
    total_loan_type = """select DISTINCT loan_type from ibrd_ug.loan"""

    cur.execute(total_loan_type)
    avg_rows = cur.fetchall()

    return avg_rows


rowstypes = get_total_loan_types()
avg_header = ["Loan Types", ""]
generate_excel(avg_header, rowstypes, main_workbook, "loan_types")

main_workbook.save("OUTPUT/OUTPUT SUMMARY.xls")

# Count for all Loan statuses in file


def get_total_loan_statuses():
    total_loan_statuses = """select derived_status, count(*) from (select distinct on(loan_number) loan_status AS derived_status, * from ibrd_ug.loan) AS TEMP
    group by TEMP.derived_status"""

    cur.execute(total_loan_statuses)
    avg_rows = cur.fetchall()

    return avg_rows


rowstatuses = get_total_loan_statuses()
avg_header = ["Loan Status", "Total Count"]
#generate_excel(avg_header, rowstatuses, main_workbook, "loan_statuses")

#main_workbook.save("OUTPUT/OUTPUT SUMMARY.xls")
# THEN THE GRAPH
amountStatus = {}
statusShow = [item[0] for item in rowstatuses]
for item in rowstatuses:
    if amountStatus.get("loan_status_count"):
        amountStatus["loan_status_count"].append(item[1])
    else:
        amountStatus["loan_status_count"] = [item[1]]

workbook = xlsxwriter.Workbook(
    'OUTPUT/GRAPH FOR LOAN STATUSES AND COUNTS.xlsx')
worksheet = workbook.add_worksheet()
bold = workbook.add_format({'bold': .5})

# Add the worksheet data that the charts will refer to.
headings = ['Loan Status', 'Total Count']
data = [
    statusShow,  # loan_status
    amountStatus["loan_status_count"],  # loan_status_count
]

worksheet.write_row('A1', headings, bold)
worksheet.write_column('A2', data[0])
worksheet.write_column('B2', data[1])

# Create a new chart object. In this case an embedded chart.
chart1 = workbook.add_chart({'type': 'line'})

# Configure the first series.
chart1.add_series({
    'name':       '=Sheet1!$B$1',
    'categories': '=Sheet1!$A$2:$A$11',
    'values':     '=Sheet1!$B$2:$B$11',
})


# Add a chart title and some axis labels.
chart1.set_title(
    {'name': 'Graph Reprensting All Loan Statuses and their total count'})
chart1.set_x_axis({'name': 'Countries'})
chart1.set_y_axis({'name': 'Amount'})

# Set an Excel chart style. Colors with white outline and shadow.
chart1.set_style(10)
# Insert the chart into the worksheet (with an offset).
worksheet.insert_chart('E2', chart1, {'x_offset': 25, 'y_offset': 10})

workbook.close()

# Email notifier to the supplier of the data with link to SFTP directory with the results


def send_email(recepient, msg_body):
   # today = date.today()
    msg = MIMEMultipart()
    msg["From"] = 'develop.emailer@gmail.com'
    msg["To"] = recepient
    msg["Subject"] = "Summary for File"
    msg.attach(MIMEText(msg_body, "plain"))

    # smtp.gmail.com is gmails mail server, Compuscans would need to use its own
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login('develop.emailer@gmail.com', 'testdevelop20')
    server.sendmail('develop.emailer@gmail.com', recepient, msg.as_string())
    server.quit()


message_body = "Good day, \nYour recent monthly file has been processed successfully.\nRefer to the SFTP directory for results. \n\n Regards\n Data Team"
send_email("develop.emailer@gmail.com", message_body)
print("Email Sent")

print("File Processing Complete, Check Output Directory For Results")
######
