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
csv_filename = 'ibrd.csv'
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
    # Region
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
    process_summary = "Select file_name, time_started, time_finished, records_processed from ibrd_ug.data_loading_log where log_id = 21"  # % log_id

    cur.execute(process_summary)
    avg_rows = cur.fetchall()

    return avg_rows


rowsprocessed = get_process_summary()
avg_header = ["File Name", "Time Started",
              "Fime_Finished", "Records Rrocessed"]
generate_excel(avg_header, rowsprocessed, main_workbook, "Process Summary")
main_workbook.save("Summary.xls")


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
avg_header = ["Country", "Average Original Principal Amount"]
generate_excel(avg_header, rows, main_workbook, "Averages_Principal_Amount")

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
generate_excel(avg_header, rows, main_workbook, "Cancelled_Amount_Per_Country")

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
