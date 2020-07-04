import csv
import psycopg2
#from statistics import get_averages_per_country


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

# Average for original_principal_amount


def get_averages_per_country():
    average_principal_per_ctry = """select TEMP.country_name, avg(TEMP.original_principal_amount) from 
        (select distinct on(LN.loan_number) C.country_name,  LD.original_principal_amount from 
        ibrd_ug.COUNTRY C, ibrd_ug.LOAN LN, ibrd_ug.LOAN_DETAILS LD where 
        C.country_id=LN.fk_country_id and LD.fk_loan_id=LN.loan_id) AS TEMP group by country_name"""

    cur.execute(average_principal_per_ctry)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_averages_per_country()
for row in rows:
    print(row)

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
for row in rows:
    print(row)

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
for row in rows:
    print(row)

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
for row in rows:
    print(row)

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
for row in rows:
    print(row)

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
for row in rows:
    print(row)

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
for row in rows:
    print(row)

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
for row in rows:
    print(row)

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
for row in rows:
    print(row)

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
for row in rows:
    print(row)

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
for row in rows:
    print(row)

# All Loan types submitted in current raw file


def get_total_loan_types():
    total_loan_type = """select DISTINCT loan_type from ibrd_ug.loan"""

    cur.execute(total_loan_type)
    avg_rows = cur.fetchall()

    return avg_rows


rows = get_total_loan_types()
for row in rows:
    print(row)
