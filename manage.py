import csv
import psycopg2

try:
    conn = psycopg2.connect(dbname="IBRD", host="localhost",
                            user="postgres", password='1234', options="-c search_path=ibrd_ug")
    cur = conn.cursor()

except psycopg2.DatabaseError as err:
    print('Error %s' % err)


reader = csv.reader(open('ibrd.csv', 'r'), delimiter=',')
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
    sold_3rd_party = row[21]
    repaid_3rd_party = row[22]
    due_3rd_party = row[23]
    loans_held = row[24]
    first_repayment_date = row[25]
    last_repayment_date = row[26]
    agreement_signing_date = row[27]
    board_approval_date = row[28]
    effective_date_most_recent = row[29]
    closed_date_most_recent = row[30]
    last_disbursement_date = row[31]
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
            insert_country = "INSERT INTO country (country_name, country_code, fk_region_id) VALUES ('%s', '%s', '%s')" % (
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
            insert_borrower = "INSERT INTO borrower (borrower) VALUES ('%s')" % (
                borrower)
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
        insert_loan = """INSERT INTO loan (loan_number, loan_status, 
            fk_borrower_id, fk_region_id, fk_guarantor_id) VALUES 
            ('%s', '%s', '%d', '%d', '%d')""" % (loan_number, loan_status, borrower_id, region_id, guarantor_id)
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
        insert_details = """insert into loan_details (fk_loan_id,end_of_period,interest_rate,currencyof_commitment,project_id,project_name,original_principal_amount,cancelled_amount,undisbursed_amount,disbursed_amount,repaid_to_ibrd,due_to_ibrd,exchange_adjustment,sold_3rd_party,repaid_3rd_party,due_3rd_party,first_repayment_date,last_repayment_date,agreement_signing_date,board_approval_date,effective_date_most_recent,closed_date_most_recent,last_disbursement_date,loans_held) VALUES ('%d', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
            loan_id, end_of_period, interest_rate, currencyof_commitment, project_id, project_name, original_principal_amount, cancelled_amount, undisbursed_amount, disbursed_amount, repaid_to_ibrd, due_to_ibrd, exchange_adjustment, sold_3rd_party, repaid_3rd_party, due_3rd_party, first_repayment_date, last_repayment_date, agreement_signing_date, board_approval_date, effective_date_most_recent, closed_date_most_recent, last_disbursement_date, loans_held)
        cur.execute(insert_details)
        conn.commit()

    except:
        print("Loan Details Error Occurred: ", err)
    # if index > 7:
    #   exit()
