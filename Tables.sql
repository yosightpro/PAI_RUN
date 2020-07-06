CREATE TABLE ibrd_ug.borrower
(
   borrower_id           integer   DEFAULT nextval('ibrd_ug.borrowers_borrower_id_seq'::regclass) NOT NULL,
   borrower              text,
   borrowers_obligation  text
);

CREATE TABLE ibrd_ug.country
(
   country_id    integer   DEFAULT nextval('ibrd_ug.countries_country_id_seq'::regclass) NOT NULL,
   country_name  text,
   fk_region_id  integer   NOT NULL,
   country_code  text
);

CREATE TABLE ibrd_ug.data_loading_log
(
   log_id             serial    NOT NULL,
   file_name          text,
   records_processed  text,
   time_started       varchar,
   time_finished      varchar
);


CREATE TABLE ibrd_ug.guarantor
(
   guarantor_id            integer   DEFAULT nextval('ibrd_ug.guarantors_guarantor_id_seq'::regclass) NOT NULL,
   guarantor               text,
   guarantor_country_code  text
);

CREATE TABLE ibrd_ug.loan
(
   loan_id          integer   DEFAULT nextval('ibrd_ug.loans_loan_id_seq'::regclass) NOT NULL,
   loan_number      text,
   loan_type        text,
   loan_status      text,
   fk_region_id     integer,
   fk_country_id    integer,
   fk_guarantor_id  integer,
   fk_borrower_id   integer
);

CREATE TABLE ibrd_ug.loan_details
(
   loan_details_id             serial    NOT NULL,
   fk_loan_id                  integer,
   end_of_period               text,
   interest_rate               text,
   currencyof_commitment       text,
   project_id                  text,
   project_name                text,
   original_principal_amount   numeric,
   cancelled_amount            numeric,
   undisbursed_amount          numeric,
   disbursed_amount            numeric,
   repaid_to_ibrd              numeric,
   due_to_ibrd                 numeric,
   exchange_adjustment         numeric,
   sold_3rd_party              numeric,
   repaid_3rd_party            numeric,
   due_3rd_party               numeric,
   first_repayment_date        text,
   last_repayment_date         text,
   agreement_signing_date      text,
   board_approval_date         text,
   effective_date_most_recent  text,
   closed_date_most_recent     text,
   last_disbursement_date      text,
   loans_held                  numeric
);

CREATE TABLE ibrd_ug.region
(
   region_id    integer   DEFAULT nextval('ibrd_ug.regions_region_id_seq'::regclass) NOT NULL,
   region_name  text      NOT NULL
);