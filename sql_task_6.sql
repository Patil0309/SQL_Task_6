CREATE TABLE loan_data_combined (
    customer_id VARCHAR(20),
    name VARCHAR(100),
    gender VARCHAR(10),
    age INT,
    marital_status VARCHAR(20),
    employment_status VARCHAR(30),
    annual_income BIGINT,
    loan_id VARCHAR(20),
    loan_type VARCHAR(30),
    loan_amount BIGINT,
    loan_term INT,
    application_date DATE,
    loan_status VARCHAR(20),
    disbursement_date DATE,
    disbursed_amount BIGINT,
    payment_date DATE,
    payment_amount BIGINT,
    principal_component FLOAT,
    interest_component FLOAT,
    balance_remaining BIGINT,
    default_date DATE,
    overdue_amount BIGINT,
    recovery_status VARCHAR(30),
    credit_score INT,
    credit_report_date DATE,
    branch_name VARCHAR(100),
    city VARCHAR(50),
    issue_type VARCHAR(100),
    ticket_status VARCHAR(20)
);

copy loan_data_combined from 'D:\DA_17\SQL_14\loan_data_combined_5000.csv' csv header;

ALTER TABLE loan_data_combined
ALTER COLUMN disbursed_amount TYPE NUMERIC

ALTER TABLE loan_data_combined
ALTER COLUMN annual_income TYPE NUMERIC,
ALTER COLUMN loan_amount TYPE NUMERIC,
ALTER COLUMN payment_amount TYPE NUMERIC,
ALTER COLUMN principal_component TYPE NUMERIC,
ALTER COLUMN interest_component TYPE NUMERIC,
ALTER COLUMN balance_remaining TYPE NUMERIC,
ALTER COLUMN overdue_amount TYPE NUMERIC

select * from loan_data_combined

update loan_data_combined set disbursed_amount =0 where disbursed_amount is null

--Normalization
--first table
create table customers_info as
(select distinct customer_id, name,gender,age,marital_status,employment_status,annual_income
from loan_data_combined)

select * from customers_info

--Second Table

create table loan_data as 
  ( select distinct customer_id,loan_id,loan_type,loan_amount,loan_term,application_date,
  loan_status,disbursement_date,disbursed_amount,
  default_date,overdue_amount,recovery_status,credit_score,
  credit_report_date from loan_data_combined)

--Third Table creating from table loan_data OR subtable of loan_data

create table defaults as 
select distinct loan_id,default_date,overdue_amount,recovery_status 
from loan_data

select * from defaults 

--Fourth Table
create table Payment_info
as (select distinct loan_id,payment_date,payment_amount,
     principal_component,interest_component,
     balance_remaining
     from loan_data_combined )
	 
--Fifth table
create table Branches as 
(select loan_id,branch_name,city,issue_type,ticket_status from loan_data_combined)

select * from branches

--Sixth table 
CREATE TABLE credits AS
(SELECT DISTINCT
    loan_id,
    credit_score,
    credit_report_date
FROM loan_data_combined)
------------------------------------------------------------------------
select * from loan_data_combined 

select * from customers_info

select * from loan_data

select * from defaults

select * from credits

select* from payment_info

select * from branches

--10 Join Queries
--1]
select l.loan_id,l.credit_score,c.name, c.gender,c.age,c.employment_status,c.annual_income 
       from customers_info as c
       inner join loan_data as l
       on c.customer_id=l.customer_id
       inner join credits as cr 
       on cr.loan_id=l.loan_id     
	   where cr.credit_score>740

--2]
select c.customer_id,c.name,l.loan_id,l.loan_type,l.loan_amount,l.loan_term 
      from customers_info as c
	  inner join loan_data as l
	  on c.customer_id=l.customer_id
	  order by loan_amount desc, loan_term desc
	  limit 10

--3]
select l.loan_id,branch_name,city,ticket_status,loan_type,loan_amount
       from branches as b
	   left join loan_data as l
	   on b.loan_id=l.loan_id
	   where ticket_status is not null and loan_type='Personal'

--4] tables included are customer,defaults,loans_data

select c.name,l.loan_amount,l.loan_status ,d.default_date,d.overdue_amount,d.recovery_status from defaults as d
       left join loan_data as l
       on l.loan_id=d.loan_id
       inner join customers_info as c
       on c.customer_id=l.customer_id
       where d.overdue_amount is not null

--5]table include load_data and and payment
--before that removing null value from balance_remaining column 

update payment_info set balance_remaining =0 where balance_remaining is null

select * from payment_info

select l.loan_id,l.loan_amount, sum(p.payment_amount)as total_pay,max(p.balance_remaining )as current_balance 
       from payment_info as p
       inner join loan_data as l
       on l.loan_id=p.loan_id
       group by l.loan_id,l.loan_amount
       having sum(p.payment_amount)is not null

--6]table includes loans and defautls
--to know a overall timeline of loan from approve to disbursement

select l.loan_id,l.loan_amount,d.overdue_amount,l.application_date,l.disbursement_date,d.default_date
       from loan_data as l
       inner join defaults as d
       on l.loan_id=d.loan_id
       where l.disbursement_date is not null and d.overdue_amount is not null

--7]Join on Braches and loan_data table

select count(b. loan_id)as total_loans,b.branch_name,city,sum(l.disbursed_amount )as total_disbursed from branches as b
       inner join loan_data as l
	   on b.loan_id=l.loan_id 
	   group by b.city,b.branch_name
	   
--8]Using customer_table and loan_data table
SELECT   l.loan_id,  c.name,l.application_date,l.disbursement_date,(l.disbursement_date - l.application_date) AS days_to_disburse
      FROM loan_data as l
      JOIN customers_info as c ON l.customer_id = c.customer_id
      WHERE l.disbursement_date IS NOT NULL;

--9] Using Multi Join on customer,defaults,credits table 
    --By analyzing the credit history and their default behaviour we can mark which loans at risk
SELECT l.loan_id, c.name, cr.credit_score, d.overdue_amount, d.recovery_status
     FROM loan_data as l
     inner JOIN customers_info as c ON l.customer_id = c.customer_id
     inner JOIN credits as cr ON l.loan_id = cr.loan_id
     inner JOIN defaults as d ON l.loan_id = d.loan_id;	   

--10]
SELECT c.name, l.loan_amount
     FROM customers_info as c
     INNER JOIN loan_data as l ON c.customer_id = l.customer_id;

--second protion of the task covering aggregation ,groupby,having function

--1]
select employment_status,count(customer_id)as total_customer 
from customers_info
group by employment_status

--2]
select * from loan_data

select loan_type,avg(loan_amount)as avg_loan_amount
from loan_data
group by loan_type
order by avg_loan_amount desc

--3]
select * from defaults

select loan_id,recovery_status ,sum(overdue_amount)as total_overdue
from defaults
group by recovery_status,loan_id
having sum(overdue_amount)>10000

--4]
select * from branches

select branch_name,city,count(loan_id)as total_loan 
from branches
group by branch_name,city
having count(loan_id)>10
order by total_loan desc

5]
select * from credits

select * from credits where credit_score >750

6]
select * from customers_info

select * from branches

select c.customer_id,c.name,b.branch_name,b.city,l.loan_type,l.loan_amount,l.loan_status
from customers_info as c
left join loan_data as l
on c.customer_id=l.customer_id
inner join branches as b
on l.loan_id=b.loan_id

7]
SELECT loan_id,loan_amount
FROM loan_data
ORDER BY loan_amount DESC
LIMIT 5;

8]
select * from loan_data

SELECT    customer_id, loan_id, loan_term
FROM loan_data
WHERE loan_term > 60
ORDER BY loan_term DESC;

--9]

SELECT  employment_status, AVG(annual_income) AS avg_income
FROM customers_info
WHERE employment_status = 'Employed'
GROUP BY employment_status;

--10]
SELECT loan_id, SUM(overdue_amount) AS total_overdue
FROM defaults
WHERE overdue_amount is null
GROUP BY loan_id




	  



















