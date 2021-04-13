
CREATE TABLE IF NOT EXISTS transactions (
    id serial primary key,
    bank varchar(255),
    bank_branch varchar(255),
    account varchar(255),
    amount decimal(12,2),
    date timestamp,
    type varchar(100),
    memo VARCHAR(255),
    fitid VARCHAR(255)
)


select * from transactions