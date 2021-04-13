# Bank Statement Processor

This project was created just to create pipeline and see how it works.
It process a bank statement file in the OFX format, and populates its results to a database.

## Running the project

Before running the code, it's needed to create the database. 
Just run the docker compose command inside the project's root folder.

`docker-compose up -d`

It will create a postgre instance. Connect to this instance using the port and credentials used in the docker-compose.taml file.
Then creates the table with the file `sql/createTable.sql`.

After that, just take some OFX file sample over the internet (or in your bank account) and put it inside the `data`folder with the name `bank_statement.ofx`.

Then run the following command:

`go run main.go`