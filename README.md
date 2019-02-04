# InfusionsoftDatabaseTransfer

This program is designed to transfer data between two Infusionsoft by Keap applications (or even Keap apps).

## Why?

Customers sometimes want new applications for various reasons: new business, rebranding, splitting businesses, or selling their business to someone who already has an IBK app.

In the past, we have transferred the data via ETL and the frontend, via XML-RPC and REST APIs, but they were either time consuming or consumed a lot of API resources.

Transferring directly through the database means we can be more exact, that we don't need API keys or any frontend interaction, and increases speed of turnaround

## Database Objects Transferred

There is a decent list of things that are transferred, but it's only a fraction of the tables in the database. The methods taken to transfer the data come from an approach of importing the data from an existing system, not copying tables and databases. Many times, the destination application has existing data in their database, and the transfer is intended to supplement their current dataset. With that said, this program does not overwrite, but simply add onto the existing data.

Here is a list of all of the different objects transferred through this program:

- Contacts
- Tags
- Contact Custom Fields
- Lead Sources
- Notes/Tasks/Appointments (aka Contact Actions)
- Products
- Opportunities
- Orders
- Subscriptions
- Credit Cards

## Technology Implemented

- Python
- Pandas
  - Dataframe package for transforming and otherwise managing table-like data
- MySQL-connector
  - Allows connection to Infusionsoft's landing server(s) for production applications
- Other built-in packages

NOTE: This program can only be run by someone who has production database access, with port forwarding enabled. The ports used to connect to the MySQL databases are outlined in the "portForwards" command in the production landing server.
