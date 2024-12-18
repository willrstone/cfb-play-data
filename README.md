cfb-play-data is a project that gathers data from an API, filters it, and then saves it in a postgres database. Once the data was saved, I used SQL queries like the one attached
to create view that contained the data I needed. The data was then pushed to Azure Blob storage via the db_to_cloud.py file.
