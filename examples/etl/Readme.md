# Tools
1. Docker: https://docs.docker.com/desktop/install/windows-install/
2. Python

# How to
1. Open and start your Docker
2. Create the `docker-compose.yml` and create MySQL and Postgres Container declaratively
3. Run this command `docker-compose up -d` to make MySQL and Postgres Image run as a Container
4. Create ETL script to Extract data from MySQL and Store it as a table in Postgres, so in this step we try to create ETL pipeline that run manually
5. Load csv file in `data` directory to MySQL database by running this command
`mysql --local-infile=1 -uroot -pmysql operational < /docker-entrypoint-initdb.d/init.sql`

