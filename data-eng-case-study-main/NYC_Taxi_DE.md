----------------------------Data Engineer Case Study on NYC tax1---------------------

1. Download docker desktop(Download for Windows AMD64 it will support intel as well verson 4.41.2.0 579 MB) and install that docker and skip unnecessary things.
2. Goto cmd and check docker --version and docker compose version which is Docker version 28.1.1, build 4eba377 and Docker Compose version v2.35.1-desktop.1
3. Download the Required Data

Download NYC Taxi Data:

Go to: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
Download "Yellow Taxi Trip Records" for:

June 2024
July 2024
August 2024
These will be .parquet files

Download Reference Data:
download the "Taxi Zone Lookup Table"
This will be a .csv file

4. Set up Project Structure
Create the following directory structure in your project:
your-project/
├── setup/
│   ├── data/          # Place the 3 monthly taxi files here, this folder we have created newly
│   └── reference-data/ # Place the taxi zone lookup table here, this folder we have created newly
└── docker-compose.yml  # Should already exist in starter code

5. Then navigate to your project root directory and run: docker compose up --build -d
My_dir:- D:\Chrome Downloads\data-eng-case-study-main>docker compose up --build -d

This command will:

Build the necessary containers, Start MinIO (object storage), Start PostgreSQL (database), Run everything in the background (-d flag)

6. Then type D:\Chrome Downloads\data-eng-case-study-main>docter ps  --It wil show the containers list those are running

CONTAINER ID   IMAGE            COMMAND                  CREATED         STATUS         PORTS                              NAMES
0531e922e1d2   postgres:16      "docker-entrypoint.s…"   3 minutes ago   Up 3 minutes   0.0.0.0:5432->5432/tcp             my-postgres-container
3c90c2da2702   custom-minio     "/usr/bin/docker-ent…"   3 minutes ago   Up 3 minutes   0.0.0.0:9000-9001->9000-9001/tcp   minio-server-combined
8f23ced7a316   dpage/pgadmin4   "/entrypoint.sh"         3 minutes ago   Up 3 minutes   443/tcp, 0.0.0.0:5050->80/tcp      pgadmin

Now as per documents this link should work http://localhost:9091/ but since minio is running on port 9000-9001 so we can use this url
http://localhost:9001/

7. Now check the credential in D:\Chrome Downloads\data-eng-case-study-main\src\main\resources\application.conf and login

Now login MinIO

After logging in, you should see: A bucket called taxi-bucket Inside the bucket, two folders: reference-data/ (containing your taxi zone lookup table) taxi-data/ (containing your 3 monthly taxi files)

8. Now login in postgres pgadmin4 and create the new database with these commands

9. For running pipeline you need to do the install extension in vscode like scala metal by scalmeta and press ctrl+shift+p and import metal build if not working then check the error if java error then configure the java lower version like jdk 11 and press ctrl+shift+p preference 

