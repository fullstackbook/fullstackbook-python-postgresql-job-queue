import psycopg

conn = psycopg.connect("dbname=job_db user=postgres")

cur = conn.cursor()

for i in range(5):
    conn.execute(
        "insert into jobs (job_data, job_status) values (%s, %s)",
        [f"hello world {i}", "pending"],
    )

conn.commit()
