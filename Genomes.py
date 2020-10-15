import psycopg2


def insert_nucleotides(k, genome, number):
    cur = con.cursor()
    if number == 1:
        cur.execute("truncate table genome_1")
    else:
        cur.execute("truncate table genome_2")

    con.commit()
    start = 0
    end = k
    length = len(genome)
    while end < length:
        # print(genome[start:end])
        comb = genome[start:end]
        values = ({'id': start, 'name': comb})
        # insert to database
        if number == 1:
            cur.execute(
                "insert into genome_1 (id, name) values (%(id)s,%(name)s)",
                values
            )
        else:
            cur.execute(
                "insert into genome_2 (id, name) values (%(id)s,%(name)s)",
                values
            )
        con.commit()
        print("Record inserted successfully")
        end += 1
        start += 1


def select_Jaccard_index(k):
    cur = con.cursor()
    cur.execute(
        "select ((select count(*) from ((select name from genome_1) intersect all    (select name from genome_2)) as t1)::float/((select count(*) from ((select name from genome_1) union all  (select  name from genome_2)) as t2)::float-(select count(*) from ((select name from genome_1) intersect all   (select name from genome_2)) as t1)::float))")
    con.commit()
    rows = cur.fetchall()

    for row in rows:
        print("Jaccard index_" + str(k) + "=", row[0])


con = psycopg2.connect(
    database="postgres",
    user="postgres",
    password="",
    host="firstrustemdatabase.cdkjzpvfcpw0.us-east-1.rds.amazonaws.com",
    port="5432"
)

print("Database opened successfully")


file_1 = open('Genome_1-1.txt')
genome_1 = file_1.read()
file_2 = open('Genome_2-1.txt')
genome_2 = file_2.read()
k = [3, 5, 9]
for i in k:
    insert_nucleotides(i, genome_1, 1)
    insert_nucleotides(i, genome_2, 2)
    select_Jaccard_index(i)
con.close()
