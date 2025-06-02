import json
import math
import psycopg2
import psycopg2.extras


def load_data(table_name, file_path, connection, header_path):
    cursor = connection.cursor()

    # Load header and data types
    with open(header_path, 'r') as f:
        headers = json.load(f)

    columns = ', '.join(f"{col} {dtype}" for col, dtype in headers.items())
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} ({columns})")

    # Read CSV and load into table
    with open(file_path, 'r', encoding='utf-8') as f:
        next(f)  # skip header line
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", f)

    connection.commit()
    cursor.close()


def range_partition(data_table_name, partition_table_prefix, num_partitions, header_path, column_to_partition, connection):
    cursor = connection.cursor()

    with open(header_path, 'r') as f:
        headers = json.load(f)

    columns = ', '.join(f"{col} {dtype}" for col, dtype in headers.items())

    cursor.execute(f"DROP TABLE IF EXISTS {partition_table_prefix} CASCADE")
    cursor.execute(f"CREATE TABLE {partition_table_prefix} ({columns}) PARTITION BY RANGE ({column_to_partition})")

    cursor.execute(f"SELECT MIN({column_to_partition}), MAX({column_to_partition}) FROM {data_table_name}")
    min_val, max_val = cursor.fetchone()
    interval = math.ceil((max_val - min_val + 1) / num_partitions)

    for i in range(num_partitions):
        start = min_val + i * interval
        end = start + interval
        part_table_name = f"{partition_table_prefix}{i}"
        cursor.execute(f"""
            CREATE TABLE {part_table_name} PARTITION OF {partition_table_prefix}
            FOR VALUES FROM ({start}) TO ({end})
        """)

    cursor.execute(f"INSERT INTO {partition_table_prefix} SELECT * FROM {data_table_name}")
    connection.commit()
    cursor.close()


def round_robin_partition(data_table_name, partition_table_name, num_partitions, header_file, connection):
    """Create round-robin partitioned table with minimal output"""
    cursor = connection.cursor()
    
    try:
        with open(header_file) as f:
            header_dict = json.load(f)
        columns = ", ".join(f"{k} {v}" for k, v in header_dict.items())

        cursor.execute(f"DROP TABLE IF EXISTS {partition_table_name} CASCADE")
        cursor.execute(f"DROP SEQUENCE IF EXISTS {partition_table_name}_insert_seq")
        connection.commit()

        cursor.execute(f"CREATE TABLE {partition_table_name} ({columns})")
        connection.commit()

        for i in range(num_partitions):
            cursor.execute(f"""
                CREATE TABLE {partition_table_name}{i} (
                    {columns}
                ) INHERITS ({partition_table_name});
            """)
        connection.commit()

        cursor.execute(f"CREATE SEQUENCE {partition_table_name}_insert_seq")
        connection.commit()

        trigger_func = f"""
        CREATE OR REPLACE FUNCTION {partition_table_name}_insert_trigger()
        RETURNS TRIGGER AS $$
        DECLARE
            part_num INTEGER;
        BEGIN
            part_num := nextval('{partition_table_name}_insert_seq') % {num_partitions};
            EXECUTE format('INSERT INTO {partition_table_name}%s VALUES ($1.*)', part_num)
            USING NEW;
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
        """
        cursor.execute(trigger_func)
        connection.commit()

        cursor.execute(f"""
            CREATE TRIGGER {partition_table_name}_trigger
            BEFORE INSERT ON {partition_table_name}
            FOR EACH ROW EXECUTE FUNCTION {partition_table_name}_insert_trigger();
        """)
        connection.commit()

        cursor.execute(f"SELECT COUNT(*) FROM {data_table_name}")
        total_rows = cursor.fetchone()[0]
        base_count = total_rows // num_partitions
        remainder = total_rows % num_partitions

        partition_counts = [base_count + 1 if i < remainder else base_count for i in range(num_partitions)]

        cursor.execute(f"SELECT * FROM {data_table_name} ORDER BY id")
        rows = cursor.fetchall()

        current_partition = 0
        rows_in_partition = 0

        for i, row in enumerate(rows):
            placeholders = ", ".join(["%s"] * len(row))
            cursor.execute(
                f"INSERT INTO {partition_table_name}{current_partition} VALUES ({placeholders})",
                row
            )
            rows_in_partition += 1
            if rows_in_partition >= partition_counts[current_partition]:
                current_partition += 1
                rows_in_partition = 0

            if i % 10000 == 0:
                connection.commit()

        cursor.execute(f"""
            ALTER SEQUENCE {partition_table_name}_insert_seq 
            RESTART WITH {total_rows % num_partitions}
        """)
        connection.commit()
     
    except Exception as e:
        connection.rollback()
        print(f"Error: {str(e)}")
        raise
    finally:
        cursor.close()



