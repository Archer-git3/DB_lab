# -*- coding: utf-8 -*-
import csv, os
import time
import psycopg2
import pandas as pd
import os

from psycopg2 import sql

GENERAL_TABLE_NAME = "res_zno"
OUT_CSV_FILE = "result_zno.csv"

txt_path="./output/result_time.txt"
CSV_CONFIG = {
    'PATH_CONFIG': {
        2020: {'path': './dataset/Odata2020File.csv',
               'encoding': 'Windows-1251'
               },
        2019: {'path': './dataset/Odata2019File.csv',
               'encoding': 'Windows-1251'
               }
    },
    'OPEN_CONFIG': {
        'delimiter': ';',
        'na_values': ['null'],
        'engine': 'python'
    }
}


SQL_PARAM = {
    'QUERY': {
        'CREATE_TABLE': f"""
        CREATE TABLE {GENERAL_TABLE_NAME} (
        ID INTEGER,
        OUTID VARCHAR(255) PRIMARY KEY,
        Birth INTEGER,
        SexTypeName VARCHAR(255),
        REGNAME VARCHAR(255),
        AREANAME VARCHAR(255),
        TERNAME VARCHAR(255),
        REGTYPENAME VARCHAR(255),
        TerTypeName VARCHAR(255),
        ClassProfileNAME VARCHAR(255),
        ClassLangName VARCHAR(255),
        EONAME VARCHAR(255),
        EOTYPENAME VARCHAR(255),
        EORegName VARCHAR(255),
        EOAreaName VARCHAR(255),
        EOTerName VARCHAR(255),
        EOParent VARCHAR(255),
        UMLTest VARCHAR(255),
        UMLTestStatus VARCHAR(255),
        UMLBall100 FLOAT,
        UMLBall12 FLOAT,
        UMLBall FLOAT,
        UMLAdaptScale VARCHAR(255),
        UMLPTName VARCHAR(255),
        UMLPTRegName VARCHAR(255),
        UMLPTAreaName VARCHAR(255),
        UMLPTTerName VARCHAR(255), 
        UkrTest VARCHAR(255),
        UkrSubTest VARCHAR(255),
        UkrTestStatus VARCHAR(255),
        UkrBall100 FLOAT,
        UkrBall12 FLOAT,
        UkrBall FLOAT,
        UkrAdaptScale VARCHAR(255),
        UkrPTName VARCHAR(255),
        UkrPTRegName VARCHAR(255),
        UkrPTAreaName VARCHAR(255),
        UkrPTTerName VARCHAR(255),
        histTest VARCHAR(255),
        HistLang VARCHAR(255),
        histTestStatus VARCHAR(255),
        histBall100 FLOAT,
        histBall12 FLOAT,
        histBall FLOAT,
        histPTName VARCHAR(255),
        histPTRegName VARCHAR(255),
        histPTAreaName VARCHAR(255),
        histPTTerName VARCHAR(255),
        mathTest VARCHAR(255),
        mathLang VARCHAR(255),
        mathTestStatus VARCHAR(255),
        mathBall100 FLOAT,
        mathBall12 FLOAT,
        mathdpalevel VARCHAR(255), 
        mathBall FLOAT,
        mathPTName VARCHAR(255),
        mathPTRegName VARCHAR(255),
        mathPTAreaName VARCHAR(255),
        mathPTTerName VARCHAR(255),
        mathsttest VARCHAR(255),
        mathstlang VARCHAR(255),
        MathStTestStatus VARCHAR(255),
        MathStBall12 VARCHAR(255),
        MathStBall VARCHAR(255),
        MathStPTName VARCHAR(255),
        MathStPTRegName VARCHAR(255),
        MathStPTAreaName VARCHAR(255),
        MathStPTTerName VARCHAR(255),
        physTest VARCHAR(255),
        physLang VARCHAR(255),
        physTestStatus VARCHAR(255),
        physBall100 FLOAT,
        physBall12 FLOAT,
        physBall FLOAT,
        physPTName VARCHAR(255),
        physPTRegName VARCHAR(255),
        physPTAreaName VARCHAR(255),
        physPTTerName VARCHAR(255),
        chemTest VARCHAR(255),
        chemLang VARCHAR(255),
        chemTestStatus VARCHAR(255),
        chemBall100 FLOAT,
        chemBall12 FLOAT,
        chemBall FLOAT,
        chemPTName VARCHAR(255),
        chemPTRegName VARCHAR(255),
        chemPTAreaName VARCHAR(255),
        chemPTTerName VARCHAR(255),
        bioTest VARCHAR(255),
        bioLang VARCHAR(255),
        bioTestStatus VARCHAR(255),
        bioBall100 FLOAT,
        bioBall12 FLOAT,
        bioBall FLOAT,
        bioPTName VARCHAR(255),
        bioPTRegName VARCHAR(255),
        bioPTAreaName VARCHAR(255),
        bioPTTerName VARCHAR(255),
        geoTest VARCHAR(255),
        geoLang VARCHAR(255),
        geoTestStatus VARCHAR(255),
        geoBall100 FLOAT,
        geoBall12 FLOAT,
        geoBall FLOAT,
        geoPTName VARCHAR(255),
        geoPTRegName VARCHAR(255),
        geoPTAreaName VARCHAR(255),
        geoPTTerName VARCHAR(255),
        engTest VARCHAR(255),
        engTestStatus VARCHAR(255),
        engBall100 FLOAT,
        engBall12 FLOAT,
        engDPALevel VARCHAR(255),
        engBall FLOAT,
        engPTName VARCHAR(255),
        engPTRegName VARCHAR(255),
        engPTAreaName VARCHAR(255),
        engPTTerName VARCHAR(255),
        fraTest VARCHAR(255),
        fraTestStatus VARCHAR(255),
        fraBall100 FLOAT,
        fraBall12 FLOAT,
        fraDPALevel VARCHAR(255),
        fraBall FLOAT,
        fraPTName VARCHAR(255),
        fraPTRegName VARCHAR(255),
        fraPTAreaName VARCHAR(255),
        fraPTTerName VARCHAR(255),
        deuTest VARCHAR(255),
        deuTestStatus VARCHAR(255),
        deuBall100 FLOAT,
        deuBall12 FLOAT,
        deuDPALevel VARCHAR(255),
        deuBall FLOAT,
        deuPTName VARCHAR(255),
        deuPTRegName VARCHAR(255),
        deuPTAreaName VARCHAR(255),
        deuPTTerName VARCHAR(255),
        spaTest VARCHAR(255),
        spaTestStatus VARCHAR(255),
        spaBall100 FLOAT,
        spaBall12 FLOAT,
        spaDPALevel VARCHAR(255),
        spaBall FLOAT,
        spaPTName VARCHAR(255),
        spaPTRegName VARCHAR(255),
        spaPTAreaName VARCHAR(255),
        spaPTTerName VARCHAR(255),
        YEAR INTEGER
    )
        """,
        'TASK_SELECT': f"""SELECT 
	regname, 
	ROUND(AVG(CASE WHEN YEAR = 2019 THEN histball END)::NUMERIC, 2) AS average_score_2019,
	ROUND(AVG(CASE WHEN YEAR = 2020 THEN histball END)::NUMERIC, 2) AS average_score_2020
FROM 
	{GENERAL_TABLE_NAME}
WHERE 
	histteststatus = 'Зараховано'
	AND 
	histball::text !~ '[^0-9.-]'
	AND
    histball IS NOT null 
	AND
	(YEAR = 2019 OR YEAR = 2020)
GROUP BY 
	regname;
        """
    }
}


def merge_dataframe_from_csv_files(csv_path_config, csv_openfile_options):
    dataframes = []

    for year in csv_path_config:
        path = csv_path_config[year]['path']

        encoding = csv_path_config[year]['encoding']
        df = pd.read_csv(path, encoding=encoding, decimal=',', **csv_openfile_options)
        df['YEAR'] = year

        dataframes.append(df)
        print(f'Read csv file for {year} year -  completed !')
    df = pd.concat(dataframes)
    df.index.name = 'ID'
    df = df.where((pd.notnull(df)), None)
    return df


def create_table(table_name, query, conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = %s)", (table_name,))
            table_exists = cur.fetchone()[0]

            if not table_exists:
                cur.execute(query)
                conn.commit()
                print(f"Table {table_name} created")
            else:
                print(f"Table {table_name} already exists")
    except Exception as e:
        print(f"Error creating table {table_name}: {str(e)}")




def load_dataframe_to_postgresql(df, table_name, conn):
    with conn.cursor() as cursor:
        get_last_idx_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(get_last_idx_query)
        last_idx = cursor.fetchone()[0]

        chunk_size = 1000
        while last_idx < len(df):
            chunk_df = df.iloc[last_idx:last_idx + chunk_size]
            chunk_df = chunk_df.reset_index(drop=True)
            cols = chunk_df.columns.tolist()
            vals = ", ".join([f"%({col})s" for col in cols])
            insert_query = sql.SQL(
                f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({vals}) ON CONFLICT DO NOTHING")
            insert_data = [dict(row) for _, row in chunk_df.iterrows()]
            cursor.executemany(insert_query, insert_data)
            conn.commit()
            last_idx += chunk_size
    return len(df)



def execute_query_to_csv(conn, query, filename):
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    output_path = os.path.join(output_dir, filename)

    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        if not rows:
            print('No data found for the query.')
            return

        header = ['область', 'середній бал з історії за 2019 рік', 'середній бал з історії за 2020 рік']
        rows = [header] + [list(row) for row in rows]

    print('The task query request has been successfully completed. It is being uploaded to a file')

    with open(output_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerows(rows)

    print(f'Output CSV file for task query request created! Check it in path: {os.path.abspath(output_path)}')


if __name__ == '__main__':
    connection = psycopg2.connect(
        host="db",
        port=5432,
        dbname="zno",
        user="postgres",
        password=111
    )

    file = open(txt_path, "w")
    start_time = time.time()
    time_first = time.time()
    merged_df = merge_dataframe_from_csv_files(CSV_CONFIG['PATH_CONFIG'], CSV_CONFIG['OPEN_CONFIG'])
    print('Dataset already imported')
    time_end_firt = time.time()
    file.write(f"merge_dataframe_from_csv_files\ntime:{round(time_end_firt-time_first,2)}")
    time_second = time.time()
    create_table(GENERAL_TABLE_NAME,SQL_PARAM['QUERY']['CREATE_TABLE'], connection)
    print('General table created')
    time_end_second = time.time()
    file.write(f"\n\ncreate_table\ntime:{round(time_end_second-time_second,2)}")
    imported_rows = 0
    time_three = time.time()
    print("load dataset to PostgreSQL ")
    while imported_rows != len(merged_df):
        imported_rows = load_dataframe_to_postgresql(merged_df, GENERAL_TABLE_NAME, connection)
    time_end_three = time.time()
    file.write(f"\n\nload_dataframe_to_postgresql\ntime:{round(time_end_three - time_three, 2)}")
    print(f'Import completed ! \nImported rows - {imported_rows}')
    time_four = time.time()
    execute_query_to_csv(connection, SQL_PARAM['QUERY']['TASK_SELECT'], OUT_CSV_FILE)
    time_end_four = time.time()
    file.write(f"\n\nload_dataframe_to_postgresql\ntime:{round(time_end_four - time_four, 2)}")
    end_time = time.time()
    print(f"Program running time: {(end_time - start_time)} seconds"  )
    file.write(f"\n\nmain\ntime: {round((end_time - start_time),2)} seconds")
    file.close()