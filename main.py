from fastapi import FastAPI, UploadFile, HTTPException
import pandas as pd
import psycopg2
from fastapi.responses import StreamingResponse
import io
import os

app = FastAPI()

DBname = os.getenv('DBNAME')
DBpass = os.getenv('DBPASS')
User = os.getenv('DBUSER')

DATABASE_URL = f"postgresql://{DBname}:{DBpass}@localhost:5432/{User}"

@app.post("/upload/")
async def upload_file(file: UploadFile = None):

    try:

        # Use pandas to read the file
        data = pd.read_csv(file.file, sep=';')
        print(data)


        # Storing data to the database
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                version = 0
                try:
                    cur.execute("SELECT MAX(version) FROM projects;")
                    current_version = cur.fetchone()[0] or 0
                    version = current_version + 1
                except:
                    pass
                for index, row in data.iterrows():
                    parent_code = None
                    if len(row[0]) != 1:
                        parent_code = row[0][:-2]
                    data = row[2:].dropna().to_json()
                    cur.execute(
                        "INSERT INTO projects (code, parent_code, name, version, data) VALUES (%s, %s, %s, %s, %s)",
                        (row[0], parent_code, row[1], version, data))

        return {"status": "success", "message": "File uploaded successfully"}
    except Exception as e:
        return {"status": "error", "message": f"{str(e)}"}


@app.get("/data/")
def get_data(version: int):
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM projects WHERE version = %s", (version,))
                data = cursor.fetchall()

        df = pd.DataFrame(data, columns=['id', 'code', 'parent_code', 'project', 'version', 'data'])
        json_data = df['data'].apply(pd.Series)
        df = pd.concat([df.drop('data', axis=1), json_data], axis=1)
        df = df.drop(['id', 'parent_code', 'version'], axis=1)

        def get_parent(code):
            parts = code.split('.')
            if len(parts) == 1:
                return None
            return '.'.join(parts[:-1])

        # Iterating through rows of the dataframe
        for _, row in df.iterrows():
            parent_code = get_parent(row['code'])
            while parent_code:
                for year in row.keys()[2:]:
                    if not pd.isna(row[year]):
                        df.loc[df['code'] == parent_code, year] = df.loc[df['code'] == parent_code, year].fillna(0) + row[
                            year]
                parent_code = get_parent(parent_code)


        # Combine the aggregated data and the terminal nodes
        response = StreamingResponse(iter([io.StringIO(df.to_csv(index=False)).getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = "attachment; filename=data.csv"

        return response
    except Exception as e:
        return {"status": "error", "message": f"{str(e)}"}
