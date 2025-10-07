import re
import pandas as pd
from dateutil import parser

# ฟังก์ชันแปลง datetime หลาย format
def normalize_datetime(series):
    """
    แปลง datetime หลาย format ให้เป็น 'YYYY-MM-DD HH:MM'
    """
    def parse_value(x):
        try:
            if pd.isna(x):
                return None
            dt = parser.parse(str(x))
            return dt.strftime("%Y-%m-%d %H:%M")  # normalize format
        except Exception:
            return None
    
    return series.apply(parse_value)

# ฟังก์ชันแปลงเป็น snake_case
def to_snake_case(col_name):
    # แปลง space, dash เป็น underscore
    col_name = re.sub(r"[ -]+", "_", col_name)
    # ลบตัวอักษรพิเศษ
    col_name = re.sub(r"[^\w_]", "", col_name)
    # แปลงเป็นตัวเล็ก
    return col_name.lower()

# ฟังชก์ชันแปลง type ของ DataFrame
def cast_data_types(df):
    """
    แปลงชนิดข้อมูลของ DataFrame
    """
   # แปลงชนิดข้อมูล
    df = df.astype({
        "sno": "int",
        "province_state": "string",
        "country_region": "string"
        })

    # แปลงวันที่
    df["observationdate"] = pd.to_datetime(df["observationdate"], format="%m/%d/%Y")
    df["last_update"] = pd.to_datetime(df["last_update"], format="%Y-%m-%d %H:%M")

    # แปลงตัวเลข
    df["confirmed"] = pd.to_numeric(df["confirmed"], errors="coerce").astype(int)
    df["deaths"] = pd.to_numeric(df["deaths"], errors="coerce").astype(int)
    df["recovered"] = pd.to_numeric(df["recovered"], errors="coerce").astype(int)
        
    return df

# Pipeline main function
def run_pipeline(input_path: str, output_path: str) -> pd.DataFrame:
    """
    Pipeline ครบชุด: อ่าน CSV → normalize → snake_case → cast → save
    """

    # ดึงข้อมูลจากไฟล์ CSV
    df = pd.read_csv(input_path)

    # standardize normalize time
    df["last_update"] = normalize_datetime(df["last_update"])

    # แปลงชื่อคอลัมน์เป็น snake_case
    df.columns = [to_snake_case(col) for col in df.columns]

    # แปลงชนิดข้อมูล
    df = cast_data_types(df)

    # บันทึกเป็นไฟล์ CSV
    df.to_csv(output_path, index=False)

    return df

if __name__ == "__main__":
    run_pipeline(
        input_path="/opt/airflow/data/raw/covid_19_raw.csv",
        output_path="/opt/airflow/data/output/covid_19_clean.csv"
    )
