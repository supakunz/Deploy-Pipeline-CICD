import pandas as pd

def handle_negative_value(df: pd.DataFrame):
   # จัดการกับ negative value ดังนี้
   df = df
   # แทนที่ด้วย 0
   df["confirmed"] = df["confirmed"].apply(lambda x: max(x, 0))
   df["deaths"] = df["deaths"].apply(lambda x: max(x, 0))
   df["recovered"] = df["recovered"].apply(lambda x: max(x, 0))

   return df

def handle_missing_value(input_path: str, output_path: str):
   # จัดการกับ missing value ดังนี้

   # ดึงข้อมูลจากไฟล์ CSV
   df = pd.read_csv(input_path)

   # แทนที่ด้วย ค่า mean (int)
   df["confirmed"] = df["confirmed"].fillna(int(df["confirmed"].mean()))
   df["deaths"] = df["deaths"].fillna(int(df["deaths"].mean()))
   df["recovered"] = df["recovered"].fillna(int(df["recovered"].mean()))

   # แทนที่ด้วย Unknow (str)
   df["province_state"] = df["province_state"].fillna("Unknown")
   df["country_region"] = df["country_region"].fillna("Unknown")

   # ลบ rows นั้นถ้าเป็น NaT (datetime)
   df = df.dropna(subset=["observationdate", "last_update"])

   # เรียกใช้ฟังก์ชันจัดการกับ negative value
   df = handle_negative_value(df)

   # สร้างเลขลำดับ unique ใหม่
   df['sno'] = range(1, len(df) + 1)
   
   # บันทึกเป็นไฟล์ CSV
   df.to_csv(output_path, index=False)

   return df