import great_expectations as gx
import sys
import pandas as pd
# ไม่ต้อง import RuntimeBatchRequest แล้ว

# 1. ข้อมูลพื้นฐาน
CONTEXT_DIR = "include/gx"
MOCK_DATA_PATH = "tests/mock_data/covid_sample.csv"
SUITE_NAME = "covid_data_suite" # ชื่อ Expectation Suite ที่ใช้

try:
    context = gx.get_context(context_root_dir=CONTEXT_DIR)
    
    # 1. โหลดไฟล์ Mock Data ด้วย Pandas
    data_df = pd.read_csv(MOCK_DATA_PATH)
    print(f"Loaded data from {MOCK_DATA_PATH}. Data shape: {data_df.shape}")

    # 🛑 2. สร้าง Validator Object จาก DataFrame โดยตรง
    print(f"Loading Expectation Suite '{SUITE_NAME}' and creating Validator...")
    
    validator = context.get_validator(
        batch=data_df, # ส่ง DataFrame ที่โหลดแล้วเข้าไป
        expectation_suite_name=SUITE_NAME # ระบุชื่อ Suite
    )

    # 🛑 3. รัน Validation บน Validator Object
    # เมธอด validate() บน Validator จะรันทุก Expectation ใน Suite
    results = validator.validate() 

    # 🛑 4. บันทึกผลลัพธ์และ Data Docs (Optional แต่แนะนำ)
    # เราสามารถใช้ context.save_validation_result() เพื่อบันทึกผล
    context.save_validation_result(results)
    context.build_data_docs()

    if not results["success"]:
        print("\n!!! Data Quality Check FAILED !!!")
        sys.exit(1)
    
    print("\n✅ Data Quality Check PASSED. (Data Docs updated)")
    
except Exception as e:
    print(f"An error occurred during Great Expectations run: {e}")
    sys.exit(1)