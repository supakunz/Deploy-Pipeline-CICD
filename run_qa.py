import great_expectations as gx
import sys
import pandas as pd
# เราจะไม่ใช้ RuntimeBatchRequest แล้ว
# from great_expectations.core.batch import RuntimeBatchRequest 

# 1. ข้อมูลพื้นฐาน
CONTEXT_DIR = "include/gx"
# CHECKPOINT_NAME = "covid_checkpoint" # 🛑 ไม่ใช้ Checkpoint Name แล้ว
MOCK_DATA_PATH = "tests/mock_data/covid_sample.csv"
SUITE_NAME = "covid_data_suite" # ชื่อ Expectation Suite ที่ใช้

try:
    context = gx.get_context(context_root_dir=CONTEXT_DIR)
    
    # 🛑 1. โหลดไฟล์ Mock Data ด้วย Pandas
    data_df = pd.read_csv(MOCK_DATA_PATH)
    print(f"Loaded data from {MOCK_DATA_PATH}. Data shape: {data_df.shape}")

    # 🛑 2. รัน Validation Operator โดยตรง (Bypass Checkpoint)
    print(f"Running Validation Suite '{SUITE_NAME}' directly on DataFrame...")

    # สร้าง Validation Batch List ที่ต้องการตรวจสอบ
    validation_batch = [
        {
            "batch": data_df,  # ส่ง DataFrame เข้าไปโดยตรง
            "expectation_suite_name": SUITE_NAME
        }
    ]

    # ใช้ Validation Operator เพื่อรันการตรวจสอบ
    results = context.run_validation_operator(
        "action_list_operator", # ใช้ชื่อ Validation Operator ที่เป็น Default
        assets_to_validate=validation_batch,
        # ตั้งชื่อ run_id เพื่อให้ผลลัพธ์ถูกบันทึกใน Data Docs (ถ้าต้องการ)
        run_id=f"ci_run_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}"
    )

    if not results.get("success", False):
        print("\n!!! Data Quality Check FAILED !!!")
        sys.exit(1)
    
    print("\n✅ Data Quality Check PASSED.")
    
except Exception as e:
    print(f"An error occurred during Great Expectations run: {e}")
    sys.exit(1)