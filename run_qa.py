import great_expectations as gx
import sys
import pandas as pd
from great_expectations.core.batch import RuntimeBatchRequest

# 1. ข้อมูลพื้นฐาน
CONTEXT_DIR = "include/gx"
CHECKPOINT_NAME = "covid_checkpoint"

# 2. Path ของ Mock Data (Path อ้างอิงจาก Root ของ Repository)
MOCK_DATA_PATH = "tests/mock_data/covid_sample.csv"
SUITE_NAME = "covid_data_suite" # ชื่อ Suite ที่ใช้

try:
    # โหลด Data Context
    context = gx.get_context(context_root_dir=CONTEXT_DIR)
    
    # 🛑 1. โหลดไฟล์ Mock Data ด้วย Pandas
    data_df = pd.read_csv(MOCK_DATA_PATH)
    print(f"Loaded data from {MOCK_DATA_PATH}. Data shape: {data_df.shape}")

    # 🛑 2. สร้าง RuntimeBatchRequest จาก DataFrame ที่โหลดแล้ว
    runtime_batch_request = RuntimeBatchRequest(
        # ใช้ชื่อ Datasource ที่ถูกตั้งค่าใน great_expectations.yml
        datasource_name="covid_datasource", 
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="covid_19_clean",  # ชื่อ Asset ใน Checkpoint
        runtime_parameters={"batch_data": data_df}, # ส่ง DataFrame เข้าไปแทน Path
        batch_identifiers={"default_identifier_name": "covid_batch"}
    )
    
    # 🛑 3. รัน Checkpoint โดยใช้ RuntimeBatchRequest
    print(f"Running Checkpoint '{CHECKPOINT_NAME}'...")
    results = context.run_checkpoint(
        checkpoint_name=CHECKPOINT_NAME,
        validations=[
            {
                "batch_request": runtime_batch_request,
                "expectation_suite_name": SUITE_NAME,
            }
        ]
    )

    if not results.get("success", False):
        print("\n!!! Data Quality Check FAILED !!!")
        sys.exit(1)
    
    print("\n✅ Data Quality Check PASSED.")
    
except Exception as e:
    print(f"An error occurred during Great Expectations run: {e}")
    sys.exit(1)