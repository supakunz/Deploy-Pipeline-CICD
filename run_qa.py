import great_expectations as gx
import sys

# 1. ข้อมูลพื้นฐาน (อ้างอิงจาก Root Directory)
CONTEXT_DIR = "include/gx"
CHECKPOINT_NAME = "covid_checkpoint"

# 2. Path ของ Mock Data ที่ต้องการใช้ใน CI (Path อ้างอิงจาก Root ของ Repository)
MOCK_DATA_PATH = "tests/mock_data/covid_sample.csv"

# 3. กำหนด Batch Request ที่จะใช้ Override
# ใช้โครงสร้างเดียวกันกับ Checkpoint YAML แต่ใส่ MOCK_DATA_PATH แทน
batch_request_override = {
    "datasource_name": "covid_datasource",
    "data_connector_name": "default_runtime_data_connector_name",
    "data_asset_name": "covid_19_clean",
    "runtime_parameters": {
        "path": MOCK_DATA_PATH # <--- Path ใหม่สำหรับ CI
    },
    "batch_identifiers": {
        "default_identifier_name": "covid_batch"
    }
}

try:
    # โหลด Data Context โดยระบุตำแหน่ง
    context = gx.get_context(context_root_dir=CONTEXT_DIR)
    
    print(f"Running Checkpoint '{CHECKPOINT_NAME}' with Mock Data Path: {MOCK_DATA_PATH}")

    # 🛑 แก้ไขโดยใช้ 'validations' เพื่อบังคับ Override การตั้งค่าทั้งหมดใน YAML
    results = context.run_checkpoint(
        checkpoint_name=CHECKPOINT_NAME,
        validations=[
            {
                "batch_request": batch_request_override,
                "expectation_suite_name": "covid_data_suite", # ต้องระบุชื่อ Expectation Suite
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