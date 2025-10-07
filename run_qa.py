import great_expectations as gx
import sys

# 1. ข้อมูลพื้นฐาน
CONTEXT_DIR = "include/gx"
CHECKPOINT_NAME = "covid_checkpoint"

# 2. ข้อมูล Mock Data ที่ต้องการใช้ใน CI (Path อ้างอิงจาก Root ของ Repository)
MOCK_DATA_PATH = "tests/mock_data/covid_sample.csv"

# 3. สร้าง Batch Request ที่จะใช้ Override
# ใช้โครงสร้างเดียวกับที่คุณกำหนดใน Checkpoint แต่เปลี่ยนแค่ค่า 'path'
# Note: Great Expectations มักจะรับ Path เป็น runtime_parameters
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
    
    # รัน Checkpoint โดยส่ง Batch Request ใหม่เข้าไปแทนที่ค่าเดิม
    print(f"Running Checkpoint '{CHECKPOINT_NAME}' with Mock Data Path: {MOCK_DATA_PATH}")
    
    # run_checkpoint จะแทนที่การตั้งค่า 'validations' และ 'batch_request' เดิมในไฟล์ Checkpoint
    results = context.run_checkpoint(
        checkpoint_name=CHECKPOINT_NAME,
        batch_request=batch_request_override,
    )

    if not results.get("success", False):
        print("\n!!! Data Quality Check FAILED !!!")
        sys.exit(1)
    
    print("\n✅ Data Quality Check PASSED.")
    
except Exception as e:
    print(f"An error occurred during Great Expectations run: {e}")
    sys.exit(1)