import great_expectations as gx
import sys
import pandas as pd
from great_expectations.core.batch import RuntimeBatchRequest # ⬅️ นำเข้าโมดูลนี้

# 1. ข้อมูลพื้นฐาน
CONTEXT_DIR = "include/gx"
MOCK_DATA_PATH = "tests/mock_data/covid_sample.csv"
SUITE_NAME = "covid_data_suite" 
DATASOURCE_NAME = "covid_datasource" # ชื่อ Datasource ใน great_expectations.yml

try:
    context = gx.get_context(context_root_dir=CONTEXT_DIR)
    
    # 1. โหลดไฟล์ Mock Data ด้วย Pandas
    data_df = pd.read_csv(MOCK_DATA_PATH)
    print(f"Loaded data from {MOCK_DATA_PATH}. Data shape: {data_df.shape}")

    # 🛑 2. สร้าง RuntimeBatchRequest ที่ส่งข้อมูล (DataFrame) เข้าไป
    runtime_batch_request = RuntimeBatchRequest(
        datasource_name=DATASOURCE_NAME,
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="covid_19_clean",
        runtime_parameters={"batch_data": data_df}, # ⬅️ ส่ง DataFrame เข้าไป
        batch_identifiers={"default_identifier_name": "covid_batch"}
    )
    
    # 🛑 3. สร้าง Batch Object (Batch List) จาก Request
    # Great Expectations จะใช้ Request และ DataFrame เพื่อสร้าง Batch Object ที่ถูกต้อง
    batch_list = context.get_batch_list(batch_request=runtime_batch_request)

    # 🛑 4. รัน Validation Operator บน Batch List
    print(f"Running Validation Suite '{SUITE_NAME}'...")
    
    results = context.run_validation_operator(
        "action_list_operator", 
        assets_to_validate=[
            {
                # ต้องระบุ 'batch_list' เพื่อส่ง Batch Object ที่สมบูรณ์เข้าสู่ Operator
                "batch_list": batch_list, 
                "expectation_suite_name": SUITE_NAME
            }
        ]
    )

    # 5. บันทึกผลลัพธ์และ Data Docs (Optional)
    context.save_validation_result(results)
    context.build_data_docs()

    if not results.get("success", False):
        print("\n!!! Data Quality Check FAILED !!!")
        sys.exit(1)
    
    print("\n✅ Data Quality Check PASSED. (Data Docs updated)")
    
except Exception as e:
    print(f"An error occurred during Great Expectations run: {e}")
    # ใช้ print(e) เพื่อดู error ที่ชัดเจนขึ้นถ้ายังเกิดปัญหา
    sys.exit(1)