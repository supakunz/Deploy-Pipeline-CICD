import pandas as pd
def load_to_warehouse(input_path, output_path):

    # Load cleaned data
    df = pd.read_csv(input_path)

    # Load data into the warehouse
    df.to_csv(output_path, index=False)
    print(f"Data loaded into warehouse from {input_path} to {output_path}")
