import pandas as pd


class ETL:
    def __init__(self, extract_path, transform_func, load_path):
        self.extract_path = extract_path
        self.transform_func = transform_func
        self.load_path = load_path

    def extract(self):
        try:
            # Perform the extraction from the specified path
            data = pd.read_csv(self.extract_path)
            return data
        except Exception as e:
            print("Extraction error:", str(e))
            return None

    def transform(self, data):
        try:
            # Apply the specified transformation function
            transformed_data = self.transform_func(data)
            return transformed_data
        except Exception as e:
            print("Transformation error:", str(e))
            return None

    def load(self, transformed_data):
        try:
            # Save the transformed data to the specified path
            transformed_data.to_csv(self.load_path, index=False)
            print("Data successfully loaded at", self.load_path)
        except Exception as e:
            print("Loading error:", str(e))

    def run(self):
        self.extract
        self.transform
        self.load
        return True
