import pandas as pd
import numpy as np
import json
import os
import yaml
from datetime import datetime
import re

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.bool_):
            return bool(obj)
        return super().default(obj)

class DataQualityChecker:
    def __init__(self, config_path="great_expectation/config/dq_config.yml", data_dir="data/kaggle-raw"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        self.threshold = float(self.config.get('threshold', 98.0))
        self.data_dir = data_dir
        self.results = []
        self.summary = {}
    
    def load_all_tables(self):
        tables = {}
        if not os.path.exists(self.data_dir):
            print(f"❌ Data directory not found: {self.data_dir}")
            return tables
        
        csv_files = [f for f in os.listdir(self.data_dir) if f.endswith(".csv") and not f.startswith('.')]
        csv_files = [f for f in csv_files if 'olist_' in f or 'product_category' in f]
        
        if not csv_files:
            print(f"❌ No CSV files found in: {self.data_dir}")
            return tables
        
        print(f"📊 Loading {len(csv_files)} raw data files from: {self.data_dir}/")
        for csv_file in csv_files:
            table_name = csv_file.replace("_dataset.csv", "").replace(".csv", "")
            file_path = os.path.join(self.data_dir, csv_file)
            try:
                df = pd.read_csv(file_path)
                tables[table_name] = df
                print(f"   ✓ {table_name}: {len(df):,} rows, {len(df.columns)} columns")
            except Exception as e:
                print(f"   ❌ Error loading {csv_file}: {e}")
        return tables

    def check_completeness(self, df, column):
        total = int(len(df))
        non_null = int(df[column].notna().sum())
        score = round(float((non_null / total * 100) if total > 0 else 0), 2)
        return {
            "score": score,
            "valid_records": non_null,
            "invalid_records": total - non_null,
            "details": f"{non_null}/{total} non-null"
        }
    
    def check_validity(self, df, column):
        total = int(len(df))
        valid_count = 0
        
        if pd.api.types.is_numeric_dtype(df[column]):
            valid_count = int(df[column].notna().sum() - df[column].isin([np.inf, -np.inf]).sum())
        elif pd.api.types.is_string_dtype(df[column]) or pd.api.types.is_object_dtype(df[column]):
            valid_count = int(df[column].notna().sum() - (df[column].astype(str).str.strip() == '').sum())
        else:
            valid_count = int(df[column].notna().sum())
        
        score = round(float((valid_count / total * 100) if total > 0 else 0), 2)
        return {
            "score": score,
            "valid_records": valid_count,
            "invalid_records": total - valid_count,
            "details": f"{valid_count}/{total} valid"
        }
    
    def check_uniqueness(self, df, column):
        """Uniqueness check - NOT included in threshold calculation"""
        total = int(len(df))
        unique_count = int(df[column].nunique())
        duplicate_count = total - unique_count
        score = round(float((unique_count / total * 100) if total > 0 else 0), 2)
        return {
            "score": score,
            "valid_records": unique_count,
            "invalid_records": duplicate_count,
            "details": f"{unique_count}/{total} unique"
        }
    
    def check_accuracy(self, df, column):
        total = int(len(df))
        
        if pd.api.types.is_numeric_dtype(df[column]):
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            accurate_count = int(((df[column] >= lower_bound) & (df[column] <= upper_bound)).sum())
        else:
            accurate_count = int(df[column].notna().sum())
        
        score = round(float((accurate_count / total * 100) if total > 0 else 0), 2)
        return {
            "score": score,
            "valid_records": accurate_count,
            "invalid_records": total - accurate_count,
            "details": f"{accurate_count}/{total} accurate"
        }
    
    def check_consistency(self, df, column):
        total = int(len(df))
        consistent_count = int(df[column].notna().sum())
        score = round(float((consistent_count / total * 100) if total > 0 else 0), 2)
        return {
            "score": score,
            "valid_records": consistent_count,
            "invalid_records": total - consistent_count,
            "details": f"{consistent_count}/{total} consistent"
        }
    
    def check_conformity(self, df, column):
        total = int(len(df))
        col_lower = column.lower()
        
        if 'email' in col_lower:
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            conforming_count = int(df[column].astype(str).str.match(email_pattern).sum())
        elif 'date' in col_lower or 'time' in col_lower:
            try:
                pd.to_datetime(df[column], errors='coerce')
                conforming_count = int(df[column].notna().sum())
            except:
                conforming_count = int(df[column].notna().sum())
        else:
            conforming_count = int(df[column].notna().sum())
        
        score = round(float((conforming_count / total * 100) if total > 0 else 0), 2)
        return {
            "score": score,
            "valid_records": conforming_count,
            "invalid_records": total - conforming_count,
            "details": f"{conforming_count}/{total} conform"
        }
    
    def run_checks(self):
        print("\n🔍 Running comprehensive data quality checks on RAW DATA...")
        print(f"   Threshold: {self.threshold}%")
        print(f"   Note: Uniqueness is tracked but NOT included in pass/fail threshold")
        print(f"   Data Source: {self.data_dir}/\n")
        
        tables = self.load_all_tables()
        
        if not tables:
            print("\n❌ No data to check. Exiting...")
            return
        
        for table_name, df in tables.items():
            print(f"\n📋 Checking table: {table_name}")
            for column in df.columns:
                completeness = self.check_completeness(df, column)
                validity = self.check_validity(df, column)
                uniqueness = self.check_uniqueness(df, column)
                accuracy = self.check_accuracy(df, column)
                consistency = self.check_consistency(df, column)
                conformity = self.check_conformity(df, column)
                
                # Calculate overall score WITHOUT uniqueness (5 dimensions only)
                overall_score = round(float((
                    completeness["score"] + 
                    validity["score"] + 
                    accuracy["score"] + 
                    consistency["score"] + 
                    conformity["score"]
                ) / 5), 2)
                
                result = {
                    "table_name": table_name,
                    "column_name": column,
                    "data_type": str(df[column].dtype),
                    "total_rows": int(len(df)),
                    "completeness": completeness,
                    "validity": validity,
                    "uniqueness": uniqueness,  # Tracked but not in overall score
                    "accuracy": accuracy,
                    "consistency": consistency,
                    "conformity": conformity,
                    "overall_score": overall_score,
                    "overall_passed": bool(overall_score >= self.threshold),
                    "timestamp": datetime.now().isoformat()
                }
                self.results.append(result)
                
                status = "✅ PASS" if result["overall_passed"] else "❌ FAIL"
                print(f"   {status} {column}: {overall_score}% (Uniqueness: {uniqueness['score']}% - informational)")
        
        print(f"\n✓ Analyzed {len(self.results)} columns across {len(tables)} tables\n")
        self.calculate_summary()
    
    def calculate_summary(self):
        total = len(self.results)
        passed = sum(1 for r in self.results if r["overall_passed"])
        
        tables = {}
        for r in self.results:
            tn = r["table_name"]
            if tn not in tables:
                tables[tn] = {"total_columns": 0, "passed_columns": 0, "failed_columns": 0, "scores": []}
            tables[tn]["total_columns"] += 1
            tables[tn]["passed_columns" if r["overall_passed"] else "failed_columns"] += 1
            tables[tn]["scores"].append(r["overall_score"])
        
        for tn in tables:
            avg = sum(tables[tn]["scores"]) / len(tables[tn]["scores"])
            tables[tn]["average_score"] = round(float(avg), 2)
            tables[tn]["passed"] = bool(tables[tn]["average_score"] >= self.threshold)
        
        self.summary = {
            "total_columns": total,
            "passed_columns": passed,
            "failed_columns": total - passed,
            "overall_pass_rate": round(float(passed/total*100) if total > 0 else 0, 2),
            "threshold": float(self.threshold),
            "total_tables": len(tables),
            "tables": tables,
            "timestamp": datetime.now().isoformat()
        }
    
    def save_results(self):
        os.makedirs("Great_Expectation", exist_ok=True)
        print("💾 Saving results...")
        with open("Great_Expectation/dq_results_detailed.json", "w") as f:
            json.dump(self.results, f, indent=2, cls=NumpyEncoder)
        print(f"   ✓ dq_results_detailed.json ({len(self.results)} records)")
        with open("Great_Expectation/dq_summary.json", "w") as f:
            json.dump(self.summary, f, indent=2, cls=NumpyEncoder)
        print(f"   ✓ dq_summary.json")
        
        csv_data = [{
            "Table": r["table_name"],
            "Column": r["column_name"],
            "Data Type": r["data_type"],
            "Total Rows": r["total_rows"],
            "Overall Score": r["overall_score"],
            "Status": "PASS" if r["overall_passed"] else "FAIL",
            "Completeness Score": r["completeness"]["score"],
            "Validity Score": r["validity"]["score"],
            "Uniqueness Score": r["uniqueness"]["score"],
            "Accuracy Score": r["accuracy"]["score"],
            "Consistency Score": r["consistency"]["score"],
            "Conformity Score": r["conformity"]["score"],
            "Threshold": self.threshold,
            "Note": "Overall score excludes Uniqueness"
        } for r in self.results]
        
        pd.DataFrame(csv_data).to_csv("Great_Expectation/dq_results_summary.csv", index=False)
        print(f"   ✓ dq_results_summary.csv\n")

if __name__ == "__main__":
    print("=" * 80)
    print("DATA QUALITY CHECK - RAW DATA (data/kaggle-raw)")
    print("=" * 80 + "\n")
    
    checker = DataQualityChecker(data_dir="data/kaggle-raw")
    checker.run_checks()
    
    if checker.results:
        checker.save_results()
        print("=" * 80)
        print(f"✅ COMPLETE! Pass Rate: {checker.summary['overall_pass_rate']}%")
        print(f"   Passed: {checker.summary['passed_columns']}/{checker.summary['total_columns']} columns")
        print(f"   Note: Overall score based on 5 dimensions (excludes Uniqueness)")
        print("=" * 80)
        print("\n📁 Results saved in: Great_Expectation/")
        print("\n🚀 Run dashboard: python data_quality_dashboard.py")