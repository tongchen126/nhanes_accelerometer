from rdata import read_rda
import pandas as pd
import os
import sys

def get_merged_df(df1, df2):
       df1 = df1.copy(deep=True)
       df2 = df2.copy(deep=True)
       df1['timestamp'] = pd.to_datetime(df1['timestamp'])
       df2['timestamp'] = pd.to_datetime(df2['timestamp'])
       df1 = df1.sort_values('timestamp')
       df2 = df2.sort_values('timestamp')
       
       df1_merged = pd.merge_asof(
              df1,
              df2,
              on='timestamp',
              direction='backward'  # Find nearest timestamp that is <= the timestamp in df1
       )
       return df1_merged.copy(deep=True)

def process_csv_file(meta_path, ms2_path, csv_path, output_prefix):
       meta = read_rda(meta_path)
       ms2 = read_rda(ms2_path)
       short_imputed = pd.read_csv(csv_path)
       short = meta['M']['metashort']
       
       header = ["acceleration (mg) - " + \
                 f"{pd.to_datetime(short['timestamp'].iloc[0]).strftime('%Y-%m-%d %H:%M:%S')} - " + \
                 f"{pd.to_datetime(short['timestamp'].iloc[-1]).strftime('%Y-%m-%d %H:%M:%S')} - " + \
                 "sampleRate = 5 seconds", "imputed"]

       impute_table = ms2['IMP']['rout']
       impute_table = pd.DataFrame({"timestamp": meta['M']['metalong']['timestamp'],
              "imputed": impute_table.apply(lambda x: 1 if x['r1'] > 0 or x['r3'] > 0 else 0, axis=1)})
       
       short_imputed = get_merged_df(short_imputed, impute_table)
       short = get_merged_df(short, impute_table)
       
       short_imputed_final = pd.DataFrame({header[0]: short_imputed['ENMO'] * 1000,
                                           header[1]: short_imputed['imputed']})
       
       short_final = pd.DataFrame({header[0]: short['ENMO'] * 1000,
                                   header[1]: short['imputed']})
       
       short_imputed_final.to_csv(output_prefix + "imputed.csv", index=False, encoding='utf-8')
       short_final.to_csv(output_prefix + "orig.csv", index=False, encoding='utf-8')
       
if __name__ == "__main__":
       #process_csv_file("/root/workspace/PycharmProjects/nhanes_process/tmp4/output_tmp/meta/basic/meta_62180.csv.RData",
       #          "/root/workspace/PycharmProjects/nhanes_process/tmp4/output_tmp/meta/ms2.out/62180.csv.RData",
       #          "/root/workspace/PycharmProjects/nhanes_process/tmp4/output_tmp/meta/csv/62180.csv.RData.csv", "tmp")
       print(f"{sys.argv}")
       process_csv_file(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
