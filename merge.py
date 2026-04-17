import pandas as pd
import glob

file_list = glob.glob('output/*.csv')
new_columns = ['@작성자', '채널 ID', '댓글 내용', '작성 시간', '댓글 링크 (하이라이트)']
df_list = [pd.read_csv(file, header=0, names=new_columns) for file in file_list]
combined_df = pd.concat(df_list, axis=0, ignore_index=True)
combined_df.to_csv('output.csv', index=False, encoding='utf-8-sig')