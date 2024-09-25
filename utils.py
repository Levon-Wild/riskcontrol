import os
import pandas as pd

def pwd():
    """获取当前工作路径"""
    return os.getcwd()

def lower_columns(df: pd.DataFrame):
    """调整DataFrame的columns全部为小写字母"""
    df.columns = df.columns.str.lower()
    return df