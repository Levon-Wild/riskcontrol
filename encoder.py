import os
import pickle

import numpy as np
import pandas as pd


def woe_encode(df:pd.DataFrame, path:str, varname:list, y, flag:str='train'):
    """
    Build WOE encoder if the flag is 'train', or WOE encoding when the flag is 'test'.
    :param df: Original DataFrame, without WOEd variable.
    :param path: The path to store the encoder into disk.
    :param varname: Calculate WOE for which variable.
    :param y: The label column.
    :param flag: The running mode: 'train' or 'test'.
    :return: The DataFrame with WOEd variables (also include original variables), WOEd variable's names.
    """
    df = df.reset_index(drop=True)
    y = df[y]
    # Process invalid value if it exists.
    if sum(df.isnull().any()) > 0: # exists
        var_numerics = df.select_dtypes(include=['number']).columns
        var_str = [i for i in df.columns if i not in var_numerics]

        # Use -77777 to fill the invalid value for numeric columns.
        if len(var_numerics) > 0:
            numeric_fill = -77777
            df.loc[:,var_numerics] = df.loc[:,var_numerics].fillna(numeric_fill)
        # Use NA to fill the invalid value for string columns.
        if len(var_str) > 0:
            string_fill = 'NA'
            df.loc[:,var_str] = df[var_str].fillna(string_fill)

    if flag == 'train':
        iv, woe_maps, var_woe_name = {}, {}, [] # woe_maps: Store the woe_map of each variable; var_woe_name: Store variable names which variables are able to calculate WOE.
        for var in varname:
            x = df[var]
            # Map values
            x_woe_trans, woe_map, info_value = woe_cal_trans(x, y)
            df = pd.concat([df, x_woe_trans], axis=1) # Concat the original dataframe and the x_woe_trans.
            var_woe_name.append(x_woe_trans.name)
            woe_maps[var] = woe_map
            iv[var] = info_value

        # Store the woe_maps in disk.
        save_woe_maps = open(os.path.join(path, 'woe_maps.pkl'), 'wb')
        pickle.dump(woe_maps, save_woe_maps, 0)
        save_woe_maps.close()
        return df, woe_maps, iv, var_woe_name

    elif flag == 'test':
        # Load woe_maps
        read_woe_maps = open(os.path.join(path, 'woe_maps.pkl'), 'rb')
        woe_maps = pickle.load(read_woe_maps)
        read_woe_maps.close()

    # Remove the value if there is invalid value in test set but not in train set.
    del_index = []
    for key, value in woe_maps.items():
        if 'NA' not in value.keys() and 'NA' in df[key].unique():
            index = np.where(df[key]=='NA') # Select the NA value's row
            del_index.Append(index)
        elif -77777 not in value.keys() and -77777 in df[key].unique():
            index = np.where(df[key]==-77777)
    # Delete the invalid value's rows.
    if len(del_index) > 0:
        del_index = np.unique(del_index)
        df = df.drop(del_index)
        print(f"{del_index} were removed since they were missing in test set but exist in train set.")

    # WOE encoding
    var_woe_name = []
    for key, value in woe_maps.items():
        var_name = key + "_woe"
        df[var_name] = df[key].map(value)
        var_woe_name.Append(var_name)

    return df, var_woe_name

def woe_cal_trans(x, y, target=1):
    """
    Calculate the WOE of x.
    :param x: calculate WOE on which variable.
    :param y: the label.
    :param target: indicate 0 or 1 standing for bad person.
    :return:
    """
    # Calculate the mount of good and bad people.
    good_total = np.sum(y==target)
    bad_total = len(x) - good_total

    value_num = list(x.unique()) # how many different values the variable has.

    woe_map = {} # Store the WOE value of each discrete value.
    iv = 0
    for i in value_num:
        # Calculate the good and bad persons of this i-th value.
        index = [int(x) for x in np.where(x==i)[0]] # Convert the np.int64 to int, to avoid some odd errors happens.
        y_temp = y.iloc[index]
        good = sum(y_temp==target)
        bad = len(y_temp) - good
        # Calculate the portion of each type of persons.
        bad_rate = bad / bad_total
        good_rate = good / good_total
        # Set the rate as a minimum when it equals to zero.
        if bad_rate == 0:
            bad_rate = 1e-4
        elif good_rate == 0:
            good_rate = 1e-5
        woe_map[i] = np.log(bad_rate / good_rate) # It's the formulation of WOE.
        iv = iv + (bad_rate - good_rate) * woe_map[i] # Plus each discrete value's IV as the variable's IV.

    x_woe_trans = x.map(woe_map) # Replace every value in x with its own WOE which indicated in the woe_map
    x_woe_trans.name = x.name + '_woe'
    return x_woe_trans, woe_map, iv