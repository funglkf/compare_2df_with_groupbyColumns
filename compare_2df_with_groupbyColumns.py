import pandas as pd
import numpy as np


def compare_2df_with_groupbyColumns(df1, df2, groupby_col, return_diff_only=False, rmv_groupby_col=False):

    g_df1 = df1.fillna(value=np.nan).groupby(groupby_col, as_index=False).agg(lambda x: ';'.join(x.str.strip()) if x.dtype == 'object'
                                                                              else ';'.join(x.astype(str).str.strip()))

    if df2 is None:
        return g_df1.drop(columns=groupby_col if rmv_groupby_col else []).T.set_axis(['df1'], axis=1)

    g_df2 = df2.fillna(value=np.nan).groupby(groupby_col, as_index=False).agg(lambda x: ';'.join(x.str.strip()) if x.dtype == 'object'
                                                                              else ';'.join(x.astype(str).str.strip()))
    # df.groupby(['Name', 'Age'],as_index=False).agg(lambda x : ';'.join(x.str.strip()) if x.dtype=='object' else x.head(1).dtype)

    if g_df1.shape[0] > 1 or g_df2.shape[0] > 1:
        raise Exception('Dataframe more thane 1 row after grouped!')

    t_g_df = pd.concat([g_df1, g_df2]).drop(
        columns=groupby_col if rmv_groupby_col else []).T.set_axis(['df1', 'df2'], axis=1)

    # t_g_df["equal"] = (t_g_df.c1 == t_g_df.c2)
    t_g_df['equal'] = t_g_df['df1'].eq(t_g_df['df2'])

    if return_diff_only:
        t_g_df = t_g_df[t_g_df['equal'] == False]

    return t_g_df
