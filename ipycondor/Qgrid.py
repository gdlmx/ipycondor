# Copyright 2019 Lukas Koschmieder

import pandas as pd
import qgrid

def to_qgrid(data, columns, index=None):
    df = pd.DataFrame(data, columns=columns)
    if index:
        df = df.set_index(index)
        df = df.sort_index()
    widget = qgrid.show_grid(df, show_toolbar=False,
        grid_options={'editable':False,
                      'minVisibleRows':10,
                      'maxVisibleRows':8})
    return widget
