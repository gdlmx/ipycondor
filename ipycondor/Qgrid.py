# Copyright 2019 Lukas Koschmieder

import pandas as pd
import qgrid

def to_qgrid(table, cols, index):
    df = pd.DataFrame(table, columns=cols)
    df = df.set_index(index)
    widget = qgrid.show_grid(df, show_toolbar=False,
        grid_options={'editable':False})
    return widget
