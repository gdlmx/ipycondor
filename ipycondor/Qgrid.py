# Copyright 2019 Lukas Koschmieder

import pandas as pd
import qgrid

def to_qgrid(table, cols):
    df = pd.DataFrame(table, columns=cols)
    df = df.set_index(cols[0])
    widget = qgrid.show_grid(df, show_toolbar=False,
        grid_options={'editable':False})
    return widget
