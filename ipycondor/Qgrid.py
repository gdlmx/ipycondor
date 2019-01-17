# Copyright 2019 Lukas Koschmieder

import pandas as pd
import qgrid

def to_qgrid(data, columns, index):
    df = pd.DataFrame(data, columns=columns)
    df = df.set_index(index)
    widget = qgrid.show_grid(df, show_toolbar=False,
        grid_options={'editable':False})
    return widget
