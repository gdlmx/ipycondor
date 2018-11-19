# Copyright 2018 Mingxuan Lin

from IPython import display

def to_html_table(table, cols):
    header = '<tr>\n' +  '\n'.join( "<th>{0}</th>".format(x) for x in cols) + '\n</tr>\n'
    content='\n'.join(
           ('<tr>\n' + '\n'.join( "<td>{0}</td>".format(x) for x in r ) + '\n</tr>') for r in table
    )
    return display.HTML(_css + '<table>'+header+content+'</table>')

_css="""
<style>
.tooltip {
    position: relative;
    display: inline-block;
    border-bottom: 1px dotted black;
}

.tooltip .tooltiptext {
    visibility: hidden;
    width: 120px;
    background-color: black;
    color: #fff;
    text-align: center;
    border-radius: 6px;
    padding: 5px 0;
    position: absolute;
    z-index: 1;
    bottom: 150%;
    left: 50%;
    margin-left: -60px;
}

.tooltip .tooltiptext::after {
    content: "";
    position: absolute;
    top: 100%;
    left: 50%;
    margin-left: -5px;
    border-width: 5px;
    border-style: solid;
    border-color: black transparent transparent transparent;
}

.tooltip:hover .tooltiptext {
    visibility: visible;
}
</style>
"""
