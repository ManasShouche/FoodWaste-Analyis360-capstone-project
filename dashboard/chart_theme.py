import plotly.graph_objects as go

_DARK = "#2C1A0E"
_FONT = dict(color=_DARK, size=13)


def apply_theme(fig: go.Figure) -> go.Figure:
    fig.update_layout(
        font=_FONT,
        title_font=dict(color=_DARK, size=16),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(
        tickfont=dict(color=_DARK),
        title_font=dict(color=_DARK),
        linecolor=_DARK,
    )
    fig.update_yaxes(
        tickfont=dict(color=_DARK),
        title_font=dict(color=_DARK),
        linecolor=_DARK,
    )
    fig.update_layout(legend=dict(font=dict(color=_DARK)))
    return fig
