import pandas as pd
import altair as alt


def check_string(string:str) -> None:
  '''When it comes to period charts, 
  it checks if the agg_level is a correct value'''
  try:
    string in ['date_hour', 'date', 'topic']
  except Exception:
    print("Introduce a valid input: 'date_hour', 'date' or 'topic'")

# EMOTION CHARTS

def get_long_emotion_df(df:pd.DataFrame, agg_level:str) -> pd.DataFrame:

  def get_long_data(df:pd.DataFrame) -> pd.DataFrame:
    agg_level = df.columns[0]
    df_emo_long = df.melt(id_vars=agg_level, value_vars=['surprise', 'fear', 'joy', 'sadness', 'anger','love'])
    df_emo_long.rename(columns={'variable':'emotion', 'value':'counts'}, inplace=True)
    df_emo_long['percent'] = df_emo_long['counts'].groupby(agg_level)['counts'].transform('sum')
    
    return df_emo_long

  check_string(agg_level)

  df_emo = df[[agg_level, 'anger', 'fear', 'joy', 'love', 'sadness', 'surprise']].groupby(agg_level).sum().reset_index()
  
  return get_long_data(df_emo)

def emotion_global_norm_bar_chart(df: pd.DataFrame) -> alt.Chart:
  df_long_e = get_long_emotion_df(df, agg_level='topic')

  return alt.Chart(df_long_e).mark_bar().encode(
    x=alt.X('sum(counts)', stack="normalize"),
    y='topic',
    color='emotion',
    tooltip = ['topic', 'emotion', 
    alt.Tooltip('sum(percent):Q', format='.1%'), 
    'sum(counts)']
  ).properties(title='Emotions per Topic - Normalized')

def total_global_emotion_donut_chart(df: pd.DataFrame) -> alt.Chart:
  df_long_e = get_long_emotion_df(df, agg_level='topic')

  return alt.Chart(df_long_e).mark_arc(innerRadius=50).encode(
    theta=alt.Theta(field='counts', type="quantitative"),
    color=alt.Color(field='emotion', type="nominal"),
    tooltip = ['topic', 'emotion', 
    alt.Tooltip('sum(percent):Q', format='.1%'), 'sum(counts)']
  ).properties(title='Emotions - Total Count')

def emotion_period_area_chart(df:pd.DataFrame, agg_level:str, normalize:bool=False) -> alt.Chart:
  check_string(agg_level)
  df_long_e = get_long_emotion_df(df, agg_level)

  if normalize == True:
    stack = 'normalize'
    how = 'Normalized'
    opacity = 1
  else:
    stack = None
    how = 'Non-Normalized'
    opacity = 0.38
  if agg_level == 'date_hour':
    period = 'Hour'
  else:
    period = 'Day'

  return alt.Chart(df_long_e).mark_area(opacity=opacity).encode(
    x=agg_level, 
    y=alt.Y("counts:Q", stack=stack),
    color='emotion', 
    tooltip=[agg_level, 'counts', 'emotion']
  ).properties(title=f'Emotions per {period} - {how}')

  